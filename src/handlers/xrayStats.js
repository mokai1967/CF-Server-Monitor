/**
 * Xray 代理流量统计（独立于探针主指标表）
 *
 * 数据来源：VPS 上的侧车脚本每 5 分钟查询 Xray StatsService
 * （user>>>paul>>>traffic>>>uplink / downlink），POST 到 /api/xray-stats。
 * Xray 计数器在 Xray 重启后清零，因此这里只存原始快照，
 * 区间用量由读取时对相邻快照求正向差分累计得到（数值回落视为重启，
 * 回落后的新值按重启后新增用量计入）。
 */
import { createSuccessResponse, createBadRequestResponse, createUnauthorizedResponse, createNotFoundResponse } from '../utils/errors.js';
import { checkAuth, hasAppPasswordAccess, simpleAuthResponse } from '../middleware/auth.js';
import { getServerDetail } from '../utils/cache.js';

export async function ensureXrayStatsTable(db) {
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS xray_stats (
      server_id TEXT NOT NULL,
      ts INTEGER NOT NULL,
      uplink INTEGER NOT NULL DEFAULT 0,
      downlink INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (server_id, ts)
    )
  `).run();
}

function normalizeSnapshotTs(value) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return Math.floor(Date.now() / 1000);
  // 毫秒时间戳归一化为秒
  return ts > 100000000000 ? Math.floor(ts / 1000) : Math.floor(ts);
}

// POST /api/xray-stats
// body: { id, secret, uplink, downlink, ts? }（ts 为秒或毫秒时间戳，缺省用服务端时间）
export async function handleXrayStatsPost(request, env) {
  let data;
  try {
    data = await request.json();
  } catch (_) {
    return createBadRequestResponse('invalidJson');
  }

  if (!data || data.secret !== env.API_SECRET) {
    return createUnauthorizedResponse('Invalid secret');
  }

  const serverId = typeof data.id === 'string' ? data.id.trim() : '';
  if (!serverId) return createBadRequestResponse('Missing ID');

  const uplink = Math.max(0, Math.floor(Number(data.uplink) || 0));
  const downlink = Math.max(0, Math.floor(Number(data.downlink) || 0));
  const ts = normalizeSnapshotTs(data.ts);

  await ensureXrayStatsTable(env.DB);

  const server = await getServerDetail(env.DB, serverId, true);
  if (!server) return createNotFoundResponse('Server not found');

  await env.DB.prepare(
    'INSERT OR REPLACE INTO xray_stats (server_id, ts, uplink, downlink) VALUES (?, ?, ?, ?)'
  ).bind(serverId, ts, uplink, downlink).run();

  return createSuccessResponse({ ok: true, ts });
}

// GET /api/xray-stats?id=<serverId>&hours=24
// 返回：最新快照（本次 Xray 会话累计）、区间内累计用量、按小时分桶的用量
export async function handleXrayStatsGet(request, env, sys) {
  const isLoggedIn = await checkAuth(request, env, sys);
  const canViewPrivate = isLoggedIn || hasAppPasswordAccess(request, sys);
  if (sys.is_public !== 'true' && !canViewPrivate) {
    return simpleAuthResponse();
  }

  const url = new URL(request.url);
  const serverId = (url.searchParams.get('id') || '').trim();
  if (!serverId) return createBadRequestResponse('Missing ID');

  let hours = parseFloat(url.searchParams.get('hours') || '24');
  if (!Number.isFinite(hours) || hours <= 0) hours = 24;
  hours = Math.min(hours, 24 * 7);

  await ensureXrayStatsTable(env.DB);

  const nowSec = Math.floor(Date.now() / 1000);
  const windowStart = nowSec - Math.floor(hours * 3600);

  // 区间起点之前的最后一条快照作为差分基线
  const baseline = await env.DB.prepare(
    'SELECT ts, uplink, downlink FROM xray_stats WHERE server_id = ? AND ts < ? ORDER BY ts DESC LIMIT 1'
  ).bind(serverId, windowStart).first();

  const rows = await env.DB.prepare(
    'SELECT ts, uplink, downlink FROM xray_stats WHERE server_id = ? AND ts >= ? ORDER BY ts ASC LIMIT 5000'
  ).bind(serverId, windowStart).all();
  const snaps = Array.isArray(rows?.results) ? rows.results : [];

  let totalUp = 0;
  let totalDown = 0;
  const hourlyMap = new Map();
  let prev = baseline || null;
  for (const s of snaps) {
    if (prev) {
      // 数值回落说明 Xray 重启过：回落后的新值即重启后新增用量
      const dUp = s.uplink >= prev.uplink ? s.uplink - prev.uplink : s.uplink;
      const dDown = s.downlink >= prev.downlink ? s.downlink - prev.downlink : s.downlink;
      totalUp += dUp;
      totalDown += dDown;
      const hourStart = Math.floor(s.ts / 3600) * 3600;
      const bucket = hourlyMap.get(hourStart) || { ts: hourStart, up: 0, down: 0 };
      bucket.up += dUp;
      bucket.down += dDown;
      hourlyMap.set(hourStart, bucket);
    }
    prev = s;
  }

  const latest = snaps.length > 0 ? snaps[snaps.length - 1] : (baseline || null);

  return createSuccessResponse({
    server_id: serverId,
    hours,
    latest: latest ? { ts: latest.ts, uplink: latest.uplink, downlink: latest.downlink } : null,
    total_up: totalUp,
    total_down: totalDown,
    samples: snaps.length,
    hourly: [...hourlyMap.values()].sort((a, b) => a.ts - b.ts)
  });
}
