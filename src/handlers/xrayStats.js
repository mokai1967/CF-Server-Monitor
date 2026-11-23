/**
 * Xray 代理流量统计（独立于探针主指标表）
 *
 * 数据来源：VPS 上的侧车脚本每 5 分钟查询 Xray StatsService
 * （user>>>paul>>>traffic>>>uplink / downlink），POST 到 /api/xray-stats。
 * Xray 计数器在 Xray 重启后清零，因此这里只存原始快照，
 * 区间用量由读取时对相邻快照求正向差分累计得到（数值回落视为重启，
 * 回落后的新值按重启后新增用量计入）。
 *
 * 侧车同时上报两个附加字段（2026-11-23 起）：
 * - device_count：最近 5 分钟内 Xray 访问日志里出现过的不同来源 IP 数
 *   （近似值；VLESS 经 Caddy 转发只算 1 台，见侧车脚本注释）。
 * - monthly_tx：vnStat 当月网卡出站总量（字节），覆盖全部 5 个协议。
 * 两者都是“最新值”语义（非累计计数器），读取时取最新一条非空快照。
 *
 * 侧车还上报隧道流量（2026-11-23 起）：
 * - tunnel_up / tunnel_down：Caddy /status 里 node1 的 traffic.bytes_in /
 *   bytes_out（当前通道累计字节；通道重连会清零）。与 Xray 计数器一样，
 *   这里只存原始快照，区间用量由读取时对相邻快照求正向差分累计得到
 *   （数值回落视为通道重连，回落后的新值按重连后新增用量计入）。
 *
 * 按设备分开统计（2026-11-23 起）：Xray 有两个用户，paul = Mac，
 * iphone = iPhone。uplink / downlink 沿用 paul 的计数（Mac）；侧车另报
 * iphone_up / iphone_down。GET 返回两用户各自的自然月累计（UTC 自然月），
 * 以及 iPhone 的 10 GiB 月上限与使用比例。
 *
 * 分协议统计（2026-11-23 起）：nftables 按端口+协议计字节，侧车每 5 分钟
 * 上报 8 个累计计数器（proto_hy2_in/out、proto_tuic_in/out、
 * proto_anytls_in/out、proto_trojan_in/out）。计数器在 nft 规则重建后
 * 清零，与 Xray/隧道计数器一样只存原始快照，GET 用正向差分累计自然月
 * 用量。VLESS 不走端口计数（已由 Xray stats 覆盖）；Trojan 计入作对照。
 */
import { createSuccessResponse, createBadRequestResponse, createUnauthorizedResponse, createNotFoundResponse } from '../utils/errors.js';
import { checkAuth, hasAppPasswordAccess, simpleAuthResponse } from '../middleware/auth.js';
import { getServerDetail } from '../utils/cache.js';

// 订阅总流量上限：1 TiB / 自然月
export const MONTHLY_QUOTA_BYTES = 1099511627776;

// iPhone 用户月上限：10 GiB（上行+下行合计）/ 自然月
export const IPHONE_QUOTA_BYTES = 10737418240;

// 自然月起始（UTC）：当月 1 号 00:00:00
function monthStartSec(nowSec) {
  const d = new Date(nowSec * 1000);
  return Math.floor(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000);
}

// 东八区（Asia/Shanghai，无夏令时）日/月起始：隧道「当天/当月」累计按
// paul 所在的东八区边界算（与 Xray 自然月累计用的 UTC 口径不同，
// 因为这是给 paul 看的“当天/当月”）。
const SHANGHAI_OFFSET_SEC = 8 * 3600;
function shanghaiDayStartSec(nowSec) {
  return Math.floor((nowSec + SHANGHAI_OFFSET_SEC) / 86400) * 86400 - SHANGHAI_OFFSET_SEC;
}
function shanghaiMonthStartSec(nowSec) {
  const d = new Date((nowSec + SHANGHAI_OFFSET_SEC) * 1000);
  return Math.floor(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000) - SHANGHAI_OFFSET_SEC;
}

// 隧道累计（正向差分）：从 startSec 起的快照求相邻正向差分累计，
// 计数器回落按通道重连清零处理（回落后的新值即重连后新增用量）。
// 与区间累计、自然月累计同一逻辑；不改 D1 表结构、不改侧车上报。
async function tunnelCumulative(db, serverId, startSec) {
  const baseline = await db.prepare(
    'SELECT ts, tunnel_up, tunnel_down FROM xray_stats WHERE server_id = ? AND ts < ? ORDER BY ts DESC LIMIT 1'
  ).bind(serverId, startSec).first();
  const rows = await db.prepare(
    'SELECT ts, tunnel_up, tunnel_down FROM xray_stats WHERE server_id = ? AND ts >= ? ORDER BY ts ASC LIMIT 5000'
  ).bind(serverId, startSec).all();
  const snaps = Array.isArray(rows?.results) ? rows.results : [];
  let up = 0;
  let down = 0;
  let prev = baseline || null;
  for (const s of snaps) {
    if (prev && prev.tunnel_up != null && prev.tunnel_down != null && s.tunnel_up != null && s.tunnel_down != null) {
      up += s.tunnel_up >= prev.tunnel_up ? s.tunnel_up - prev.tunnel_up : s.tunnel_up;
      down += s.tunnel_down >= prev.tunnel_down ? s.tunnel_down - prev.tunnel_down : s.tunnel_down;
    }
    prev = s;
  }
  return { up, down };
}

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
  // 2026-11-23 新增列：在线设备数、月出站总量、隧道流量。老表用 ALTER 补列。
  // 2026-11-23 新增列：iPhone 用户计数器（按设备分开统计）。
  // 2026-11-23 新增列：分协议 nft 计数器（HY2/TUIC/AnyTLS/Trojan 各上下行）。
  for (const col of ['device_count INTEGER', 'monthly_tx INTEGER', 'tunnel_up INTEGER', 'tunnel_down INTEGER', 'iphone_up INTEGER', 'iphone_down INTEGER', 'proto_hy2_in INTEGER', 'proto_hy2_out INTEGER', 'proto_tuic_in INTEGER', 'proto_tuic_out INTEGER', 'proto_anytls_in INTEGER', 'proto_anytls_out INTEGER', 'proto_trojan_in INTEGER', 'proto_trojan_out INTEGER']) {
    try {
      await db.prepare(`ALTER TABLE xray_stats ADD COLUMN ${col}`).run();
    } catch (_) {
      // 列已存在，忽略
    }
  }
}

function normalizeSnapshotTs(value) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return Math.floor(Date.now() / 1000);
  // 毫秒时间戳归一化为秒
  return ts > 100000000000 ? Math.floor(ts / 1000) : Math.floor(ts);
}

// POST /api/xray-stats
// body: { id, secret, uplink, downlink, ts?, device_count?, monthly_tx?, tunnel_up?, tunnel_down?, iphone_up?, iphone_down?, proto_hy2_in?, proto_hy2_out?, proto_tuic_in?, proto_tuic_out?, proto_anytls_in?, proto_anytls_out?, proto_trojan_in?, proto_trojan_out? }
// （ts 为秒或毫秒时间戳，缺省用服务端时间；device_count / monthly_tx 为
// 可选的最新值语义字段，缺省时不覆盖已有值；tunnel_up / tunnel_down 为
// 可选的隧道通道累计计数器，缺省时存 NULL；iphone_up / iphone_down 为
// 可选的 iPhone 用户累计计数器，缺省时存 NULL；proto_* 为可选的分协议
// nft 累计计数器，缺省时存 NULL）
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
  const deviceCount = data.device_count == null ? null : Math.max(0, Math.floor(Number(data.device_count) || 0));
  const monthlyTx = data.monthly_tx == null ? null : Math.max(0, Math.floor(Number(data.monthly_tx) || 0));
  const tunnelUp = data.tunnel_up == null ? null : Math.max(0, Math.floor(Number(data.tunnel_up) || 0));
  const tunnelDown = data.tunnel_down == null ? null : Math.max(0, Math.floor(Number(data.tunnel_down) || 0));
  const iphoneUp = data.iphone_up == null ? null : Math.max(0, Math.floor(Number(data.iphone_up) || 0));
  const iphoneDown = data.iphone_down == null ? null : Math.max(0, Math.floor(Number(data.iphone_down) || 0));
  const protoNum = (v) => (v == null ? null : Math.max(0, Math.floor(Number(v) || 0)));
  const protoHy2In = protoNum(data.proto_hy2_in);
  const protoHy2Out = protoNum(data.proto_hy2_out);
  const protoTuicIn = protoNum(data.proto_tuic_in);
  const protoTuicOut = protoNum(data.proto_tuic_out);
  const protoAnytlsIn = protoNum(data.proto_anytls_in);
  const protoAnytlsOut = protoNum(data.proto_anytls_out);
  const protoTrojanIn = protoNum(data.proto_trojan_in);
  const protoTrojanOut = protoNum(data.proto_trojan_out);

  await ensureXrayStatsTable(env.DB);

  const server = await getServerDetail(env.DB, serverId, true);
  if (!server) return createNotFoundResponse('Server not found');

  await env.DB.prepare(
    'INSERT OR REPLACE INTO xray_stats (server_id, ts, uplink, downlink, device_count, monthly_tx, tunnel_up, tunnel_down, iphone_up, iphone_down, proto_hy2_in, proto_hy2_out, proto_tuic_in, proto_tuic_out, proto_anytls_in, proto_anytls_out, proto_trojan_in, proto_trojan_out) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
  ).bind(serverId, ts, uplink, downlink, deviceCount, monthlyTx, tunnelUp, tunnelDown, iphoneUp, iphoneDown, protoHy2In, protoHy2Out, protoTuicIn, protoTuicOut, protoAnytlsIn, protoAnytlsOut, protoTrojanIn, protoTrojanOut).run();

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
    'SELECT ts, uplink, downlink, tunnel_up, tunnel_down FROM xray_stats WHERE server_id = ? AND ts < ? ORDER BY ts DESC LIMIT 1'
  ).bind(serverId, windowStart).first();

  const rows = await env.DB.prepare(
    'SELECT ts, uplink, downlink, tunnel_up, tunnel_down FROM xray_stats WHERE server_id = ? AND ts >= ? ORDER BY ts ASC LIMIT 5000'
  ).bind(serverId, windowStart).all();
  const snaps = Array.isArray(rows?.results) ? rows.results : [];

  let totalUp = 0;
  let totalDown = 0;
  let tunnelTotalUp = 0;
  let tunnelTotalDown = 0;
  const hourlyMap = new Map();
  let prev = baseline || null;
  for (const s of snaps) {
    if (prev) {
      // 数值回落说明 Xray 重启过：回落后的新值即重启后新增用量
      const dUp = s.uplink >= prev.uplink ? s.uplink - prev.uplink : s.uplink;
      const dDown = s.downlink >= prev.downlink ? s.downlink - prev.downlink : s.downlink;
      totalUp += dUp;
      totalDown += dDown;
      // 隧道计数器同理（通道重连会清零）；任一端缺隧道值时跳过该对快照
      if (prev.tunnel_up != null && prev.tunnel_down != null && s.tunnel_up != null && s.tunnel_down != null) {
        tunnelTotalUp += s.tunnel_up >= prev.tunnel_up ? s.tunnel_up - prev.tunnel_up : s.tunnel_up;
        tunnelTotalDown += s.tunnel_down >= prev.tunnel_down ? s.tunnel_down - prev.tunnel_down : s.tunnel_down;
      }
      const hourStart = Math.floor(s.ts / 3600) * 3600;
      const bucket = hourlyMap.get(hourStart) || { ts: hourStart, up: 0, down: 0 };
      bucket.up += dUp;
      bucket.down += dDown;
      hourlyMap.set(hourStart, bucket);
    }
    prev = s;
  }

  const latest = snaps.length > 0 ? snaps[snaps.length - 1] : (baseline || null);

  // 附加字段取最新一条非空快照（最新值语义；侧车某次算不出时不覆盖旧值）
  const latestDevice = await env.DB.prepare(
    'SELECT device_count FROM xray_stats WHERE server_id = ? AND device_count IS NOT NULL ORDER BY ts DESC LIMIT 1'
  ).bind(serverId).first();
  const latestMonthly = await env.DB.prepare(
    'SELECT monthly_tx FROM xray_stats WHERE server_id = ? AND monthly_tx IS NOT NULL ORDER BY ts DESC LIMIT 1'
  ).bind(serverId).first();
  const monthlyTx = latestMonthly?.monthly_tx ?? null;
  const usedPercent = monthlyTx == null ? 0 : +(monthlyTx / MONTHLY_QUOTA_BYTES * 100).toFixed(1);
  const overQuota = monthlyTx != null && monthlyTx > MONTHLY_QUOTA_BYTES;

  // 隧道最新会话原始值（最新一条带隧道计数的快照）；此前从未上报过时为 null，
  // 此时区间累计也一并返回 null，前端据此隐藏隧道行。
  const latestTunnel = await env.DB.prepare(
    'SELECT ts, tunnel_up, tunnel_down FROM xray_stats WHERE server_id = ? AND tunnel_up IS NOT NULL AND tunnel_down IS NOT NULL ORDER BY ts DESC LIMIT 1'
  ).bind(serverId).first();
  const tunnelLatest = latestTunnel ? { ts: latestTunnel.ts, up: latestTunnel.tunnel_up, down: latestTunnel.tunnel_down } : null;

  // 隧道当天/当月累计（东八区日/月边界，正向差分累计，与区间累计同一逻辑）。
  // 当天从东八区 00:00 起算，当月从东八区当月 1 号 00:00 起算；隧道上报
  // 2026-11-23 才开始，本月是部分月。
  const tunnelToday = await tunnelCumulative(env.DB, serverId, shanghaiDayStartSec(nowSec));
  const tunnelMonth = await tunnelCumulative(env.DB, serverId, shanghaiMonthStartSec(nowSec));

  // 按设备分开的自然月累计（UTC 自然月）：Mac = paul 的 uplink/downlink，
  // iPhone = iphone_up/iphone_down。计数器重启会清零，用正向差分累计，
  // 与区间累计同一逻辑；缺 iPhone 值的快照对不计入 iPhone。
  // 分协议自然月累计同理：nft 计数器在规则重建后清零，用正向差分累计；
  // 缺分协议值的快照对不计入该协议。
  const mStart = monthStartSec(nowSec);
  const mBaseline = await env.DB.prepare(
    'SELECT ts, uplink, downlink, iphone_up, iphone_down, proto_hy2_in, proto_hy2_out, proto_tuic_in, proto_tuic_out, proto_anytls_in, proto_anytls_out, proto_trojan_in, proto_trojan_out FROM xray_stats WHERE server_id = ? AND ts < ? ORDER BY ts DESC LIMIT 1'
  ).bind(serverId, mStart).first();
  const mRows = await env.DB.prepare(
    'SELECT ts, uplink, downlink, iphone_up, iphone_down, proto_hy2_in, proto_hy2_out, proto_tuic_in, proto_tuic_out, proto_anytls_in, proto_anytls_out, proto_trojan_in, proto_trojan_out FROM xray_stats WHERE server_id = ? AND ts >= ? ORDER BY ts ASC LIMIT 5000'
  ).bind(serverId, mStart).all();
  const mSnaps = Array.isArray(mRows?.results) ? mRows.results : [];
  let macUp = 0, macDown = 0, iphoneUp = 0, iphoneDown = 0, hasIphone = false;
  let protoHy2Up = 0, protoHy2Down = 0, protoTuicUp = 0, protoTuicDown = 0;
  let protoAnytlsUp = 0, protoAnytlsDown = 0, protoTrojanUp = 0, protoTrojanDown = 0;
  let hasProto = false;
  const protoKeys = ['proto_hy2_in', 'proto_hy2_out', 'proto_tuic_in', 'proto_tuic_out', 'proto_anytls_in', 'proto_anytls_out', 'proto_trojan_in', 'proto_trojan_out'];
  const protoDiff = (prev, cur, key) => {
    if (prev == null || cur == null || prev[key] == null || cur[key] == null) return 0;
    return cur[key] >= prev[key] ? cur[key] - prev[key] : cur[key];
  };
  if (mBaseline && protoKeys.every((k) => mBaseline[k] != null)) hasProto = true;
  let mPrev = mBaseline || null;
  for (const s of mSnaps) {
    if (s.iphone_up != null && s.iphone_down != null) hasIphone = true;
    if (protoKeys.every((k) => s[k] != null)) hasProto = true;
    if (mPrev) {
      macUp += s.uplink >= mPrev.uplink ? s.uplink - mPrev.uplink : s.uplink;
      macDown += s.downlink >= mPrev.downlink ? s.downlink - mPrev.downlink : s.downlink;
      if (mPrev.iphone_up != null && mPrev.iphone_down != null && s.iphone_up != null && s.iphone_down != null) {
        iphoneUp += s.iphone_up >= mPrev.iphone_up ? s.iphone_up - mPrev.iphone_up : s.iphone_up;
        iphoneDown += s.iphone_down >= mPrev.iphone_down ? s.iphone_down - mPrev.iphone_down : s.iphone_down;
      }
      protoHy2Down += protoDiff(mPrev, s, 'proto_hy2_in');
      protoHy2Up += protoDiff(mPrev, s, 'proto_hy2_out');
      protoTuicDown += protoDiff(mPrev, s, 'proto_tuic_in');
      protoTuicUp += protoDiff(mPrev, s, 'proto_tuic_out');
      protoAnytlsDown += protoDiff(mPrev, s, 'proto_anytls_in');
      protoAnytlsUp += protoDiff(mPrev, s, 'proto_anytls_out');
      protoTrojanDown += protoDiff(mPrev, s, 'proto_trojan_in');
      protoTrojanUp += protoDiff(mPrev, s, 'proto_trojan_out');
    }
    mPrev = s;
  }
  const iphoneTotal = iphoneUp + iphoneDown;
  const iphoneUsedPercent = +(iphoneTotal / IPHONE_QUOTA_BYTES * 100).toFixed(1);
  const iphoneOverQuota = iphoneTotal > IPHONE_QUOTA_BYTES;
  const protocols = hasProto ? {
    hy2: { up: protoHy2Up, down: protoHy2Down },
    tuic: { up: protoTuicUp, down: protoTuicDown },
    anytls: { up: protoAnytlsUp, down: protoAnytlsDown },
    trojan: { up: protoTrojanUp, down: protoTrojanDown }
  } : null;

  return createSuccessResponse({
    server_id: serverId,
    hours,
    latest: latest ? { ts: latest.ts, uplink: latest.uplink, downlink: latest.downlink } : null,
    total_up: totalUp,
    total_down: totalDown,
    samples: snaps.length,
    hourly: [...hourlyMap.values()].sort((a, b) => a.ts - b.ts),
    device_count: latestDevice?.device_count ?? null,
    monthly_tx: monthlyTx,
    quota: MONTHLY_QUOTA_BYTES,
    used_percent: usedPercent,
    over_quota: overQuota,
    tunnel_up: tunnelLatest ? tunnelTotalUp : null,
    tunnel_down: tunnelLatest ? tunnelTotalDown : null,
    tunnel_latest: tunnelLatest,
    tunnel_today_up: tunnelLatest ? tunnelToday.up : null,
    tunnel_today_down: tunnelLatest ? tunnelToday.down : null,
    tunnel_month_up: tunnelLatest ? tunnelMonth.up : null,
    tunnel_month_down: tunnelLatest ? tunnelMonth.down : null,
    users: {
      mac: { up: macUp, down: macDown },
      iphone: hasIphone ? { up: iphoneUp, down: iphoneDown } : null
    },
    iphone_quota: IPHONE_QUOTA_BYTES,
    iphone_used_percent: iphoneUsedPercent,
    iphone_over_quota: iphoneOverQuota,
    protocols
  });
}
