import assert from 'node:assert/strict';
import test from 'node:test';
import { Miniflare } from 'miniflare';

import { handleXrayStatsPost, handleXrayStatsGet } from '../src/handlers/xrayStats.js';
import { clearAllCaches } from '../src/utils/cache.js';

const SERVER_ID = 'srv-xray-1';
const SECRET = 'test-secret';

function createMiniflare() {
  return new Miniflare({
    modules: true,
    script: 'export default { fetch() { return new Response("OK"); } }',
    d1Databases: { DB: 'xray-stats-test' }
  });
}

async function seedServer(db) {
  await db.prepare(
    'CREATE TABLE IF NOT EXISTS servers (id TEXT PRIMARY KEY, name TEXT)'
  ).run();
  await db.prepare(
    'INSERT OR REPLACE INTO servers (id, name) VALUES (?, ?)'
  ).bind(SERVER_ID, 'Oracle-SG').run();
}

function postRequest(body) {
  return new Request('http://localhost/api/xray-stats', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
}

test('xray stats: auth, ingest, restart-aware totals and hourly buckets', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    // 未带密钥 -> 401
    let res = await handleXrayStatsPost(postRequest({
      id: SERVER_ID, uplink: 10, downlink: 20
    }), env);
    assert.equal(res.status, 401);

    // 未知服务器 -> 404
    res = await handleXrayStatsPost(postRequest({
      id: 'no-such-server', secret: SECRET, uplink: 10, downlink: 20
    }), env);
    assert.equal(res.status, 404);

    const nowSec = Math.floor(Date.now() / 1000);
    const snap = async (ts, up, down) => {
      const r = await handleXrayStatsPost(postRequest({
        id: SERVER_ID, secret: SECRET, uplink: up, downlink: down, ts
      }), env);
      assert.equal(r.status, 200);
    };

    // 区间外基线（25 小时前）
    await snap(nowSec - 25 * 3600, 1000, 2000);
    // 区间内：正常增长、一次重启回落、再增长
    await snap(nowSec - 3600, 1500, 2600); // +500 / +600
    await snap(nowSec - 1800, 200, 300);   // 重启：+200 / +300
    await snap(nowSec - 60, 700, 900);     // +500 / +600

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();

    assert.equal(data.server_id, SERVER_ID);
    assert.equal(data.total_up, 1200);
    assert.equal(data.total_down, 1500);
    assert.equal(data.latest.uplink, 700);
    assert.equal(data.latest.downlink, 900);
    assert.equal(data.samples, 3);
    assert.ok(Array.isArray(data.hourly) && data.hourly.length >= 1);
    const hourlyUp = data.hourly.reduce((sum, h) => sum + h.up, 0);
    const hourlyDown = data.hourly.reduce((sum, h) => sum + h.down, 0);
    assert.equal(hourlyUp, 1200);
    assert.equal(hourlyDown, 1500);

    // 缺 id -> 400
    const badRes = await handleXrayStatsGet(
      new Request('http://localhost/api/xray-stats'),
      env, sys
    );
    assert.equal(badRes.status, 400);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: tunnel counters accumulate by diff and expose latest session', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const snap = async (ts, up, down, tunUp, tunDown) => {
      const body = { id: SERVER_ID, secret: SECRET, uplink: up, downlink: down, ts };
      if (tunUp != null) body.tunnel_up = tunUp;
      if (tunDown != null) body.tunnel_down = tunDown;
      const r = await handleXrayStatsPost(postRequest(body), env);
      assert.equal(r.status, 200);
    };

    // 区间外基线（25 小时前，无隧道字段）
    await snap(nowSec - 25 * 3600, 1000, 2000);
    // 区间内：隧道计数增长、一次通道重连回落、再增长
    // 首条带隧道值：与基线（无隧道值）不计差分
    await snap(nowSec - 3600, 1500, 2600, 100000, 900000);
    await snap(nowSec - 1800, 2000, 3200, 130000, 1200000); // +30000 / +300000
    await snap(nowSec - 900, 2500, 3800, 5000, 40000);      // 重连回落：+5000 / +40000
    await snap(nowSec - 60, 3000, 4400, 25000, 240000);     // +20000 / +200000

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();

    // 隧道区间累计：30000+5000+20000=55000；300000+40000+200000=540000
    assert.equal(data.tunnel_up, 55000);
    assert.equal(data.tunnel_down, 540000);
    // 最新会话原始值
    assert.deepEqual(data.tunnel_latest, { ts: nowSec - 60, up: 25000, down: 240000 });
    // Xray 自身的区间累计不受影响：4 个区间内快照各 +500 / +600
    assert.equal(data.total_up, 2000);
    assert.equal(data.total_down, 2400);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: tunnel today/month totals use Shanghai boundaries', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const SHANGHAI = 8 * 3600;
    const dayStart = Math.floor((nowSec + SHANGHAI) / 86400) * 86400 - SHANGHAI;
    const d = new Date((nowSec + SHANGHAI) * 1000);
    const monthStart = Math.floor(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000) - SHANGHAI;
    const isFirst = dayStart === monthStart;
    // 距东八区午夜不足 1 小时时，今日快照可能跨日，跳过以免误报
    if (nowSec - dayStart < 3600) return;

    const snap = async (ts, tunUp, tunDown) => {
      const body = { id: SERVER_ID, secret: SECRET, uplink: 100, downlink: 200, ts, tunnel_up: tunUp, tunnel_down: tunDown };
      const r = await handleXrayStatsPost(postRequest(body), env);
      assert.equal(r.status, 200);
    };

    // 月初前基线
    await snap(monthStart - 7200, 1000, 2000);
    // 昨日（当月内，除非今天是 1 号）
    await snap(dayStart - 3600, 3000, 6000);
    // 今日：增长、一次通道重连回落、再增长
    await snap(nowSec - 3600, 5000, 10000); // 相对昨日 +2000 / +4000
    await snap(nowSec - 1800, 500, 1000);   // 重连回落：+500 / +1000
    await snap(nowSec - 60, 1500, 3000);    // +1000 / +2000

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();

    // 当天累计（东八区）：2000+500+1000=3500；4000+1000+2000=7000
    assert.equal(data.tunnel_today_up, 3500);
    assert.equal(data.tunnel_today_down, 7000);
    // 当月累计（东八区）：1 号当天与当天相同，否则多出昨日部分
    assert.equal(data.tunnel_month_up, isFirst ? 3500 : 5500);
    assert.equal(data.tunnel_month_down, isFirst ? 7000 : 11000);
    // 当月应 >= 当天
    assert.ok(data.tunnel_month_up >= data.tunnel_today_up);
    assert.ok(data.tunnel_month_down >= data.tunnel_today_down);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: today total combines tunnel and Xray for the Shanghai day', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const SHANGHAI = 8 * 3600;
    const dayStart = Math.floor((nowSec + SHANGHAI) / 86400) * 86400 - SHANGHAI;
    // 距东八区午夜不足 1 小时时，今日快照可能跨日，跳过以免误报
    if (nowSec - dayStart < 3600) return;

    const snap = async (ts, up, down, ipUp, ipDown, tunUp, tunDown) => {
      const body = { id: SERVER_ID, secret: SECRET, uplink: up, downlink: down, ts };
      if (ipUp != null) body.iphone_up = ipUp;
      if (ipDown != null) body.iphone_down = ipDown;
      if (tunUp != null) body.tunnel_up = tunUp;
      if (tunDown != null) body.tunnel_down = tunDown;
      const r = await handleXrayStatsPost(postRequest(body), env);
      assert.equal(r.status, 200);
    };

    // 昨日基线（东八区昨日，带 Xray + iPhone + 隧道值）
    await snap(dayStart - 3600, 10000, 20000, 1000, 2000, 5000, 10000);
    // 今日：Xray 正常增长、一次重启回落、再增长；iPhone 同步增长；
    // 隧道增长、一次通道重连回落、再增长
    await snap(nowSec - 3600, 11000, 22000, 1500, 3000, 8000, 16000); // Xray mac +1000/+2000, iphone +500/+1000; 隧道 +3000/+6000
    await snap(nowSec - 1800, 500, 800, 200, 300, 500, 1000);         // Xray 重启：mac +500/+800, iphone +200/+300; 隧道重连：+500/+1000
    await snap(nowSec - 60, 1500, 2800, 1200, 2300, 1500, 3000);      // Xray mac +1000/+2000, iphone +1000/+2000; 隧道 +1000/+2000

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();

    // 隧道当天：up 3000+500+1000=4500, down 6000+1000+2000=9000
    assert.equal(data.tunnel_today_up, 4500);
    assert.equal(data.tunnel_today_down, 9000);
    // Xray 当天：mac up 1000+500+1000=2500, down 2000+800+2000=4800；
    // iPhone up 500+200+1000=1700, down 1000+300+2000=3300；
    // Xray 当天合计 up 4200, down 8100
    // 今日总计 = 隧道当天 + Xray 当天
    assert.equal(data.today_total_up, 4500 + 4200);
    assert.equal(data.today_total_down, 9000 + 8100);
    // 今日总计应 >= 隧道当天（Xray 为正）
    assert.ok(data.today_total_up >= data.tunnel_today_up);
    assert.ok(data.today_total_down >= data.tunnel_today_down);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: today total is null only with no snapshots at all', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    // 一条快照都不发：没有任何代理数据
    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();
    assert.equal(data.today_total_up, null);
    assert.equal(data.today_total_down, null);
    assert.equal(data.tunnel_today_up, null);
    assert.equal(data.tunnel_today_down, null);
    assert.equal(data.latest, null);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: per-user monthly totals and iphone quota', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const d = new Date(nowSec * 1000);
    const mStart = Math.floor(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000);
    const snap = async (ts, up, down, ipUp, ipDown) => {
      const body = { id: SERVER_ID, secret: SECRET, uplink: up, downlink: down, ts };
      if (ipUp != null) body.iphone_up = ipUp;
      if (ipDown != null) body.iphone_down = ipDown;
      const r = await handleXrayStatsPost(postRequest(body), env);
      assert.equal(r.status, 200);
    };

    // 月初前基线（带 iPhone 值）
    await snap(mStart - 3600, 10000, 20000, 1000, 2000);
    // 月内：正常增长、一次重启回落、再增长
    await snap(mStart + 3600, 11000, 22000, 1500, 3000); // mac +1000/+2000, iphone +500/+1000
    await snap(mStart + 7200, 500, 800, 200, 300);       // 重启：mac +500/+800, iphone +200/+300
    await snap(mStart + 10800, 1500, 2800, 1200, 2300);  // mac +1000/+2000, iphone +1000/+2000

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();

    // Mac（paul）月累计：up 2500，down 4800
    assert.deepEqual(data.users.mac, { up: 2500, down: 4800 });
    // iPhone 月累计：up 1700，down 3300，合计 5000
    assert.deepEqual(data.users.iphone, { up: 1700, down: 3300 });
    assert.equal(data.iphone_quota, 10737418240);
    assert.equal(data.iphone_used_percent, 0);
    assert.equal(data.iphone_over_quota, false);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: users.iphone is null before any iphone snapshot', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const r = await handleXrayStatsPost(postRequest({
      id: SERVER_ID, secret: SECRET, uplink: 100, downlink: 200, ts: nowSec - 60
    }), env);
    assert.equal(r.status, 200);

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();
    assert.deepEqual(data.users.mac, { up: 0, down: 0 });
    assert.equal(data.users.iphone, null);
    assert.equal(data.iphone_over_quota, false);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: tunnel fields are null before any tunnel snapshot', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const r = await handleXrayStatsPost(postRequest({
      id: SERVER_ID, secret: SECRET, uplink: 100, downlink: 200, ts: nowSec - 60
    }), env);
    assert.equal(r.status, 200);

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();
    assert.equal(data.tunnel_up, null);
    assert.equal(data.tunnel_down, null);
    assert.equal(data.tunnel_latest, null);
    assert.equal(data.tunnel_today_up, null);
    assert.equal(data.tunnel_today_down, null);
    assert.equal(data.tunnel_month_up, null);
    assert.equal(data.tunnel_month_down, null);
    // 有 Xray 快照但无隧道数据时，今日总计不为 null（等于 Xray 当天累计，
    // 这里当天只有一条快照、无差分可算，故为 0）
    assert.equal(data.today_total_up, 0);
    assert.equal(data.today_total_down, 0);
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: per-protocol monthly totals with counter reset', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const d = new Date(nowSec * 1000);
    const mStart = Math.floor(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000);
    const proto = (hy2In, hy2Out, tuicIn, tuicOut, anyIn, anyOut, trIn, trOut) => ({
      proto_hy2_in: hy2In, proto_hy2_out: hy2Out,
      proto_tuic_in: tuicIn, proto_tuic_out: tuicOut,
      proto_anytls_in: anyIn, proto_anytls_out: anyOut,
      proto_trojan_in: trIn, proto_trojan_out: trOut
    });
    const snap = async (ts, extra) => {
      const body = { id: SERVER_ID, secret: SECRET, uplink: 100, downlink: 200, ts, ...extra };
      const r = await handleXrayStatsPost(postRequest(body), env);
      assert.equal(r.status, 200);
    };

    // 月初前基线（带分协议值）
    await snap(mStart - 3600, proto(1000, 2000, 500, 600, 100, 200, 300, 400));
    // 月内：正常增长
    await snap(mStart + 3600, proto(1500, 3000, 700, 900, 150, 300, 400, 500));
    // 规则重建导致计数器清零：回落后的新值按新增用量计入
    await snap(mStart + 7200, proto(200, 300, 50, 60, 10, 20, 30, 40));
    // 再增长
    await snap(mStart + 10800, proto(1200, 1300, 150, 160, 110, 120, 130, 140));

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();

    // hy2: in 500+200+1000=1700, out 1000+300+1000=2300
    assert.deepEqual(data.protocols.hy2, { up: 2300, down: 1700 });
    // tuic: in 200+50+100=350, out 300+60+100=460
    assert.deepEqual(data.protocols.tuic, { up: 460, down: 350 });
    // anytls: in 50+10+100=160, out 100+20+100=220
    assert.deepEqual(data.protocols.anytls, { up: 220, down: 160 });
    // trojan: in 100+30+100=230, out 100+40+100=240
    assert.deepEqual(data.protocols.trojan, { up: 240, down: 230 });
  } finally {
    await miniflare.dispose();
  }
});

test('xray stats: protocols is null before any proto snapshot', async () => {
  const miniflare = createMiniflare();
  try {
    const db = await miniflare.getD1Database('DB');
    await seedServer(db);
    clearAllCaches();
    const env = { DB: db, API_SECRET: SECRET };
    const sys = { is_public: 'true' };

    const nowSec = Math.floor(Date.now() / 1000);
    const r = await handleXrayStatsPost(postRequest({
      id: SERVER_ID, secret: SECRET, uplink: 100, downlink: 200, ts: nowSec - 60
    }), env);
    assert.equal(r.status, 200);

    const getRes = await handleXrayStatsGet(
      new Request(`http://localhost/api/xray-stats?id=${SERVER_ID}&hours=24`),
      env, sys
    );
    assert.equal(getRes.status, 200);
    const data = await getRes.json();
    assert.equal(data.protocols, null);
  } finally {
    await miniflare.dispose();
  }
});
