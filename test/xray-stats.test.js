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
  } finally {
    await miniflare.dispose();
  }
});
