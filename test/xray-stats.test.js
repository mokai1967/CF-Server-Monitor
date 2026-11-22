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
