// GREEN PHONE EDIT
// Tile-topia room and leaderboard server.
// WebSocket rooms keep cross device games in sync. The game rules stay in the
// browser. This server only checks whose turn it is, keeps the newest state in
// Redis, and sends it to everyone in the room. HTTP routes hold the leaderboards.
//
// Environment:
//   PORT       set by Railway
//   REDIS_URL  set by the Railway Redis service. Without it the server keeps
//              everything in memory, which is fine for local testing only.

'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const ROOM_TTL = 24 * 60 * 60;          // rooms expire one day after the last write
const SWAP_MS = +process.env.SWAP_MS || 2 * 60 * 1000;         // a human away this long becomes a bot
const EVICT_MS = 60 * 60 * 1000;        // empty rooms leave the memory cache after an hour
const MAX_SEATS = 4;
const MAX_STATE = 4 * 1024 * 1024;
const MODES = ['heart', 'sprint', 'classic', 'daily'];
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ';

// ---------- storage ----------
// One small interface so Redis and the in memory fallback behave the same.
function memoryStore() {
  const kv = new Map(), zs = new Map();
  function alive(k) { const e = kv.get(k); if (!e) return null; if (e.exp && e.exp < Date.now()) { kv.delete(k); return null; } return e; }
  return {
    kind: 'memory',
    async get(k) { const e = alive(k); return e ? e.v : null; },
    async set(k, v, ttl) { kv.set(k, { v, exp: ttl ? Date.now() + ttl * 1000 : 0 }); },
    async setnx(k, v, ttl) { if (alive(k)) return false; kv.set(k, { v, exp: ttl ? Date.now() + ttl * 1000 : 0 }); return true; },
    async zadd(k, score, m) { if (!zs.has(k)) zs.set(k, new Map()); zs.get(k).set(m, score); },
    async expire() {},
    async ztop(k, n) {
      const z = zs.get(k); if (!z) return [];
      return [...z.entries()].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? 1 : -1)).slice(0, n).map(e => ({ m: e[0], s: e[1] }));
    },
    async zrank(k, m) {
      const z = zs.get(k); if (!z || !z.has(m)) return null;
      const s = z.get(m); let r = 0; z.forEach((v, key) => { if (v > s || (v === s && key > m)) r++; }); return r;
    },
    async zcard(k) { const z = zs.get(k); return z ? z.size : 0; }
  };
}
function redisStore(url) {
  const Redis = require('ioredis');
  const r = new Redis(url, { maxRetriesPerRequest: 3 });
  r.on('error', e => console.error('redis', e.message));
  return {
    kind: 'redis',
    get: k => r.get(k),
    set: (k, v, ttl) => ttl ? r.set(k, v, 'EX', ttl) : r.set(k, v),
    setnx: async (k, v, ttl) => (await r.set(k, v, 'EX', ttl, 'NX')) === 'OK',
    zadd: (k, s, m) => r.zadd(k, s, m),
    expire: (k, ttl) => r.expire(k, ttl),
    async ztop(k, n) {
      const a = await r.zrevrange(k, 0, n - 1, 'WITHSCORES'); const out = [];
      for (let i = 0; i < a.length; i += 2) out.push({ m: a[i], s: +a[i + 1] });
      return out;
    },
    zrank: (k, m) => r.zrevrank(k, m),
    zcard: k => r.zcard(k)
  };
}
const store = process.env.REDIS_URL ? redisStore(process.env.REDIS_URL) : memoryStore();

// ---------- rooms ----------
const rooms = new Map();   // code -> room (cache, Redis is the copy that survives restarts)
const live = new Map();    // code -> { socks:Set, away:Map(seat->since), timers:Map, evict }

const roomKey = c => 'tt:room:' + c;
function liveOf(code) {
  let l = live.get(code);
  if (!l) { l = { socks: new Set(), away: new Map(), timers: new Map(), evict: null }; live.set(code, l); }
  return l;
}
async function saveRoom(room) { await store.set(roomKey(room.code), JSON.stringify(room), ROOM_TTL); }
async function loadRoom(code) {
  if (rooms.has(code)) return rooms.get(code);
  const raw = await store.get(roomKey(code));
  if (!raw) return null;
  const room = JSON.parse(raw);
  rooms.set(code, room);
  // After a restart nobody is connected yet. Start the away clock now for every human seat.
  const l = liveOf(code);
  room.seats.forEach((s, i) => { if (!s.open && !s.bot) markAway(room, i, l); });
  return room;
}
function newCode() { let c = ''; for (let i = 0; i < 4; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]; return c; }

// One queue per room so two messages never edit the same room at once.
const locks = new Map();
function withRoom(code, fn) {
  const prev = locks.get(code) || Promise.resolve();
  const next = prev.then(fn, fn).catch(e => console.error('room', code, e));
  locks.set(code, next);
  next.finally(() => { if (locks.get(code) === next) locks.delete(code); });
  return next;
}

function connectedSeats(code) {
  const on = new Set(); const l = live.get(code);
  if (l) l.socks.forEach(ws => { if (ws.seat != null) on.add(ws.seat); });
  return on;
}
// Bots run on the connected human with the lowest seat number.
function botRunner(room) {
  const on = connectedSeats(room.code);
  for (let i = 0; i < room.seats.length; i++) { const s = room.seats[i]; if (!s.bot && !s.open && on.has(i)) return i; }
  return -1;
}
function presence(room) {
  const on = connectedSeats(room.code), l = liveOf(room.code), now = Date.now();
  return {
    t: 'presence', code: room.code, started: room.started, v: room.v, mode: room.mode, botRunner: botRunner(room),
    seats: room.seats.map((s, i) => ({
      name: s.name, bot: !!s.bot, open: !!s.open, swapped: !!s.swapped, on: on.has(i),
      away: (!s.bot && !s.open && !on.has(i) && l.away.has(i)) ? Math.floor((now - l.away.get(i)) / 1000) : 0
    }))
  };
}
function send(ws, msg) { if (ws.readyState === 1) ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); }
function broadcast(code, msg, except) {
  const l = live.get(code); if (!l) return;
  const s = JSON.stringify(msg);
  l.socks.forEach(ws => { if (ws !== except) send(ws, s); });
}
function markAway(room, seat, l) {
  l = l || liveOf(room.code);
  if (!l.away.has(seat)) l.away.set(seat, Date.now());
  clearTimeout(l.timers.get(seat));
  l.timers.set(seat, setTimeout(() => withRoom(room.code, async () => {
    l.timers.delete(seat);
    if (connectedSeats(room.code).has(seat)) return;
    const s = room.seats[seat];
    if (!room.started || !s || s.bot || s.open) return;
    s.bot = true; s.swapped = true;
    await saveRoom(room);
    broadcast(room.code, presence(room));
  }), SWAP_MS));
}
function clearAway(code, seat) {
  const l = liveOf(code); l.away.delete(seat);
  clearTimeout(l.timers.get(seat)); l.timers.delete(seat);
}
function scheduleEvict(code) {
  const l = liveOf(code); clearTimeout(l.evict);
  l.evict = setTimeout(() => {
    if (l.socks.size) return;
    l.timers.forEach(t => clearTimeout(t));
    live.delete(code); rooms.delete(code);
  }, EVICT_MS);
}
function cleanName(n) { return String(n || '').replace(/[^\x20-\x7E]|[<>&"'`]/g, '').trim().slice(0, 16) || 'Player'; }
function cleanToken(t) { t = String(t || ''); return /^[A-Za-z0-9_-]{12,64}$/.test(t) ? t : null; }

function attach(ws, room, seat) {
  // A second socket for the same seat replaces the first (old tab or half dead connection).
  const l = liveOf(room.code);
  l.socks.forEach(o => { if (o !== ws && o.seat === seat) { o.seat = null; o.close(4000, 'replaced'); } });
  ws.code = room.code; ws.seat = seat;
  l.socks.add(ws); clearTimeout(l.evict);
  clearAway(room.code, seat);
}

async function onCreate(ws, m) {
  const token = cleanToken(m.token); if (!token) return send(ws, { t: 'error', msg: 'Bad player token' });
  const n = Math.max(2, Math.min(MAX_SEATS, m.seats | 0));
  let code = null;
  for (let i = 0; i < 20 && !code; i++) { const c = newCode(); if (await store.setnx(roomKey(c), '{}', ROOM_TTL)) code = c; }
  if (!code) return send(ws, { t: 'error', msg: 'Could not make a room code, try again' });
  const room = { code, created: Date.now(), started: false, v: 0, state: null, mode: 'marathon', seats: [] };
  for (let i = 0; i < n; i++) room.seats.push({ token: null, name: '', bot: false, open: true });
  room.seats[0] = { token, name: cleanName(m.name), bot: false, open: false };
  rooms.set(code, room);
  await withRoom(code, async () => {
    await saveRoom(room);
    attach(ws, room, 0);
    send(ws, { t: 'joined', code, seat: 0 });
    broadcast(code, presence(room));
  });
}

async function onJoin(ws, m) {
  const token = cleanToken(m.token); if (!token) return send(ws, { t: 'error', msg: 'Bad player token' });
  const code = String(m.code || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
  await withRoom(code, async () => {
    const room = await loadRoom(code);
    if (!room || !room.seats) return send(ws, { t: 'gone', code, msg: 'No room with code ' + code });
    let seat = room.seats.findIndex(s => s.token === token);
    if (seat < 0) {
      if (room.started) return send(ws, { t: 'error', msg: 'That game already started and every seat is taken' });
      seat = room.seats.findIndex(s => s.open);
      if (seat < 0) return send(ws, { t: 'error', msg: 'Room ' + code + ' is full' });
      room.seats[seat] = { token, name: cleanName(m.name), bot: false, open: false };
    } else {
      const s = room.seats[seat];
      if (m.name) s.name = cleanName(m.name);
      if (s.swapped) { s.bot = false; s.swapped = false; }   // a returning player takes the seat back from the bot
    }
    await saveRoom(room);
    attach(ws, room, seat);
    send(ws, { t: 'joined', code, seat });
    if (room.state) send(ws, { t: 'state', v: room.v, state: room.state });
    broadcast(code, presence(room));
  });
}

function stateLooksOk(room, s) {
  return s && typeof s === 'object' && Array.isArray(s.players) && s.players.length === room.seats.length &&
    Number.isInteger(s.turn) && s.turn >= 0 && s.turn < room.seats.length && Array.isArray(s.b);
}

async function onStart(ws, m) {
  await withRoom(ws.code, async () => {
    const room = rooms.get(ws.code); if (!room) return;
    if (ws.seat !== 0) return send(ws, { t: 'error', msg: 'Only the room creator can start' });
    if (room.started && !m.restart) return send(ws, { t: 'error', msg: 'Game already started' });
    if (!stateLooksOk(room, m.state)) return send(ws, { t: 'error', msg: 'Start state does not match the seats' });
    room.seats.forEach((s, i) => { if (s.open) { s.open = false; s.bot = true; s.name = ''; } m.state.players[i].npc = !!s.bot; });
    room.started = true; room.v = room.v + 1; room.state = m.state;
    room.mode = typeof m.state.mode === 'string' ? m.state.mode : 'marathon';
    await saveRoom(room);
    send(ws, { t: 'ack', v: room.v });
    broadcast(room.code, { t: 'state', v: room.v, state: room.state, by: ws.seat }, ws);
    broadcast(room.code, presence(room));
  });
}

async function onState(ws, m) {
  await withRoom(ws.code, async () => {
    const room = rooms.get(ws.code); if (!room || !room.started || !room.state) return;
    const cur = room.state.turn, seatOfTurn = room.seats[cur];
    const mayMove = (ws.seat === cur && !seatOfTurn.bot) || (seatOfTurn.bot && ws.seat === botRunner(room));
    if (!mayMove || m.v !== room.v + 1 || !stateLooksOk(room, m.state)) {
      return send(ws, { t: 'state', v: room.v, state: room.state, reject: true });
    }
    room.seats.forEach((s, i) => { m.state.players[i].npc = !!s.bot; });
    room.v = m.v; room.state = m.state; room.turnAt = Date.now();
    await saveRoom(room);
    send(ws, { t: 'ack', v: room.v });
    broadcast(room.code, { t: 'state', v: room.v, state: room.state, by: ws.seat }, ws);
  });
}

function onPreview(ws, m) {
  const room = rooms.get(ws.code); if (!room || !room.state) return;
  const cur = room.state.turn;
  if (ws.seat !== cur && !(room.seats[cur].bot && ws.seat === botRunner(room))) return;
  broadcast(room.code, { t: 'preview', sel: typeof m.sel === 'string' ? m.sel.slice(0, 24) : null, rot: (m.rot | 0) & 3, v: room.v }, ws);
}

async function onLeave(ws) {
  const code = ws.code; if (!code) return;
  await withRoom(code, async () => {
    const room = rooms.get(code); if (!room) return;
    const seat = ws.seat; ws.seat = null; ws.code = null;
    liveOf(code).socks.delete(ws);
    // Leaving before the start frees the seat. Leaving after the start hands it to a bot right away.
    if (seat != null && room.seats[seat]) {
      if (!room.started && seat !== 0) room.seats[seat] = { token: null, name: '', bot: false, open: true };
      else if (room.started) { room.seats[seat].bot = true; room.seats[seat].swapped = false; room.seats[seat].token = null; }
      clearAway(code, seat);
      await saveRoom(room);
    }
    broadcast(code, presence(room));
    if (!liveOf(code).socks.size) scheduleEvict(code);
  });
}

function onClose(ws) {
  const code = ws.code; if (!code) return;
  withRoom(code, async () => {
    const l = liveOf(code); l.socks.delete(ws);
    const room = rooms.get(code);
    if (room && ws.seat != null && !connectedSeats(code).has(ws.seat)) markAway(room, ws.seat, l);
    if (room) broadcast(code, presence(room));
    if (!l.socks.size) scheduleEvict(code);
  });
}

// ---------- leaderboards ----------
function weekKey(d) {
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = t.getUTCDay() || 7; t.setUTCDate(t.getUTCDate() + 4 - day);
  const y = t.getUTCFullYear(), w = Math.ceil(((t - Date.UTC(y, 0, 1)) / 86400000 + 1) / 7);
  return y + '-W' + String(w).padStart(2, '0');
}
const lbKey = (mode, period) => 'tt:lb:' + mode + ':' + (period === 'week' ? 'wk:' + weekKey(new Date()) : 'all');
// GREEN PHONE EDIT: Daily Challenge boards are keyed by the Pacific date the bag was seeded from.
const dayKey = day => 'tt:lb:daily:day:' + day;
function pacificDay(offsetDays) {
  const d = new Date(Date.now() + (offsetDays || 0) * 86400000);
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}
const lastPost = new Map();

async function postScore(body) {
  const token = cleanToken(body.token); if (!token) return [400, { error: 'Bad player token' }];
  const mode = String(body.mode); if (!MODES.includes(mode)) return [400, { error: 'Unknown mode' }];
  const score = body.score | 0, tiles = body.tiles | 0, ms = body.ms | 0;
  if (score < 0 || score > 10000000 || tiles < 1 || tiles > 100000) return [400, { error: 'Score out of range' }];
  if (score > tiles * 80) return [400, { error: 'Score too high for that many tiles' }];
  if (mode === 'sprint' && ms > 6 * 60 * 1000) return [400, { error: 'Sprint ran too long' }];
  const now = Date.now();
  if (now - (lastPost.get(token) || 0) < 5000) return [429, { error: 'Slow down' }];
  let day = null;
  if (mode === 'daily') {
    day = String(body.seed || '');
    if (![pacificDay(-1), pacificDay(0), pacificDay(1)].includes(day)) return [400, { error: 'That Daily Challenge is closed' }];
    if (!(await store.setnx('tt:dailydone:' + day + ':' + token, '1', 3 * 86400))) return [409, { error: 'Already played the Daily Challenge for ' + day }];
  }
  lastPost.set(token, now);
  const id = now.toString(36) + crypto.randomBytes(3).toString('hex');
  const rec = body.rec && typeof body.rec === 'object' ? body.rec : {};
  const run = {
    id, name: cleanName(body.name), mode, score, tiles, ms, at: now,
    seed: String(body.seed || '').slice(0, 32), room: body.room ? String(body.room).slice(0, 8) : '',
    rec: { road: rec.road | 0, city: rec.city | 0, river: rec.river | 0, meadow: rec.meadow | 0 }
  };
  await store.set('tt:run:' + id, JSON.stringify(run));
  if (day) {
    const dk = dayKey(day);
    await store.zadd(dk, score, id); await store.expire(dk, 40 * 86400);
    const dr = await store.zrank(dk, id);
    return [200, { id, rank: dr == null ? null : dr + 1, total: await store.zcard(dk), day }];
  }
  const all = lbKey(mode, 'all'), wk = lbKey(mode, 'week');
  await store.zadd(all, score, id);
  await store.zadd(wk, score, id); await store.expire(wk, 40 * 86400);
  const rank = await store.zrank(all, id);
  return [200, { id, rank: rank == null ? null : rank + 1, total: await store.zcard(all) }];
}
async function getBoard(q) {
  const mode = q.get('mode'); if (!MODES.includes(mode)) return [400, { error: 'Unknown mode' }];
  const n = Math.max(1, Math.min(50, +q.get('n') || 10));
  let period = q.get('period') === 'week' ? 'week' : 'all', key;
  if (mode === 'daily') {
    const day = /^\d{4}-\d{2}-\d{2}$/.test(q.get('day') || '') ? q.get('day') : pacificDay(0);
    period = 'day:' + day; key = dayKey(day);
  } else key = lbKey(mode, period);
  const top = await store.ztop(key, n);
  const rows = [];
  for (const e of top) {
    const raw = await store.get('tt:run:' + e.m); if (!raw) continue;
    const r = JSON.parse(raw);
    rows.push({ id: r.id, name: r.name, score: r.score, tiles: r.tiles, at: r.at, rec: r.rec });
  }
  return [200, { mode, period, rows }];
}

// ---------- http + websocket ----------
function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}
const server = http.createServer((req, res) => {
  cors(res);
  const url = new URL(req.url, 'http://x');
  const reply = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  if (req.method === 'GET' && url.pathname === '/') return reply(200, { ok: true, store: store.kind, rooms: rooms.size });
  if (req.method === 'GET' && url.pathname === '/board') {
    return getBoard(url.searchParams).then(r => reply(r[0], r[1]), e => reply(500, { error: e.message }));
  }
  if (req.method === 'POST' && url.pathname === '/score') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 10000) req.destroy(); });
    req.on('end', () => {
      let j; try { j = JSON.parse(body); } catch (e) { return reply(400, { error: 'Bad JSON' }); }
      postScore(j).then(r => reply(r[0], r[1]), e => reply(500, { error: e.message }));
    });
    return;
  }
  reply(404, { error: 'Not found' });
});

const wss = new WebSocketServer({ server, maxPayload: MAX_STATE });
wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'create') return onCreate(ws, m);
    if (m.t === 'join') return onJoin(ws, m);
    if (m.t === 'ping') return send(ws, { t: 'pong' });
    if (!ws.code) return send(ws, { t: 'error', msg: 'Join a room first' });
    if (m.t === 'start') return onStart(ws, m);
    if (m.t === 'state') return onState(ws, m);
    if (m.t === 'preview') return onPreview(ws, m);
    if (m.t === 'leave') return onLeave(ws);
  });
  ws.on('close', () => onClose(ws));
});
// Drop sockets that stop answering pings, so away timers start promptly.
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; try { ws.ping(); } catch (e) {} });
}, 25000);

server.listen(PORT, () => console.log('tile-topia server on ' + PORT + ' using ' + store.kind + ' store'));
