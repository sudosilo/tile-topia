// GREEN PHONE EDIT
// Replays a finished solo run inside a copy of the game (game.html) and returns the score it really makes.
// game.html must be a copy of the index.html the phones run. RULES_V inside it says which rules it holds.
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const nodeCrypto = require('crypto');

const html = fs.readFileSync(path.join(__dirname, 'game.html'), 'utf8');
const m = /<script>([\s\S]*)<\/script>/.exec(html);
if (!m) throw new Error('game.html has no script');
const script = new vm.Script(m[1], { filename: 'game.html' });
const RULES_V = (/var RULES_V='([^']+)'/.exec(m[1]) || [])[1] || '';

function noop() {}
// A canvas context that accepts every call and draws nothing.
function ctxStub() {
  const base = {
    getImageData: (x, y, w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }),
    measureText: () => ({ width: 0 }),
    createLinearGradient: () => ({ addColorStop: noop }),
    createRadialGradient: () => ({ addColorStop: noop }),
    createPattern: () => ({}),
    isPointInPath: () => false
  };
  return new Proxy(base, { get: (t, k) => (k in t ? t[k] : noop), set: (t, k, v) => { t[k] = v; return true; } });
}
function elStub() {
  const el = {
    style: {}, dataset: {}, value: '', textContent: '', innerHTML: '', width: 32, height: 32,
    classList: { add: noop, remove: noop, toggle: noop, contains: () => false },
    getContext: () => ctxStub(), addEventListener: noop, setPointerCapture: noop, appendChild: noop
  };
  return el;
}
function sandbox() {
  const els = new Map(), store = new Map();
  const document = {
    getElementById: id => { if (!els.has(id)) els.set(id, elStub()); return els.get(id); },
    createElement: () => elStub(), querySelector: () => null, querySelectorAll: () => [],
    addEventListener: noop, body: elStub(), hidden: false
  };
  const sb = {
    document, console: { log: noop, error: noop, warn: noop },
    localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) },
    performance: { now: () => 0 }, requestAnimationFrame: noop, setTimeout: () => 0, clearTimeout: noop, setInterval: () => 0,
    addEventListener: noop, innerWidth: 400, innerHeight: 800, devicePixelRatio: 1,
    location: { search: '' }, navigator: { userAgent: 'replay' }, crypto: nodeCrypto.webcrypto,
    fetch: () => new Promise(noop), WebSocket: function () { throw new Error('no sockets in replay'); },
    Intl, Math, Date, JSON, Map, Set, Array, Object, Uint8Array, Int8Array, Int16Array, Uint8ClampedArray, Promise
  };
  sb.window = sb;
  return sb;
}
function fnv(t) { let h = 2166136261; for (let i = 0; i < t.length; i++) { h ^= t.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; }

// Returns { ok, score, tiles } or { ok:false, error }.
function replay(run) {
  const log = run.log;
  if (!Array.isArray(log) || !log.length) return { ok: false, error: 'Run has no move log' };
  if (log.length > 6000) return { ok: false, error: 'Run is too long to check' };
  const sb = sandbox();
  let api = null;
  sb.__ttHook = a => { api = a; };
  vm.createContext(sb);
  script.runInContext(sb, { timeout: 3000 });
  if (!api) return { ok: false, error: 'Game copy did not load' };
  let seed;
  if (run.mode === 'daily') seed = fnv('tiletopia-' + run.seed);
  else { seed = Number(run.seed); if (!Number.isFinite(seed)) return { ok: false, error: 'Bad seed' }; }
  api.setClock(0);
  api.newGame(1, 0, 'cyber', { mode: run.mode, seed: seed >>> 0, day: run.mode === 'daily' ? run.seed : null });
  const st = api.st;
  let lastMs = 0;
  const started = Date.now();
  for (const a of log) {
    if (!Array.isArray(a)) return { ok: false, error: 'Bad move' };
    const s = st();
    if (s.over) return { ok: false, error: 'Moves after the run ended' };
    const kind = a[0], ms = Number(a[a.length - 1]);
    if (!Number.isFinite(ms) || ms < lastMs) return { ok: false, error: 'Move times go backwards' };
    lastMs = ms; api.setClock(ms);
    if (kind === 'p') {
      const k = a[1], r = a[2];
      if (typeof k !== 'string' || !/^-?\d{1,4},-?\d{1,4}$/.test(k) || ![0, 1, 2, 3].includes(r)) return { ok: false, error: 'Bad placement' };
      const before = s.placed, list = api.fits().get(k);
      if (!list || list.indexOf(r) < 0) return { ok: false, error: 'A tile did not fit where the log put it' };
      api.pick(k, r); api.place();
      if (st().placed !== before + 1) return { ok: false, error: 'A placement did not take' };
    } else if (kind === 'h') api.hold();
    else if (kind === 'x') { if (s.players[0].rerolls <= 0) return { ok: false, error: 'Reroll with none left' }; api.reroll(); }
    else if (kind === 't') { if (run.mode !== 'heart') return { ok: false, error: 'Timeout outside Heart Zone' }; api.hzTimeout(); }
    else return { ok: false, error: 'Unknown move' };
    if (Date.now() - started > 8000) return { ok: false, error: 'Replay took too long' };
  }
  const s = st();
  if (run.mode === 'sprint') {
    if (lastMs > 300000 + 2000) return { ok: false, error: 'Sprint moves after time ran out' };
  } else if (!s.over) return { ok: false, error: 'Run did not end' };
  return { ok: true, score: s.players[0].score, tiles: s.placed };
}

module.exports = { replay, RULES_V };
