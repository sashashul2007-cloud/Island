const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

// ---- хранилище: Upstash Redis (если заданы переменные) или файлы в папке data ----
const UP_URL = process.env.UPSTASH_REDIS_REST_URL, UP_TOK = process.env.UPSTASH_REDIS_REST_TOKEN;
const DATA_DIR = path.join(__dirname, 'data');
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
const fname = k => path.join(DATA_DIR, k.replace(/[^a-z0-9_-]/gi, '_') + '.json');
async function up(cmd) {
  const r = await fetch(UP_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOK, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  return (await r.json()).result;
}
async function kvGet(k) { try { if (UP_URL) { const x = await up(['GET', k]); return x ? JSON.parse(x) : null; } return JSON.parse(fs.readFileSync(fname(k), 'utf8')); } catch { return null; } }
async function kvSet(k, val) { try { if (UP_URL) await up(['SET', k, JSON.stringify(val)]); else fs.writeFileSync(fname(k), JSON.stringify(val)); } catch (e) { console.log('kvSet', e.message); } }
async function kvDel(k) { try { if (UP_URL) await up(['DEL', k]); else fs.unlinkSync(fname(k)); } catch {} }
const lb = new Map(); let lbDirty = false;
kvGet('lb').then(d => { if (d) Object.entries(d).forEach(([k, x]) => lb.set(k, x)); });
setInterval(() => {
  if (!lbDirty) return; lbDirty = false;
  if (lb.size > 500) [...lb.entries()].sort((a, b) => a[1].ts - b[1].ts).slice(0, lb.size - 500).forEach(([k]) => lb.delete(k));
  kvSet('lb', Object.fromEntries(lb));
}, 15000);

// ---- события архипелага и сундуки на острове сокровищ ----
const EVENTS = [['gold', 90000], ['storm', 70000], ['meteor', 60000]];
let curEv = null, nextEv = Date.now() + 180000, nextChest = Date.now() + 90000, chestSeq = 1;
const chests = new Map();
function spawnChest() {
  if (chests.size >= 6) return;
  let x, z; do { x = +((Math.random() - 0.5) * 44).toFixed(1); z = +((Math.random() - 0.5) * 44).toFixed(1); } while (Math.hypot(x, z + 22.4) < 6);
  const c = { id: chestSeq++, x, z, secs: 60 + Math.floor(Math.random() * 180) };
  chests.set(c.id, c); bc({ t: 'chest_new', id: c.id, x, z });
}
function startEvent(k) {
  const ms = EVENTS.find(e => e[0] === k)[1];
  curEv = { k, until: Date.now() + ms }; bc({ t: 'event', k, ms });
  if (k === 'meteor') setTimeout(() => { for (let i = 0; i < 3; i++) spawnChest(); }, ms * 0.5);
}
setInterval(() => {
  const now = Date.now();
  if (curEv && now > curEv.until) curEv = null;
  if (!players.size) return;
  if (now >= nextEv) { startEvent(EVENTS[Math.floor(Math.random() * EVENTS.length)][0]); nextEv = now + (6 + Math.random() * 4) * 60000; }
  if (now >= nextChest && chests.size < 3) { spawnChest(); nextChest = now + 150000; }
}, 5000);

const PORT = process.env.PORT || 3000;
const MAX = 16;
const RING = 290; // как в игре // максимум игроков (по числу мест на кольце)
const html = fs.readFileSync(path.join(__dirname, 'index.html'));

// ---------- музыка: свои файлы (папка music или рядом с index.html) ----------
const MUSIC_EXT = { '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4', '.wav': 'audio/wav' };
function listMusic() {
  const out = [], seen = new Set();
  for (const dir of [path.join(__dirname, 'music'), __dirname]) {
    let names = [];
    try { names = fs.readdirSync(dir); } catch { continue; }
    for (const f of names) {
      const ext = path.extname(f).toLowerCase();
      if (MUSIC_EXT[ext] && !seen.has(f)) { seen.add(f); out.push({ f, file: path.join(dir, f), ext }); }
    }
  }
  return out.sort((a, b) => a.f.localeCompare(b.f, 'ru'));
}
function serveFile(req, res, file, type) {
  const size = fs.statSync(file).size;
  let start = 0, end = size - 1, code = 200;
  const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (m && (m[1] !== '' || m[2] !== '')) {
    if (m[1] === '') { start = Math.max(0, size - parseInt(m[2], 10)); }
    else { start = parseInt(m[1], 10); if (m[2] !== '') end = Math.min(end, parseInt(m[2], 10)); }
    if (start > end || start >= size) { res.writeHead(416, { 'Content-Range': 'bytes */' + size }); res.end(); return; }
    code = 206;
  }
  const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Content-Length': end - start + 1, 'Cache-Control': 'public, max-age=3600' };
  if (code === 206) h['Content-Range'] = `bytes ${start}-${end}/${size}`;
  res.writeHead(code, h);
  fs.createReadStream(file, { start, end }).pipe(res);
}

// ---------- радио «Радио Дача»: адрес потока ищем в каталоге Radio Browser, играем через наш сервер ----------
let radioCache = { url: null, t: 0 };
async function resolveRadio() {
  if (process.env.RADIO_URL) return process.env.RADIO_URL; // можно задать адрес потока вручную
  if (radioCache.url && Date.now() - radioCache.t < 3600e3) return radioCache.url;
  const q = encodeURIComponent('Радио Дача');
  for (const host of ['de1', 'nl1', 'at1']) {
    try {
      const r = await fetch(`https://${host}.api.radio-browser.info/json/stations/search?name=${q}&hidebroken=true&order=clickcount&reverse=true&limit=40`,
        { headers: { 'User-Agent': 'island-game/1.0' }, signal: AbortSignal.timeout(8000) });
      if (!r.ok) continue;
      const list = (await r.json()).filter(s => /дача/i.test(s.name) && s.url_resolved && !/\.m3u8?(\?|$)/i.test(s.url_resolved));
      if (!list.length) continue;
      const exact = list.filter(s => /^радио дача$/i.test(String(s.name).trim()));
      const best = (exact.length ? exact : list)[0];
      radioCache = { url: best.url_resolved, t: Date.now() };
      return radioCache.url;
    } catch { /* пробуем следующий сервер */ }
  }
  return radioCache.url;
}
function pipeStream(url, req, res, hops) {
  const lib = url.startsWith('https') ? https : http;
  const up = lib.get(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Icy-MetaData': '0' } }, r => {
    if ([301, 302, 303, 307, 308].includes(r.statusCode) && r.headers.location && hops < 4) {
      r.resume(); pipeStream(new URL(r.headers.location, url).toString(), req, res, hops + 1); return;
    }
    if (r.statusCode !== 200) { r.resume(); res.writeHead(502); res.end('radio unavailable'); return; }
    res.writeHead(200, { 'Content-Type': r.headers['content-type'] || 'audio/mpeg', 'Cache-Control': 'no-store' });
    r.pipe(res);
    req.on('close', () => up.destroy());
  });
  up.on('error', () => { if (!res.headersSent) res.writeHead(502); res.end(); });
  up.setTimeout(10000, () => up.destroy());
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.end('ok'); return; }
  if (url === '/api/music') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(listMusic().map(t => ({ name: path.basename(t.f, t.ext), url: '/m/' + encodeURIComponent(t.f) }))));
    return;
  }
  if (url.startsWith('/m/')) {
    let name = ''; try { name = decodeURIComponent(url.slice(3)); } catch { /* пусто */ }
    const t = listMusic().find(x => x.f === name); // отдаём только файлы из списка
    if (!t) { res.writeHead(404); res.end('not found'); return; }
    serveFile(req, res, t.file, MUSIC_EXT[t.ext]);
    return;
  }
  if (url === '/radio') {
    resolveRadio().then(u => {
      if (!u) { res.writeHead(503); res.end('radio not found'); return; }
      pipeStream(u, req, res, 0);
    });
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });
const players = new Map();
const clamp = (v, a, b) => Math.min(b, Math.max(a, +v || 0));
const K = ['h', 'f', 's', 'c', 'n'];
const objc = (o, a, b) => { const r = {}; K.forEach(k => { r[k] = clamp(o && o[k], a, b) | 0; }); return r; };
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bc = (o, except) => {
  const s = JSON.stringify(o);
  players.forEach(p => { if (p.ws !== except && p.ws.readyState === 1) p.ws.send(s); });
};
const alliances = new Map(); // ключ (имя в нижнем регистре) -> { name, leader, members }
const alKey = s => String(s).toLowerCase();
const deny = (ws, msg) => send(ws, { t: 'deny', msg });
function setAlly(p, name) { p.al = name; bc({ t: 'ally', id: p.id, al: name }); }
function leaveAlly(p) {
  if (!p.al) return;
  const a = alliances.get(alKey(p.al));
  setAlly(p, null);
  if (!a) return;
  a.members.delete(p.id);
  if (a.members.size === 0) alliances.delete(alKey(a.name));
  else if (a.leader === p.id) a.leader = [...a.members][0];
}
const pub = p => ({ id: p.id, slot: p.slot, name: p.name, c: p.c, al: p.al || null, d: p.d, sh: Math.max(0, (p.shieldUntil || 0) - Date.now()) });

wss.on('connection', ws => {
  const used = new Set([...players.values()].map(p => p.slot));
  const free = [...Array(MAX).keys()].filter(i => !used.has(i));
  if (!free.length) { send(ws, { t: 'full' }); ws.close(); return; }

  const id = Math.random().toString(36).slice(2, 10);
  const p = { ws, id, slot: free[0], name: 'Игрок', c: Math.random(), d: null,
              ready: false, alive: true, lastState: 0, lastStrike: 0, ch: {}, al: null, inv: {} };
  players.set(id, p);
  ws.on('pong', () => { p.alive = true; });
  send(ws, { t: 'init', id, slot: p.slot,
             players: [...players.values()].filter(q => q !== p && q.ready).map(pub),
             chests: [...chests.values()].map(({ id, x, z }) => ({ id, x, z })),
             event: curEv ? { k: curEv.k, ms: curEv.until - Date.now() } : null });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const now = Date.now();
    if (m.t === 'hello' && !p.ready) {
      p.name = String(m.name || 'Игрок').replace(/[<>&"]/g, '').slice(0, 16) || 'Игрок';
      p.c = clamp(m.c, 0, 1);
      p.ready = true;
      const pid = String(m.pid || '');
      if (/^[0-9a-f]{16,64}$/i.test(pid)) {
        p.key = crypto.createHash('sha256').update(pid).digest('hex').slice(0, 32);
        kvGet('save:' + p.key).then(d => send(ws, { t: 'load', data: d })).catch(() => send(ws, { t: 'load', data: null }));
      } else send(ws, { t: 'load', data: null });
      bc({ t: 'join', ...pub(p) }, ws);
    } else if (m.t === 'state' && p.ready) {
      if (now - p.lastState < 80) return;
      p.lastState = now;
      const d = m.d || {};
      p.d = {
        lv: objc(d.lv, 0, 30), pvo: objc(d.pvo, 0, 500),
        r: Array.isArray(d.r) ? d.r.slice(0, 4).map(x => clamp(x, 1, 500)) : [],
        br: d.br ? 1 : 0,
        tu: d.tu ? 1 : 0, ty: clamp(d.ty, -7, 7), tp: clamp(d.tp, -1.6, 1.6), wx: clamp(d.wx, -700, 700), wz: clamp(d.wz, -700, 700), sail: d.sail ? 1 : 0,
        bo: d.bo ? 1 : 0, bx: clamp(d.bx, -700, 700), bz: clamp(d.bz, -700, 700), by: clamp(d.by, -7, 7),
        pr: clamp(d.pr, 0, 999) | 0,
        tr: Array.isArray(d.tr) ? d.tr.slice(0, 3).map(x => clamp(x, 0, 10) | 0) : [0, 0, 0]
      };
      bc({ t: 'state', id, d: p.d }, ws);
    } else if (m.t === 'shot' && p.ready) {
      if (!p.d || !p.d.tu || now - (p.lastShot || 0) < 80) return;
      const o = Array.isArray(m.o) ? m.o.map(Number) : null, vel = Array.isArray(m.v) ? m.v.map(Number) : null;
      if (!o || !vel || o.length !== 3 || vel.length !== 3 || ![...o, ...vel].every(Number.isFinite)) return;
      if (Math.hypot(...vel) > 220 || Math.hypot(...o) > 800) return;
      p.lastShot = now;
      bc({ t: 'shot', id, o, v: vel, rad: 3.6 + 0.5 * ((p.d.tr && p.d.tr[2]) || 0) });
    } else if (m.t === 'chat' && p.ready) {
      if (now - (p.lastChat || 0) < 1000) return;
      const text = String(m.text || '').replace(/[<>]/g, '').trim().slice(0, 120);
      if (!text) return;
      p.lastChat = now;
      const scope = m.scope === 'al' && p.al ? 'al' : 'all';
      const o = { t: 'chat', id, n: p.name, al: p.al || null, sc: scope, text };
      if (scope === 'al') players.forEach(q => { if (q.al === p.al && q.ws.readyState === 1) q.ws.send(JSON.stringify(o)); });
      else bc(o);
    } else if (m.t === 'chest' && p.ready) {
      const c = chests.get(m.id);
      if (!c || !p.d || p.d.sail) return;
      if (Math.hypot((p.d.wx || 0) - c.x, (p.d.wz || 0) - c.z) > 8) return;
      chests.delete(c.id);
      bc({ t: 'chest_gone', id: c.id, by: p.name, byId: id, secs: c.secs });
    } else if (m.t === 'save' && p.ready && p.key) {
      if (now - (p.lastSave || 0) < 8000 || !m.data) return;
      if (JSON.stringify(m.data).length > 20000) return;
      p.lastSave = now;
      kvSet('save:' + p.key, m.data);
      const u = m.pub || {};
      lb.set(p.key, { n: p.name, inc: clamp(u.inc, 0, 1e30), pw: clamp(u.pw, 0, 1e9), pr: clamp(u.pr, 0, 999) | 0,
                      sk: clamp(u.sk, 0, 1e9) | 0, hit: clamp(u.hit, 0, 1e9) | 0, ts: now });
      lbDirty = true;
    } else if (m.t === 'wipe' && p.ready && p.key) {
      kvDel('save:' + p.key); lb.delete(p.key); lbDirty = true;
    } else if (m.t === 'lb' && p.ready) {
      if (now - (p.lastLb || 0) < 2000) return;
      p.lastLb = now;
      const on = new Set([...players.values()].map(q => q.key).filter(Boolean));
      send(ws, { t: 'lb', list: [...lb.entries()].map(([k, x]) => ({ ...x, on: on.has(k) })).sort((a, b) => b.inc - a.inc).slice(0, 200) });
    } else if (m.t === 'sink' && p.ready) {
      const s = players.get(m.by);
      if (!s || s === p || (s.al && s.al === p.al) || !s.lastShot || now - s.lastShot > 12000 || now - (p.lastSunk || 0) < 3000) return;
      p.lastSunk = now;
      bc({ t: 'sunk', id, by: m.by });
    } else if (m.t === 'a_create' && p.ready) {
      if (p.al) { deny(ws, 'Ты уже в альянсе'); return; }
      const name = String(m.name || '').replace(/[<>&"]/g, '').trim().slice(0, 14);
      if (name.length < 2) { deny(ws, 'Название слишком короткое'); return; }
      if (alliances.has(alKey(name))) { deny(ws, 'Такое название уже занято'); return; }
      alliances.set(alKey(name), { name, leader: id, members: new Set([id]) });
      setAlly(p, name);
    } else if (m.t === 'a_invite' && p.ready) {
      const a = p.al && alliances.get(alKey(p.al)), t = players.get(m.to);
      if (!a || !t || t === p || !t.ready) return;
      if (t.al) { deny(ws, 'Этот игрок уже в альянсе'); return; }
      if (a.members.size >= 6) { deny(ws, 'В альянсе максимум 6 игроков'); return; }
      t.inv[alKey(a.name)] = now + 120000;
      send(t.ws, { t: 'a_invited', from: id, fromName: p.name, al: a.name });
      deny(ws, 'Приглашение отправлено: ' + t.name);
    } else if (m.t === 'a_accept' && p.ready) {
      const k = alKey(m.al || ''), a = alliances.get(k);
      if (p.al || !a || !(p.inv[k] > now)) { deny(ws, 'Приглашение недействительно'); return; }
      if (a.members.size >= 6) { deny(ws, 'В альянсе уже 6 игроков'); return; }
      delete p.inv[k]; a.members.add(id); setAlly(p, a.name);
    } else if (m.t === 'a_leave' && p.ready) {
      leaveAlly(p);
    } else if (m.t === 'shield' && p.ready) {
      if (!p.d || !p.d.br) { send(ws, { t: 'deny', msg: 'Сначала построй мост' }); return; }
      p.shieldUntil = now + 300000; // щит на 5 минут
      bc({ t: 'shield', id, ms: 300000 });
    } else if (m.t === 'strike' && p.ready) {
      const target = players.get(m.to), loc = String(m.loc);
      if (!target || target === p || !target.ready || !target.d || !K.includes(loc)) return;
      if (!(target.d.lv[loc] > 0)) return;
      if (p.al && p.al === target.al) { deny(ws, 'Нельзя бить союзника'); return; }
      if (now - p.lastStrike < 1000) { send(ws, { t: 'deny', msg: 'Ракеты ещё перезаряжаются' }); return; }
      p.lastStrike = now;
      if (now < (target.shieldUntil || 0)) { bc({ t: 'strike', from: id, to: m.to, loc, l: clamp(m.l, 1, 500), ic: true, sh: true }); return; }
      const maxLvl = Math.max(1, ...((p.d && p.d.r) || [1]));
      const l = Math.min(clamp(m.l, 1, 500), maxLvl);
      // ПВО цели: 4 заряда, +1 заряд за 8 секунд
      let ic = false;
      const pv = target.d.pvo[loc] | 0;
      if (pv > 0) {
        const c = target.ch[loc] || (target.ch[loc] = { n: 4, t: now });
        if (c.n < 4) {
          const add = Math.floor((now - c.t) / 8000);
          if (add > 0) { c.n = Math.min(4, c.n + add); c.t += add * 8000; }
        }
        if (c.n >= 4) c.t = now;
        if (c.n > 0) { c.n--; ic = Math.random() < Math.min(0.95, pv / (pv + l) * 1.6); }
      }
      bc({ t: 'strike', from: id, to: m.to, loc, l, ic });
    }
  });

  ws.on('close', () => { leaveAlly(p); players.delete(id); if (p.ready) bc({ t: 'leave', id }); });
});

// отключаем «мёртвые» соединения
setInterval(() => {
  players.forEach(p => {
    if (!p.alive) { p.ws.terminate(); return; }
    p.alive = false; p.ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log('Сервер запущен на порту ' + PORT));
