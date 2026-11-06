const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX = 16; // максимум игроков (по числу мест на кольце)
const html = fs.readFileSync(path.join(__dirname, 'index.html'));

const server = http.createServer((req, res) => {
  if (req.url.split('?')[0] === '/health') { res.end('ok'); return; }
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
const pub = p => ({ id: p.id, slot: p.slot, name: p.name, c: p.c, d: p.d, sh: Math.max(0, (p.shieldUntil || 0) - Date.now()) });

wss.on('connection', ws => {
  const used = new Set([...players.values()].map(p => p.slot));
  const free = [...Array(MAX).keys()].filter(i => !used.has(i));
  if (!free.length) { send(ws, { t: 'full' }); ws.close(); return; }

  const id = Math.random().toString(36).slice(2, 10);
  const p = { ws, id, slot: free[0], name: 'Игрок', c: Math.random(), d: null,
              ready: false, alive: true, lastState: 0, lastStrike: 0, ch: {} };
  players.set(id, p);
  ws.on('pong', () => { p.alive = true; });
  send(ws, { t: 'init', id, slot: p.slot,
             players: [...players.values()].filter(q => q !== p && q.ready).map(pub) });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const now = Date.now();
    if (m.t === 'hello' && !p.ready) {
      p.name = String(m.name || 'Игрок').replace(/[<>&"]/g, '').slice(0, 16) || 'Игрок';
      p.c = clamp(m.c, 0, 1);
      p.ready = true;
      bc({ t: 'join', ...pub(p) }, ws);
    } else if (m.t === 'state' && p.ready) {
      if (now - p.lastState < 80) return;
      p.lastState = now;
      const d = m.d || {};
      p.d = {
        lv: objc(d.lv, 0, 30), pvo: objc(d.pvo, 0, 500),
        r: Array.isArray(d.r) ? d.r.slice(0, 4).map(x => clamp(x, 1, 500)) : [],
        br: d.br ? 1 : 0,
        x: clamp(d.x, -40, 40), z: clamp(d.z, -40, 260)
      };
      bc({ t: 'state', id, d: p.d }, ws);
    } else if (m.t === 'shield' && p.ready) {
      if (!p.d || !p.d.br) { send(ws, { t: 'deny', msg: 'Сначала построй мост' }); return; }
      p.shieldUntil = now + 300000; // щит на 5 минут
      bc({ t: 'shield', id, ms: 300000 });
    } else if (m.t === 'strike' && p.ready) {
      const target = players.get(m.to), loc = String(m.loc);
      if (!target || target === p || !target.ready || !target.d || !K.includes(loc)) return;
      if (!(target.d.lv[loc] > 0)) return;
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

  ws.on('close', () => { players.delete(id); if (p.ready) bc({ t: 'leave', id }); });
});

// отключаем «мёртвые» соединения
setInterval(() => {
  players.forEach(p => {
    if (!p.alive) { p.ws.terminate(); return; }
    p.alive = false; p.ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log('Сервер запущен на порту ' + PORT));
