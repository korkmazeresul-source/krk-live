import express from 'express';
import http from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import { TikTokLiveConnection } from 'tiktok-live-connector';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const username = (process.env.TIKTOK_USERNAME || 'krkmedya').replace(/^@/, '');
const port = Number(process.env.PORT || 3000);
const state = { username, connected: false, connecting: false, gifts: 0, points: 0, goal: 10000, events: [], lastError: null };
const broadcast = () => {
  const message = JSON.stringify({ type: 'state', data: state });
  for (const client of wss.clients) if (client.readyState === WebSocket.OPEN) client.send(message);
};
function addEvent(type, detail, points = 0) {
  state.events.unshift({ type, detail, points, at: Date.now() });
  state.events = state.events.slice(0, 30);
  broadcast();
}
app.use(express.static('public'));
app.get('/health', (_req, res) => res.json({ ok: true, connected: state.connected }));
app.get('/api/status', (_req, res) => res.json(state));
wss.on('connection', ws => ws.send(JSON.stringify({ type: 'state', data: state })));
let connection;
let retryTimer;
const seen = new Map();
function retry() { clearTimeout(retryTimer); retryTimer = setTimeout(connect, 30000); }
async function connect() {
  if (state.connecting || state.connected) return;
  state.connecting = true;
  state.lastError = null;
  broadcast();
  try {
    connection = new TikTokLiveConnection(username, { processInitialData: false });
    connection.on('connected', () => {
      state.connected = true;
      state.connecting = false;
      addEvent('system', 'TikTok LIVE bağlantısı kuruldu');
    });
    connection.on('disconnected', () => {
      state.connected = false;
      state.connecting = false;
      addEvent('system', 'TikTok LIVE bağlantısı kesildi');
      retry();
    });
    connection.on('gift', data => {
      if (data.giftType === 1 && !data.repeatEnd) return;
      const count = Math.max(1, Number(data.repeatCount || 1));
      const key = [data.userId || data.uniqueId, data.giftId, data.msgId || data.timestamp || '', count].join(':');
      if (seen.has(key)) return;
      seen.set(key, Date.now());
      const value = Math.max(0, Number(data.diamondCount ?? data.gift?.diamond_count ?? 0)) * count;
      state.gifts += count;
      state.points += value;
      addEvent('gift', `${data.nickname || data.uniqueId || 'İzleyici'} · ${data.giftName || 'Hediye'} ×${count}`, value);
    });
    connection.on('chat', data => addEvent('chat', `${data.nickname || data.uniqueId || 'İzleyici'}: ${String(data.comment || '').slice(0, 120)}`));
    await connection.connect();
  } catch (error) {
    state.connected = false;
    state.connecting = false;
    state.lastError = String(error?.message || error);
    addEvent('system', 'Bağlantı kurulamadı: ' + state.lastError);
    retry();
  }
}
setInterval(() => { for (const [key, time] of seen) if (Date.now() - time > 120000) seen.delete(key); }, 60000).unref();
server.listen(port, '0.0.0.0', () => { console.log('KRK Live running on port', port); connect(); });
