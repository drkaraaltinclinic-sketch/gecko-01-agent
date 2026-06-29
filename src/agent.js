/**
 * GECKO-01 — DataStream Pipeline Agent
 * Foundation agent for the multi-agent crypto intelligence network.
 * Streams CoinGecko data 24/7 and broadcasts events to all connected child agents.
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const fetch = require('node-fetch');
const cors = require('cors');
const http = require('http');
const path = require('path');

// ─── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const CG_API_KEY = process.env.CG_API_KEY || 'CG-sPRZEVjq2JVYm9TNZVsH3tqj';
const CG_BASE = 'https://api.coingecko.com/api/v3';
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '30000');

const ASSETS = [
  { id: 'bitcoin',       sym: 'BTC',  name: 'Bitcoin',    category: 'L1' },
  { id: 'ethereum',      sym: 'ETH',  name: 'Ethereum',   category: 'L1' },
  { id: 'solana',        sym: 'SOL',  name: 'Solana',     category: 'L1' },
  { id: 'ripple',        sym: 'XRP',  name: 'XRP',        category: 'L1' },
  { id: 'cardano',       sym: 'ADA',  name: 'Cardano',    category: 'L1' },
  { id: 'avalanche-2',   sym: 'AVAX', name: 'Avalanche',  category: 'L1' },
  { id: 'chainlink',     sym: 'LINK', name: 'Chainlink',  category: 'ORACLE' },
  { id: 'uniswap',       sym: 'UNI',  name: 'Uniswap',   category: 'DEX' },
  { id: 'aave',          sym: 'AAVE', name: 'Aave',       category: 'DEFI' },
  { id: 'polkadot',      sym: 'DOT',  name: 'Polkadot',  category: 'L0' },
];

const RWA_ASSETS = [
  { id: 'ondo-finance',      sym: 'ONDO', name: 'Ondo Finance',   type: 'Treasury' },
  { id: 'centrifuge',        sym: 'CFG',  name: 'Centrifuge',     type: 'Credit' },
  { id: 'maple',             sym: 'MPL',  name: 'Maple Finance',  type: 'Lending' },
];

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  market: {},
  lastPull: null,
  pullCount: 0,
  alertCount: 0,
  eventCount: 0,
  startTime: Date.now(),
  errors: [],
  connectedAgents: new Map(),
};

// ─── Alert Thresholds ────────────────────────────────────────────────────────
const THRESHOLDS = {
  PUMP_LOW:    3,
  PUMP_MED:    8,
  PUMP_HIGH:   15,
  DUMP_LOW:   -3,
  DUMP_MED:   -8,
  DUMP_HIGH:  -15,
  VOL_SPIKE:   0.15,
  ATH_NEAR:    0.98,
};

// ─── App Setup ───────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ─── Event Bus ───────────────────────────────────────────────────────────────
function broadcast(event) {
  const payload = JSON.stringify({ ...event, agentId: 'GECKO-01', timestamp: new Date().toISOString() });
  state.eventCount++;
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

function emit(type, topic, data, severity = 'INFO') {
  const event = { type, topic, data, severity };
  broadcast(event);
  const sym = data.symbol ? data.symbol.toUpperCase() : '';
  const logLine = `[${new Date().toISOString()}] [${type}] [${topic}]${sym ? ' ' + sym : ''} ${JSON.stringify(data).substring(0, 120)}`;
  console.log(logLine);
  return event;
}

// ─── CoinGecko Fetch ─────────────────────────────────────────────────────────
async function fetchMarketData() {
  const allIds = [...ASSETS, ...RWA_ASSETS].map(a => a.id).join(',');
  const url = `${CG_BASE}/coins/markets` +
    `?vs_currency=usd` +
    `&ids=${allIds}` +
    `&order=market_cap_desc` +
    `&per_page=50` +
    `&page=1` +
    `&sparkline=true` +
    `&price_change_percentage=1h,24h,7d` +
    `&x_cg_demo_api_key=${CG_API_KEY}`;

  emit('SYS', 'gecko.api.request', { endpoint: '/coins/markets', assets: allIds.split(',').length });

  const res = await fetch(url, {
    headers: { 'Accept': 'application/json', 'User-Agent': 'GECKO-01-Agent/1.0' },
    timeout: 15000,
  });

  if (!res.ok) throw new Error(`CoinGecko API ${res.status}: ${res.statusText}`);
  return res.json();
}

// ─── Market Tick Processing ───────────────────────────────────────────────────
function processTick(coin) {
  const prev = state.market[coin.id];
  state.market[coin.id] = coin;

  const meta = ASSETS.find(a => a.id === coin.id) || RWA_ASSETS.find(a => a.id === coin.id);
  const isRwa = !!RWA_ASSETS.find(a => a.id === coin.id);

  const tick = {
    id:          coin.id,
    symbol:      coin.symbol,
    name:        coin.name,
    price:       coin.current_price,
    change_1h:   coin.price_change_percentage_1h_in_currency || 0,
    change_24h:  coin.price_change_percentage_24h || 0,
    change_7d:   coin.price_change_percentage_7d_in_currency || 0,
    volume:      coin.total_volume,
    mcap:        coin.market_cap,
    high_24h:    coin.high_24h,
    low_24h:     coin.low_24h,
    ath:         coin.ath,
    rank:        coin.market_cap_rank,
    sparkline:   coin.sparkline_in_7d?.price || [],
    category:    meta?.category || (isRwa ? 'RWA' : 'UNKNOWN'),
    isRwa,
    prevPrice:   prev?.current_price || null,
  };

  const topic = isRwa ? 'gecko.rwa.tick' : 'gecko.market.tick';
  emit('PRICE', topic, tick);
  return tick;
}

// ─── Alert Engine ────────────────────────────────────────────────────────────
function checkAlerts(tick) {
  const alerts = [];
  const chg = tick.change_24h;
  const volRatio = tick.mcap > 0 ? tick.volume / tick.mcap : 0;

  if (chg >= THRESHOLDS.PUMP_HIGH) {
    alerts.push({ type: 'PUMP', severity: 'HIGH', value: chg, asset: tick.symbol });
  } else if (chg >= THRESHOLDS.PUMP_MED) {
    alerts.push({ type: 'PUMP', severity: 'MED', value: chg, asset: tick.symbol });
  } else if (chg >= THRESHOLDS.PUMP_LOW) {
    alerts.push({ type: 'PUMP', severity: 'LOW', value: chg, asset: tick.symbol });
  }

  if (chg <= THRESHOLDS.DUMP_HIGH) {
    alerts.push({ type: 'DUMP', severity: 'HIGH', value: chg, asset: tick.symbol });
  } else if (chg <= THRESHOLDS.DUMP_MED) {
    alerts.push({ type: 'DUMP', severity: 'MED', value: chg, asset: tick.symbol });
  } else if (chg <= THRESHOLDS.DUMP_LOW) {
    alerts.push({ type: 'DUMP', severity: 'LOW', value: chg, asset: tick.symbol });
  }

  if (volRatio >= THRESHOLDS.VOL_SPIKE) {
    alerts.push({ type: 'VOL_SPIKE', severity: 'MED', value: volRatio, asset: tick.symbol });
  }

  if (tick.ath && tick.price >= tick.ath * THRESHOLDS.ATH_NEAR) {
    alerts.push({ type: 'ATH_NEAR', severity: 'HIGH', value: tick.price, asset: tick.symbol, ath: tick.ath });
  }

  alerts.forEach(alert => {
    state.alertCount++;
    emit('ALERT', 'gecko.alert.fire', { ...alert, price: tick.price, timestamp: new Date().toISOString() }, alert.severity);
  });

  return alerts;
}

// ─── Main Pull Cycle ─────────────────────────────────────────────────────────
async function runCycle() {
  try {
    const data = await fetchMarketData();
    state.pullCount++;
    state.lastPull = new Date().toISOString();
    state.errors = state.errors.slice(-10);

    const ticks = data.map(processTick);
    ticks.forEach(checkAlerts);

    emit('SYS', 'gecko.cycle.complete', {
      pullNumber:  state.pullCount,
      assetsCount: data.length,
      alertsFired: state.alertCount,
      nextPullMs:  POLL_INTERVAL_MS,
    });

    console.log(`✓ Cycle #${state.pullCount} complete — ${data.length} assets, ${state.alertCount} total alerts`);
  } catch (err) {
    const errMsg = err.message || String(err);
    state.errors.push({ time: new Date().toISOString(), message: errMsg });
    emit('ERROR', 'gecko.error', { message: errMsg, pullCount: state.pullCount }, 'HIGH');
    console.error('✗ Cycle error:', errMsg);
  }
}

// ─── WebSocket Connection Handler ─────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const agentId = req.url?.replace('/?agent=', '') || `CLIENT-${Date.now()}`;
  state.connectedAgents.set(ws, { id: agentId, connectedAt: new Date().toISOString() });

  console.log(`[WS] Agent connected: ${agentId} (total: ${wss.clients.size})`);

  ws.send(JSON.stringify({
    type:      'SYS',
    topic:     'gecko.handshake',
    agentId:   'GECKO-01',
    timestamp: new Date().toISOString(),
    data: {
      welcome:       `Connected to GECKO-01 DataStream Pipeline`,
      version:       '1.0.0',
      pollIntervalMs: POLL_INTERVAL_MS,
      assets:        ASSETS.map(a => ({ id: a.id, sym: a.sym, category: a.category })),
      rwaAssets:     RWA_ASSETS.map(a => ({ id: a.id, sym: a.sym, type: a.type })),
      topics: [
        'gecko.market.tick',
        'gecko.rwa.tick',
        'gecko.alert.fire',
        'gecko.cycle.complete',
        'gecko.api.request',
        'gecko.error',
        'gecko.handshake',
      ],
      snapshot: state.market,
      stats: {
        uptime:     Date.now() - state.startTime,
        pullCount:  state.pullCount,
        alertCount: state.alertCount,
        lastPull:   state.lastPull,
      },
    },
  }));

  emit('SYS', 'gecko.agent.connect', { agentId, totalClients: wss.clients.size });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      console.log(`[WS] Message from ${agentId}:`, msg);
      if (msg.type === 'PING') {
        ws.send(JSON.stringify({ type: 'PONG', agentId: 'GECKO-01', timestamp: new Date().toISOString() }));
      }
      if (msg.type === 'SUBSCRIBE') {
        ws.send(JSON.stringify({ type: 'SYS', topic: 'gecko.subscribe.ack', data: { topics: msg.topics }, agentId: 'GECKO-01', timestamp: new Date().toISOString() }));
      }
    } catch (e) {
      console.warn('[WS] Bad message from', agentId, e.message);
    }
  });

  ws.on('close', () => {
    const info = state.connectedAgents.get(ws);
    state.connectedAgents.delete(ws);
    emit('SYS', 'gecko.agent.disconnect', { agentId: info?.id, totalClients: wss.clients.size });
    console.log(`[WS] Agent disconnected: ${info?.id}`);
  });

  ws.on('error', (err) => console.error(`[WS] Error from ${agentId}:`, err.message));
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/health', (_, res) => {
  res.json({
    agent:      'GECKO-01',
    status:     'LIVE',
    uptime:     Date.now() - state.startTime,
    pullCount:  state.pullCount,
    alertCount: state.alertCount,
    lastPull:   state.lastPull,
    clients:    wss.clients.size,
    errors:     state.errors.slice(-3),
  });
});

app.get('/snapshot', (_, res) => {
  res.json({
    agent:      'GECKO-01',
    timestamp:  new Date().toISOString(),
    market:     state.market,
    stats: {
      pullCount:  state.pullCount,
      alertCount: state.alertCount,
      eventCount: state.eventCount,
      lastPull:   state.lastPull,
      uptime:     Date.now() - state.startTime,
    },
  });
});

app.get('/assets', (_, res) => {
  res.json({
    crypto: ASSETS.map(a => ({ ...a, data: state.market[a.id] || null })),
    rwa:    RWA_ASSETS.map(a => ({ ...a, data: state.market[a.id] || null })),
  });
});

app.get('/alerts/history', (_, res) => {
  res.json({ count: state.alertCount, errors: state.errors });
});

// ─── Boot ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log('');
  console.log('╔════════════════════════════════════════════════╗');
  console.log('║     GECKO-01 DataStream Pipeline Agent         ║');
  console.log('║     Foundation Agent v1.0.0                    ║');
  console.log('╚════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  HTTP  →  http://localhost:${PORT}`);
  console.log(`  WS    →  ws://localhost:${PORT}`);
  console.log(`  Poll  →  every ${POLL_INTERVAL_MS / 1000}s`);
  console.log(`  Key   →  ${CG_API_KEY.substring(0, 8)}...`);
  console.log('');

  runCycle();
  setInterval(runCycle, POLL_INTERVAL_MS);
});

process.on('SIGTERM', () => { console.log('GECKO-01 shutting down...'); process.exit(0); });
process.on('SIGINT',  () => { console.log('GECKO-01 shutting down...'); process.exit(0); });
