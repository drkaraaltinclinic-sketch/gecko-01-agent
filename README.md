# GECKO-01 — DataStream Pipeline Agent

Foundation agent for the multi-agent crypto intelligence network.
Streams CoinGecko market data 24/7 and broadcasts events via WebSocket to all connected child agents.

## Architecture

```
CoinGecko API
     │  (poll every 30s)
     ▼
 GECKO-01 Agent  ──── HTTP REST  ──► /health /snapshot /assets
 (Railway)       ──── WebSocket  ──► ALPHA-01, RISK-01, RWA-01, REPORT-01 ...
     │
     └── Event Bus broadcasts:
           gecko.market.tick
           gecko.rwa.tick
           gecko.alert.fire
           gecko.cycle.complete
           gecko.agent.connect
           gecko.error
```

## Deploy to Railway

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "GECKO-01 DataStream Agent v1.0.0"
git remote add origin https://github.com/YOUR_USERNAME/gecko-01-agent.git
git push -u origin main
```

### 2. Create Railway Project
1. Go to https://railway.app
2. New Project → Deploy from GitHub repo
3. Select your repo

### 3. Set Environment Variables
In Railway → Service → Variables, add:
```
CG_API_KEY=CG-sPRZEVjq2JVYm9TNZVsH3tqj
POLL_INTERVAL_MS=30000
```

### 4. Generate Domain
Railway → Service → Settings → Networking → Generate Domain

Your agent will be live at: `https://gecko-01-xxxx.railway.app`

## REST Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Agent health, uptime, pull count |
| `GET /snapshot` | Full market data snapshot |
| `GET /assets` | All tracked assets with live data |
| `GET /alerts/history` | Recent alerts and errors |

## WebSocket

Connect child agents at:
```
wss://your-app.railway.app/?agent=YOUR-AGENT-ID
```

On connect, GECKO-01 sends a `gecko.handshake` event with:
- Full market snapshot
- Asset registry
- Data contract (topic list)
- Current stats

## Event Topics

| Topic | Description |
|---|---|
| `gecko.market.tick` | Price update for a crypto asset |
| `gecko.rwa.tick` | Price update for a RWA token |
| `gecko.alert.fire` | Threshold breach alert |
| `gecko.cycle.complete` | Full pull cycle finished |
| `gecko.api.request` | API call initiated |
| `gecko.agent.connect` | New agent connected |
| `gecko.error` | API error occurred |

## Child Agent Template (Node.js)

```js
const WebSocket = require('ws');

const ws = new WebSocket('wss://your-gecko-agent.railway.app/?agent=ALPHA-01');

ws.on('open', () => {
  console.log('Connected to GECKO-01 pipeline');
  ws.send(JSON.stringify({ type: 'SUBSCRIBE', topics: ['gecko.market.tick', 'gecko.alert.fire'] }));
});

ws.on('message', (raw) => {
  const event = JSON.parse(raw);
  if (event.topic === 'gecko.market.tick') {
    const { symbol, price, change_24h, volume } = event.data;
    // Your agent logic here
  }
  if (event.topic === 'gecko.alert.fire') {
    const { type, severity, asset, value } = event.data;
    // Handle alert
  }
});
```

## Upgrading to CoinGecko Pro

With a Pro key, set `POLL_INTERVAL_MS=10000` for 10-second cycles (or 5000 for 5s).
Pro also unlocks WebSocket direct streaming from CoinGecko — contact for integration.
