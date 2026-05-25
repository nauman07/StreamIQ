# StreamIQ 📈

> Real-time market data pipeline  built with Kafka, Python, FastAPI, and WebSockets.

StreamIQ is a production-pattern data engineering project that ingests live stock market data, processes it through a Kafka stream, computes technical indicators, and delivers live updates to a dashboard via WebSockets.

---

![Dashboard](https://img.shields.io/badge/Dashboard-localhost%3A3001-6366f1?style=flat-square)
![API](https://img.shields.io/badge/API-localhost%3A8001-26a69a?style=flat-square)
![Kafka](https://img.shields.io/badge/Kafka-9092-ef5350?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.11-3776ab?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ed?style=flat-square)

---

## What it does

```
yfinance (market data)
        ↓
   Kafka Producer          publishes OHLCV ticks every 30s
        ↓
  Kafka topic: market.raw
        ↓
   Kafka Consumer          calculates SMA 20/50, RSI 14, volume spikes, signals
        ↓
    PostgreSQL             persists raw + processed ticks with time-series indexes
        ↓
   FastAPI + WebSocket     serves REST API + pushes live updates every 5s
        ↓
  Chart.js Dashboard       live price chart, RSI panel, signal badges, ticker cards
```

---

## Features

| Feature | Details |
|---|---|
| **Live price charts** | Close price with SMA 20 and SMA 50 overlaid |
| **RSI panel** | 14-period Relative Strength Index with overbought/oversold zones |
| **Signal engine** | BUY / SELL / NEUTRAL based on SMA crossover + RSI |
| **Volume spike detection** | Alerts when volume exceeds 2× the 20-period average |
| **WebSocket streaming** | Dashboard updates in real-time without polling |
| **5 symbols tracked** | AAPL, GOOGL, MSFT, TSLA, NVDA |
| **Manual injection** | POST /inject endpoint to seed data when markets are closed |

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Docker Compose                  │
│                                                  │
│  ┌───────────┐    ┌──────────┐                  │
│  │ Zookeeper │◄──►│  Kafka   │                  │
│  │  :2181    │    │  :9092   │                  │
│  └───────────┘    └────┬─────┘                  │
│                        │  topic: market.raw      │
│              ┌─────────┴──────────┐             │
│              │                    │             │
│       ┌──────▼──────┐    ┌────────▼───────┐    │
│       │  Producer   │    │   Consumer     │    │
│       │  yfinance   │    │  indicators.py │    │
│       │  → Kafka    │    │  → Postgres    │    │
│       └─────────────┘    └────────────────┘    │
│                                   │             │
│                          ┌────────▼───────┐    │
│                          │  PostgreSQL    │    │
│                          │  :5433         │    │
│                          └────────┬───────┘    │
│                                   │             │
│                          ┌────────▼───────┐    │
│                          │   FastAPI      │    │
│                          │   :8001        │    │
│                          │   WebSocket    │    │
│                          └────────┬───────┘    │
│                                   │             │
│                          ┌────────▼───────┐    │
│                          │   nginx        │    │
│                          │   :3001        │    │
│                          │   Chart.js     │    │
│                          └────────────────┘    │
└─────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.5 + Zookeeper |
| Data ingestion | Python + yfinance |
| Stream processing | Python + pandas + numpy |
| Database | PostgreSQL 15 |
| API | FastAPI (async) + WebSockets |
| Frontend | Vanilla JS + Chart.js 4 |
| Web server | nginx |
| Infrastructure | Docker Compose |

---

## Quickstart

### Prerequisites
- Docker Desktop (4GB+ RAM allocated)
- Docker Compose

### 1. Clone
```bash
git clone https://github.com/nauman07/streamiq.git
cd streamiq
```

### 2. Configure
```bash
cp .env.example .env
```

The `.env` file works out of the box  no API keys needed. yfinance is free.

### 3. Run
```bash
docker compose up --build
```

First boot takes ~3 minutes  Kafka and Zookeeper are large images. You'll know it's ready when you see:

```
streamiq_producer | AAPL: close=309.19 vol=1234567
streamiq_producer | Published 5 ticks → sleeping 30s
streamiq_consumer | AAPL | close=309.19 RSI=62.3 signal=NEUTRAL
```

### 4. Open

| Service | URL | What you get |
|---|---|---|
| Dashboard | http://localhost:3001 | Live charts + ticker cards |
| API Docs | http://localhost:8001/docs | Swagger UI for all endpoints |

---

## Indicators

### SMA (Simple Moving Average)
Tracks the average closing price over the last N periods. Two lines are shown  SMA 20 (short-term trend) and SMA 50 (medium-term trend). When SMA 20 crosses above SMA 50, it's a bullish signal.

### RSI (Relative Strength Index)
Momentum oscillator from 0–100. Above 70 = overbought (potential sell). Below 30 = oversold (potential buy). Calculated using exponential weighted moving averages of gains and losses over 14 periods.

### Volume Spike
Fires when current volume exceeds 2× the 20-period average volume. Shown as a ⚡ alert on the ticker card and a pulsing border.

### Signal Logic
```python
if SMA_20 > SMA_50 and RSI < 70:  → BUY
if SMA_20 < SMA_50 and RSI > 30:  → SELL
else:                               → NEUTRAL
```

---

## API Reference

### GET /latest
Latest processed tick for all symbols.
```json
[
  {
    "symbol": "AAPL",
    "close": 309.19,
    "volume": 1234567,
    "sma_20": 305.40,
    "sma_50": 298.10,
    "rsi_14": 62.3,
    "volume_spike": false,
    "signal": "BUY"
  }
]
```

### GET /history/{symbol}?limit=100
Historical processed ticks for chart rendering.

### GET /stats
Dashboard aggregates  total ticks, signal breakdown, volume spike count.

### WebSocket /ws
Connect to receive live tick broadcasts every 5 seconds.
```javascript
const ws = new WebSocket("ws://localhost:8001/ws");
ws.onmessage = (e) => {
  const { type, data } = JSON.parse(e.data);
  // type: "tick", data: array of latest processed ticks
};
```

### POST /inject
Manually push a tick  useful when markets are closed or during development.
```json
{"symbol": "AAPL", "close": 309.19, "volume": 1234567}
```

---

## Project Structure

```
streamiq/
│
├── docker-compose.yml          ← 7 services, one command
├── .env                        ← config (no secrets needed)
├── .env.example
│
├── producer/                   ← DATA INGESTION
│   ├── producer.py             ← yfinance → Kafka topic: market.raw
│   ├── requirements.txt        ← yfinance, kafka-python, pandas
│   └── Dockerfile
│
├── consumer/                   ← STREAM PROCESSING
│   ├── consumer.py             ← Kafka → indicators → PostgreSQL
│   ├── indicators.py           ← SMA, RSI, volume spike, signal
│   ├── requirements.txt        ← kafka-python, pandas, psycopg2, numpy
│   └── Dockerfile
│
├── fastapi/                    ← API + WEBSOCKET
│   ├── main.py                 ← REST endpoints + WebSocket broadcaster
│   ├── database.py             ← SQLAlchemy async engine
│   ├── requirements.txt        ← fastapi, asyncpg, sqlalchemy, websockets
│   └── Dockerfile
│
├── frontend/                   ← DASHBOARD
│   ├── index.html
│   ├── nginx.conf
│   ├── css/style.css           ← dark trading terminal theme
│   └── js/
│       ├── api.js              ← fetch calls + WebSocket client
│       └── app.js              ← Chart.js charts + live update logic
│
└── postgres/
    └── init.sql                ← market_raw + market_processed + indexes
```

---

## Data Flow in Detail

```
1. Producer (every 30s)
   └── yfinance.Ticker(symbol).history(period="5d", interval="1d")
   └── publishes JSON tick to Kafka topic "market.raw"

2. Consumer (continuous)
   └── reads from "market.raw"
   └── maintains rolling 100-tick window per symbol in memory
   └── computes SMA 20, SMA 50, RSI 14, volume spike, signal
   └── writes to market_raw and market_processed in PostgreSQL

3. FastAPI broadcaster (every 5s)
   └── queries market_processed for latest tick per symbol
   └── pushes JSON payload to all connected WebSocket clients

4. Frontend
   └── WebSocket receives tick → updates Chart.js datasets
   └── Falls back to REST polling every 30s if WS disconnects
```

---

## Market Hours Note

yfinance returns intraday data only during US market hours (Mon–Fri 9:30am–4:00pm EST). Outside those hours the producer uses daily OHLCV data, so prices won't change tick-by-tick  but the full pipeline still runs and all indicators are computed. Use `POST /inject` to simulate live ticks during development.

---

## Services

| Container | Image | Port | Role |
|---|---|---|---|
| streamiq_zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| streamiq_kafka | confluentinc/cp-kafka:7.5.0 | 29092 | Message broker |
| streamiq_postgres | postgres:15-alpine | 5433 | Time-series storage |
| streamiq_producer | market-producer (custom) |  | Data ingestion |
| streamiq_consumer | market-consumer (custom) |  | Stream processing |
| streamiq_fastapi | market-fastapi (custom) | 8001 | API + WebSocket |
| streamiq_frontend | nginx:alpine | 3001 | Static dashboard |

---

## Roadmap

- [ ] GitHub Actions CI/CD pipeline
- [ ] pytest test suite with mock Kafka producer
- [ ] Additional symbols + crypto (BTC, ETH via yfinance)
- [ ] MACD indicator
- [ ] Alert system  email/Slack on CRITICAL signals
- [ ] Historical backtesting endpoint

---

## Built with

- [Apache Kafka](https://kafka.apache.org/)  distributed message streaming
- [yfinance](https://github.com/ranaroussi/yfinance)  Yahoo Finance market data
- [FastAPI](https://fastapi.tiangolo.com/)  async Python API framework
- [SQLAlchemy](https://www.sqlalchemy.org/)  async ORM
- [Chart.js](https://www.chartjs.org/)  canvas-based charting
- [PostgreSQL](https://www.postgresql.org/)  primary data store
- [Docker](https://www.docker.com/)  containerisation

---

## Disclaimer

StreamIQ is a portfolio and learning project. It is not financial advice. Do not use it to make trading decisions.

---

## License

MIT
