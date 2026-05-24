const SYMBOLS = ["AAPL","GOOGL","MSFT","TSLA","NVDA"];
let activeSymbol = SYMBOLS[0];
let priceChart   = null;
let rsiChart     = null;
let ws           = null;

// ── Chart setup ────────────────────────────────────────────────────────────

function initPriceChart() {
  const ctx = document.getElementById("price-chart").getContext("2d");
  priceChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        { label: "Close",  data: [], borderColor: "#2962ff", borderWidth: 2, pointRadius: 0, tension: 0.3 },
        { label: "SMA 20", data: [], borderColor: "#26a69a", borderWidth: 1.5, pointRadius: 0, borderDash: [4,2], tension: 0.3 },
        { label: "SMA 50", data: [], borderColor: "#ff9800", borderWidth: 1.5, pointRadius: 0, borderDash: [4,2], tension: 0.3 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { labels: { color: "#5d6578", boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: "#5d6578", maxTicksLimit: 8 }, grid: { color: "#1e2334" } },
        y: { ticks: { color: "#5d6578" }, grid: { color: "#1e2334" } },
      },
    },
  });
}

function initRsiChart() {
  const ctx = document.getElementById("rsi-chart").getContext("2d");
  rsiChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        { label: "RSI 14", data: [], borderColor: "#7c4dff", borderWidth: 2, pointRadius: 0, fill: false, tension: 0.3 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#5d6578", maxTicksLimit: 6 }, grid: { color: "#1e2334" } },
        y: {
          min: 0, max: 100,
          ticks: { color: "#5d6578" },
          grid: { color: "#1e2334" },
        },
      },
    },
  });
}

function updateCharts(history) {
  const labels = history.map(d => new Date(d.ts).toLocaleTimeString());
  priceChart.data.labels                  = labels;
  priceChart.data.datasets[0].data        = history.map(d => d.close);
  priceChart.data.datasets[1].data        = history.map(d => d.sma_20);
  priceChart.data.datasets[2].data        = history.map(d => d.sma_50);
  priceChart.update("none");

  rsiChart.data.labels             = labels;
  rsiChart.data.datasets[0].data   = history.map(d => d.rsi_14);
  rsiChart.update("none");
}


// ── Symbol tabs ────────────────────────────────────────────────────────────

function buildTabs() {
  const container = document.getElementById("symbol-tabs");
  container.innerHTML = SYMBOLS.map(s =>
    `<button class="sym-tab ${s === activeSymbol ? "active" : ""}" data-sym="${s}">${s}</button>`
  ).join("");
  container.querySelectorAll(".sym-tab").forEach(btn => {
    btn.addEventListener("click", () => switchSymbol(btn.dataset.sym));
  });
}

async function switchSymbol(symbol) {
  activeSymbol = symbol;
  document.querySelectorAll(".sym-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.sym === symbol);
  });
  document.getElementById("chart-title").textContent = `${symbol} — Price & Moving Averages`;
  await loadHistory(symbol);
}


// ── Data loading ───────────────────────────────────────────────────────────

async function loadHistory(symbol) {
  try {
    const history = await api.getHistory(symbol, 100);
    if (history.length === 0) return;
    updateCharts(history);

    const latest = history[history.length - 1];
    const badge  = document.getElementById("current-signal");
    badge.textContent  = latest.signal || "—";
    badge.className    = `signal-badge signal-${latest.signal || "NEUTRAL"}`;
  } catch (e) {
    console.error("History load error:", e);
  }
}

async function loadStats() {
  try {
    const stats = await api.getStats();
    document.getElementById("stat-total").textContent  = stats.total_ticks  ?? "—";
    document.getElementById("stat-buy").textContent    = stats.signals?.BUY  ?? 0;
    document.getElementById("stat-sell").textContent   = stats.signals?.SELL ?? 0;
    document.getElementById("stat-spikes").textContent = stats.volume_spikes ?? 0;
  } catch(e) { console.error("Stats error:", e); }
}

async function loadLatest() {
  try {
    const data = await api.getLatest();
    renderTickers(data);
  } catch(e) { console.error("Latest error:", e); }
}


// ── Ticker list ────────────────────────────────────────────────────────────

function rsiClass(rsi) {
  if (!rsi) return "rsi-mid";
  if (rsi > 70) return "rsi-high";
  if (rsi < 30) return "rsi-low";
  return "rsi-mid";
}

function renderTickers(data) {
  const list = document.getElementById("ticker-list");
  if (!data || data.length === 0) {
    list.innerHTML = `<div class="no-data">Waiting for data...<br>Producer is fetching market data.</div>`;
    return;
  }
  list.innerHTML = data.map(d => `
    <div class="ticker-row ${d.volume_spike ? "spike-alert" : ""}" onclick="switchSymbol('${d.symbol}')">
      <div>
        <div class="ticker-sym">${d.symbol} ${d.volume_spike ? "⚡" : ""}</div>
        <div class="ticker-meta">RSI: <span class="${rsiClass(d.rsi_14)} ticker-rsi">${d.rsi_14 ? d.rsi_14.toFixed(1) : "—"}</span></div>
      </div>
      <div style="text-align:right">
        <div class="ticker-price">$${d.close ? d.close.toFixed(2) : "—"}</div>
        <div><span class="signal-badge signal-${d.signal}">${d.signal}</span></div>
      </div>
    </div>
  `).join("");
}


// ── WebSocket ──────────────────────────────────────────────────────────────

function connectWS() {
  ws = new WebSocket(WS_URL);
  const dot   = document.getElementById("ws-status");
  const label = document.getElementById("ws-label");

  ws.onopen = () => {
    dot.className   = "status-dot connected";
    label.textContent = "Live";
  };

  ws.onmessage = async (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "tick") {
      renderTickers(msg.data);

      // If active symbol is in the update, reload its chart
      const updated = msg.data.find(d => d.symbol === activeSymbol);
      if (updated) {
        await loadHistory(activeSymbol);
        await loadStats();
      }
    }
  };

  ws.onclose = () => {
    dot.className   = "status-dot disconnected";
    label.textContent = "Reconnecting...";
    setTimeout(connectWS, 3000);
  };

  ws.onerror = () => ws.close();
}


// ── Init ───────────────────────────────────────────────────────────────────

async function init() {
  initPriceChart();
  initRsiChart();
  buildTabs();
  document.getElementById("chart-title").textContent = `${activeSymbol} — Price & Moving Averages`;

  await loadLatest();
  await loadHistory(activeSymbol);
  await loadStats();

  connectWS();

  // Fallback poll every 30s if WS dies
  setInterval(async () => {
    await loadLatest();
    await loadStats();
  }, 30000);
}

init();
