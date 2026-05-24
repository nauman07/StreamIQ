const API_BASE = "http://localhost:8001";
const WS_URL   = "ws://localhost:8001/ws";

const api = {
  async getLatest()          { return (await fetch(`${API_BASE}/latest`)).json(); },
  async getHistory(symbol, limit=100) { return (await fetch(`${API_BASE}/history/${symbol}?limit=${limit}`)).json(); },
  async getStats()           { return (await fetch(`${API_BASE}/stats`)).json(); },
  async getSymbols()         { return (await fetch(`${API_BASE}/symbols`)).json(); },
};
