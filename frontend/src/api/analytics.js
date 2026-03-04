const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:5001";

async function get(url) {
  const res = await fetch(`${API_BASE}${url}`);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || data?.detail || `HTTP ${res.status}`);
  return { data };
}

export const getKPIs = () => get("/analytics/kpis");
export const getSalesByDay = () => get("/analytics/sales-by-day");
export const getTopProducts = (limit = 10) => get(`/analytics/top-products?limit=${limit}`);
export const getTopCustomers = (limit = 10) => get(`/analytics/top-customers?limit=${limit}`);
