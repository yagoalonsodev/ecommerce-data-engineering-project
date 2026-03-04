import axios from "axios";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:5001";

export const getKPIs = () => axios.get(`${API_BASE}/analytics/kpis`);
export const getSalesByDay = () => axios.get(`${API_BASE}/analytics/sales-by-day`);
export const getTopProducts = (limit = 10) =>
  axios.get(`${API_BASE}/analytics/top-products?limit=${limit}`);
export const getTopCustomers = (limit = 10) =>
  axios.get(`${API_BASE}/analytics/top-customers?limit=${limit}`);
