import { useEffect, useState } from "react";
import {
  API_BASE,
  getKPIs,
  getSalesByDay,
  getTopProducts,
  getTopCustomers,
} from "../api/analytics";
import KPICards from "../components/KPICards";
import SalesChart from "../components/SalesChart";
import TopProducts from "../components/TopProducts";
import TopCustomers from "../components/TopCustomers";

export default function Dashboard() {
  const [kpis, setKpis] = useState(null);
  const [sales, setSales] = useState([]);
  const [topProducts, setTopProducts] = useState([]);
  const [topCustomers, setTopCustomers] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    Promise.all([
      getKPIs().then((res) => setKpis(res.data)),
      getSalesByDay().then((res) => setSales(res.data)),
      getTopProducts(10).then((res) => setTopProducts(res.data)),
      getTopCustomers(10).then((res) => setTopCustomers(res.data)),
    ]).catch((err) => setError(err.message || "Error loading data"));
  }, []);

  if (error) {
    return (
      <div className="min-h-screen bg-gray-100 p-10">
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
          {error}. Asegúrate de que la API esté disponible
          {API_BASE ? (
            <> (API: {API_BASE})</>
          ) : (
            <> y que <code>VITE_API_BASE</code> esté configurada en el build.</>
          )}
        </div>
      </div>
    );
  }

  if (!kpis) {
    return (
      <div className="min-h-screen bg-gray-100 p-10 flex items-center justify-center">
        <div className="text-gray-500">Loading...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100 p-10">
      <h1 className="text-3xl font-bold mb-8 text-gray-800">Ecommerce Analytics</h1>
      <KPICards data={kpis} />
      <SalesChart data={sales} />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
        <TopProducts data={topProducts} />
        <TopCustomers data={topCustomers} />
      </div>
    </div>
  );
}
