export default function KPICards({ data }) {
  if (!data) return null;

  const formatCurrency = (n) =>
    new Intl.NumberFormat("en-US", { style: "decimal", maximumFractionDigits: 0 }).format(n);

  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
      <div className="bg-white shadow rounded-2xl p-6">
        <h3 className="text-gray-500 text-sm">Total Revenue</h3>
        <p className="text-2xl font-bold">${formatCurrency(data.total_revenue)}</p>
      </div>
      <div className="bg-white shadow rounded-2xl p-6">
        <h3 className="text-gray-500 text-sm">Total Orders</h3>
        <p className="text-2xl font-bold">{formatCurrency(data.total_orders)}</p>
      </div>
      <div className="bg-white shadow rounded-2xl p-6">
        <h3 className="text-gray-500 text-sm">Total Sales</h3>
        <p className="text-2xl font-bold">{formatCurrency(data.total_sales)}</p>
      </div>
      <div className="bg-white shadow rounded-2xl p-6">
        <h3 className="text-gray-500 text-sm">Avg Order Value</h3>
        <p className="text-2xl font-bold">${formatCurrency(data.average_order_value)}</p>
      </div>
    </div>
  );
}
