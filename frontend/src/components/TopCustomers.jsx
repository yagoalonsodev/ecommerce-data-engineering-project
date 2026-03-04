export default function TopCustomers({ data }) {
  if (!data?.length) return null;

  const formatCurrency = (n) =>
    new Intl.NumberFormat("en-US", { style: "decimal", maximumFractionDigits: 0 }).format(n);

  return (
    <div className="bg-white shadow rounded-2xl p-6 mt-8">
      <h2 className="text-lg font-semibold mb-4">Top Customers by Revenue</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 text-left text-gray-500">
              <th className="pb-3 font-medium">Customer</th>
              <th className="pb-3 font-medium">Location</th>
              <th className="pb-3 font-medium text-right">Revenue</th>
              <th className="pb-3 font-medium text-right">Orders</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={row.customer_id || i} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="py-3 font-medium text-gray-900">{row.customer_name || "—"}</td>
                <td className="py-3 text-gray-600">
                  {[row.city, row.country].filter(Boolean).join(", ") || "—"}
                </td>
                <td className="py-3 text-right font-medium">${formatCurrency(row.total_revenue)}</td>
                <td className="py-3 text-right">{row.order_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
