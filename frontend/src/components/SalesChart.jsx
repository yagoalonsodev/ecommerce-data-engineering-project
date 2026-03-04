import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

export default function SalesChart({ data }) {
  if (!data?.length) return null;

  return (
    <div className="bg-white shadow rounded-2xl p-6 mt-8">
      <h2 className="text-lg font-semibold mb-4">Sales by Day</h2>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis dataKey="date" tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => `$${v / 1000}k`} />
          <Tooltip formatter={(value) => [`$${Number(value).toLocaleString()}`, "Revenue"]} />
          <Line type="monotone" dataKey="total_revenue" stroke="#2563eb" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
