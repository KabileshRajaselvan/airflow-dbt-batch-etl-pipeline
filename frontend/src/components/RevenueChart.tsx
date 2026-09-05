import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { DailyRevenue } from "../api/client";

export function RevenueChart({ rows }: { rows: DailyRevenue[] }) {
  const data = [...rows].reverse().map((r) => ({
    date: r.metric_date,
    total_revenue: r.total_revenue,
    order_count: r.order_count,
  }));

  return (
    <div className="chart-card">
      <h3>Daily Revenue</h3>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data}>
          <XAxis dataKey="date" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Bar dataKey="total_revenue" fill="#2563eb" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
