import type { SegmentSummary } from "../api/client";

export function SegmentTable({ rows }: { rows: SegmentSummary[] }) {
  return (
    <div className="chart-card">
      <h3>User LTV Segment Summary</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Segment</th>
            <th>Users</th>
            <th>Total Revenue</th>
            <th>Avg LTV Score</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.segment}>
              <td>{r.segment}</td>
              <td>{r.user_count}</td>
              <td>${r.total_revenue.toFixed(2)}</td>
              <td>{r.avg_ltv_score.toFixed(1)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && <p className="empty-state">No data yet.</p>}
    </div>
  );
}
