import type { ProductEngagement } from "../api/client";

export function ProductEngagementTable({ rows }: { rows: ProductEngagement[] }) {
  return (
    <div className="chart-card">
      <h3>Top Product Engagement</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Product</th>
            <th>Views</th>
            <th>Add to Cart</th>
            <th>Purchases</th>
            <th>Distinct Users</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={`${r.product_id}-${r.metric_date}-${i}`}>
              <td>{r.metric_date}</td>
              <td>{r.product_id}</td>
              <td>{r.view_count}</td>
              <td>{r.add_to_cart_count}</td>
              <td>{r.purchase_count}</td>
              <td>{r.distinct_users}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && <p className="empty-state">No data yet.</p>}
    </div>
  );
}
