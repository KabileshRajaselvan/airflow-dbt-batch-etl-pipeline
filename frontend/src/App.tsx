import { api } from "./api/client";
import { ProductEngagementTable } from "./components/ProductEngagementTable";
import { RevenueChart } from "./components/RevenueChart";
import { SegmentTable } from "./components/SegmentTable";
import { StatusBanner } from "./components/StatusBanner";
import { usePolling } from "./hooks/usePolling";

const POLL_MS = 10000;

export default function App() {
  const status = usePolling(() => api.status(), POLL_MS);
  const revenue = usePolling(() => api.dailyRevenue(30), POLL_MS);
  const engagement = usePolling(() => api.productEngagement(20), POLL_MS);
  const segments = usePolling(() => api.segmentSummary(), POLL_MS);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Airflow dbt Batch ETL Pipeline</h1>
        <p className="subtitle">mock API + source Postgres + MinIO &rarr; dbt bronze/silver/gold &rarr; marts</p>
      </header>

      <StatusBanner status={status.data} />

      <div className="grid-2">
        <RevenueChart rows={revenue.data ?? []} />
        <SegmentTable rows={segments.data ?? []} />
      </div>

      <ProductEngagementTable rows={engagement.data ?? []} />

      {(status.error || revenue.error) && (
        <p className="error-banner">API unreachable ({status.error ?? revenue.error}). Is the backend running?</p>
      )}
    </div>
  );
}
