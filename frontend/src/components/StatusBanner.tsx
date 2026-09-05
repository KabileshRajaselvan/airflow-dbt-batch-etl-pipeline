import type { PipelineStatus } from "../api/client";

export function StatusBanner({ status }: { status: PipelineStatus | null }) {
  if (!status || !status.latest_metric_date) {
    return (
      <div className="status-banner status-empty">
        No marts published yet — trigger the <code>etl_pipeline</code> DAG in Airflow.
      </div>
    );
  }

  return (
    <div className="status-banner status-ok">
      Latest metrics: <strong>{status.latest_metric_date}</strong> · {status.days_available} day(s)
      available · published {status.latest_published_at ? new Date(status.latest_published_at).toLocaleString() : "-"}
    </div>
  );
}
