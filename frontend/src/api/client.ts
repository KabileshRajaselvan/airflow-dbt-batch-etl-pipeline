const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8093";

export interface DailyRevenue {
  metric_date: string;
  order_count: number;
  distinct_customers: number;
  total_revenue: number;
  avg_order_value: number;
  published_at: string;
}

export interface ProductEngagement {
  metric_date: string;
  product_id: string;
  view_count: number;
  add_to_cart_count: number;
  purchase_count: number;
  distinct_users: number;
  published_at: string;
}

export interface SegmentSummary {
  segment: string;
  user_count: number;
  total_revenue: number;
  avg_ltv_score: number;
  published_at: string;
}

export interface PipelineStatus {
  latest_metric_date: string | null;
  days_available: number;
  latest_published_at: string | null;
}

async function getJSON<T>(path: string): Promise<T> {
  const resp = await fetch(`${BASE_URL}${path}`);
  if (!resp.ok) {
    throw new Error(`${path} failed: ${resp.status}`);
  }
  return resp.json();
}

export const api = {
  dailyRevenue: (limit = 30) => getJSON<DailyRevenue[]>(`/api/marts/daily-revenue?limit=${limit}`),
  productEngagement: (limit = 20) => getJSON<ProductEngagement[]>(`/api/marts/product-engagement?limit=${limit}`),
  segmentSummary: () => getJSON<SegmentSummary[]>(`/api/marts/segment-summary`),
  status: () => getJSON<PipelineStatus>(`/api/marts/status`),
};
