export type Summary = {
  orders: number; order_lines: number; unit_fill_rate: number; on_time_rate: number;
  revenue: number; start_date: string; end_date: string; high_risk_items: number;
};
export type Daily = { order_date: string; orders: number; unit_fill_rate: number; on_time_rate: number; revenue: number };
export type ByDc = { dc_id: string; city: string; state: string; unit_fill_rate: number; on_time_rate: number; revenue: number };
export type Anomaly = { order_date: string; dc_id: string; value: number; trailing_mean: number; z_score: number };
export type Risk = {
  dc_id: string; ndc: string; product_name: string; therapeutic_class: string; requires_cold_chain: boolean;
  on_hand_units: number; avg_daily_demand: number; days_of_supply: number; risk_level: string;
};
export type AskResult = { question: string; sql: string; rows: Record<string, unknown>[]; answer: string; chart: string; source: string };

async function get<T>(path: string, params: Record<string, string | undefined> = {}): Promise<T> {
  const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v) as [string, string][]);
  const res = await fetch(`/api${path}${qs.size ? `?${qs}` : ""}`);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return res.json();
}

export const api = {
  summary: (dc?: string) => get<Summary>("/kpis/summary", { dc_id: dc }),
  daily: (dc?: string) => get<Daily[]>("/kpis/daily", { dc_id: dc }),
  byDc: () => get<ByDc[]>("/kpis/by-dc"),
  anomalies: () => get<Anomaly[]>("/anomalies", { metric: "unit_fill_rate", z: "3" }),
  risk: (dc?: string) => get<Risk[]>("/stockout-risk", { level: "HIGH", dc_id: dc, limit: "20" }),
  ask: async (question: string): Promise<AskResult> => {
    const res = await fetch("/api/ask", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question }),
    });
    if (!res.ok) throw new Error(`ask: ${res.status}`);
    return res.json();
  },
};
