export const pct = (v: number | null | undefined, digits = 1) =>
  v == null ? "–" : `${(v * 100).toFixed(digits)}%`;

export const usd = (v: number | null | undefined) =>
  v == null ? "–" : v >= 1e6 ? `$${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `$${(v / 1e3).toFixed(0)}K` : `$${v.toFixed(0)}`;

export const num = (v: number | null | undefined) => (v == null ? "–" : v.toLocaleString("en-US"));

export const shortDate = (iso: string) =>
  new Date(`${iso.slice(0, 10)}T00:00:00Z`).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });

/** Format any cell coming back from /api/ask, where the column meaning is only known by name. */
export const cell = (key: string, v: unknown): string => {
  if (typeof v === "number") {
    if (key.includes("rate")) return pct(v);
    if (key.includes("revenue") || key.includes("amount")) return usd(v);
    return Number.isInteger(v) ? num(v) : v.toFixed(2);
  }
  return v == null ? "–" : String(v);
};
