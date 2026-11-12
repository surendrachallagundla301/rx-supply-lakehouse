import { useEffect, useMemo, useState } from "react";
import {
  Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ReferenceDot, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { api, type Anomaly, type AskResult, type ByDc, type Daily, type Risk, type Summary } from "./api";
import { cell, num, pct, shortDate, usd } from "./format";

const DCS = ["DC01", "DC02", "DC03", "DC04", "DC05"];
const EXAMPLES = ["Which DC has the lowest fill rate?", "Show items at risk of stockout", "Revenue by therapeutic class"];

export function Kpi({ label, value, note }: { label: string; value: string; note?: string }) {
  return (
    <div className="kpi">
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {note && <div className="kpi-note">{note}</div>}
    </div>
  );
}

function AskPanel() {
  const [q, setQ] = useState("");
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState<AskResult | null>(null);
  const [err, setErr] = useState("");

  const submit = async (question: string) => {
    setQ(question); setBusy(true); setErr("");
    try { setRes(await api.ask(question)); } catch (e) { setErr(String(e)); } finally { setBusy(false); }
  };
  const cols = res?.rows[0] ? Object.keys(res.rows[0]) : [];

  return (
    <section className="card span-2">
      <h2>Ask the data</h2>
      <form onSubmit={(e) => { e.preventDefault(); if (q.trim().length > 2) submit(q); }} className="ask">
        <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="e.g. Which DC has the lowest on-time rate?"
               aria-label="Question" />
        <button disabled={busy}>{busy ? "Thinking…" : "Ask"}</button>
      </form>
      <div className="chips">
        {EXAMPLES.map((x) => <button key={x} className="chip" onClick={() => submit(x)}>{x}</button>)}
      </div>
      {err && <p className="error">{err}</p>}
      {res && (
        <div className="answer">
          <p>{res.answer}</p>
          <details><summary>SQL ({res.source === "llm" ? "written by the model, checked by the guard" : "rule-based"})</summary>
            <pre>{res.sql}</pre></details>
          {cols.length > 0 && (
            <div className="table-wrap"><table>
              <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
              <tbody>{res.rows.slice(0, 12).map((r, i) =>
                <tr key={i}>{cols.map((c) => <td key={c}>{cell(c, r[c])}</td>)}</tr>)}</tbody>
            </table></div>
          )}
        </div>
      )}
    </section>
  );
}

export default function App() {
  const [dc, setDc] = useState<string>("");
  const [summary, setSummary] = useState<Summary | null>(null);
  const [daily, setDaily] = useState<Daily[]>([]);
  const [byDc, setByDc] = useState<ByDc[]>([]);
  const [anoms, setAnoms] = useState<Anomaly[]>([]);
  const [risk, setRisk] = useState<Risk[]>([]);
  const [error, setError] = useState("");

  useEffect(() => { api.byDc().then(setByDc).catch((e) => setError(String(e))); }, []);
  useEffect(() => {
    const d = dc || undefined;
    Promise.all([api.summary(d), api.daily(d), api.anomalies(), api.risk(d)])
      .then(([s, dl, a, r]) => { setSummary(s); setDaily(dl); setAnoms(a); setRisk(r); setError(""); })
      .catch((e) => setError(String(e)));
  }, [dc]);

  const shownAnoms = useMemo(() => anoms.filter((a) => !dc || a.dc_id === dc), [anoms, dc]);
  const dailyByDate = useMemo(() => new Map(daily.map((d) => [d.order_date, d])), [daily]);

  return (
    <div className="page">
      <header>
        <div>
          <h1>Rx Supply Dashboard</h1>
          <p className="sub">Synthetic pharmaceutical distribution data · gold layer of the Databricks lakehouse
            {summary && ` · ${shortDate(summary.start_date)} – ${shortDate(summary.end_date)}`}</p>
        </div>
        <label className="filter">Distribution center
          <select value={dc} onChange={(e) => setDc(e.target.value)}>
            <option value="">All DCs</option>
            {DCS.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </label>
      </header>

      {error && <p className="error">Could not load data: {error}. Is the API running on :8000?</p>}

      <section className="kpis">
        <Kpi label="Unit fill rate" value={pct(summary?.unit_fill_rate)} note="units shipped ÷ ordered" />
        <Kpi label="On-time delivery" value={pct(summary?.on_time_rate)} note="delivered by promised date" />
        <Kpi label="Revenue" value={usd(summary?.revenue)} note={`${num(summary?.orders)} orders`} />
        <Kpi label="High stockout risk" value={num(summary?.high_risk_items)} note="items under 3 days of supply" />
      </section>

      <div className="grid">
        <section className="card span-2">
          <h2>Fill rate and on-time delivery by day</h2>
          <p className="hint">Red dots: days a DC's fill rate fell more than 3σ below its trailing 28-day mean
            ({shownAnoms.length} found{shownAnoms.length ? `, ${[...new Set(shownAnoms.map((a) => a.dc_id))].join(", ")}` : ""}).</p>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={daily} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis dataKey="order_date" tickFormatter={shortDate} minTickGap={40} stroke="var(--muted)" fontSize={12} />
              <YAxis domain={[0.4, 1]} tickFormatter={(v) => pct(v, 0)} stroke="var(--muted)" fontSize={12} width={48} />
              <Tooltip formatter={(v: number) => pct(v)} labelFormatter={shortDate} />
              <Legend verticalAlign="top" height={28} iconType="plainline" />
              <Line type="monotone" dataKey="unit_fill_rate" name="Fill rate" stroke="var(--c1)" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="on_time_rate" name="On-time" stroke="var(--c2)" dot={false} strokeWidth={2} />
              {dc && shownAnoms.map((a) => dailyByDate.has(a.order_date) && (
                <ReferenceDot key={a.order_date + a.dc_id} x={a.order_date} y={dailyByDate.get(a.order_date)!.unit_fill_rate}
                              r={5} fill="var(--bad)" stroke="none" />))}
            </LineChart>
          </ResponsiveContainer>
          {!dc && shownAnoms.length > 0 && <p className="hint">Select a DC to see its anomaly days on the chart.</p>}
        </section>

        <section className="card">
          <h2>Fill rate by DC</h2>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={byDc} layout="vertical" margin={{ left: 8, right: 16 }}>
              <CartesianGrid stroke="var(--grid)" horizontal={false} />
              <XAxis type="number" domain={[0.9, 1]} tickFormatter={(v) => pct(v, 0)} stroke="var(--muted)" fontSize={12} />
              <YAxis type="category" dataKey="dc_id" stroke="var(--muted)" fontSize={12} width={44} />
              <Tooltip formatter={(v: number) => pct(v, 2)} />
              <Bar dataKey="unit_fill_rate" name="Fill rate" fill="var(--c1)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </section>

        <section className="card">
          <h2>Items at high stockout risk</h2>
          {risk.length === 0 ? <p className="hint">No high-risk items{dc && ` at ${dc}`}.</p> : (
            <div className="table-wrap"><table>
              <thead><tr><th>DC</th><th>Product</th><th>Class</th><th className="r">Days of supply</th></tr></thead>
              <tbody>{risk.map((r) => (
                <tr key={r.dc_id + r.ndc}>
                  <td>{r.dc_id}</td>
                  <td>{r.product_name}<div className="muted">{r.ndc}{r.requires_cold_chain ? " · cold chain" : ""}</div></td>
                  <td>{r.therapeutic_class}</td>
                  <td className="r bad">{r.days_of_supply.toFixed(1)}</td>
                </tr>))}</tbody>
            </table></div>
          )}
        </section>

        <AskPanel />
      </div>
    </div>
  );
}
