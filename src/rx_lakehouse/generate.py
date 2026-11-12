"""Synthetic raw data for a pharmaceutical wholesaler.

Everything here is fake: invented product names, made-up NDCs, numbered customers. The generator
deliberately injects the problems real feeds have (duplicate lines, malformed NDCs, unknown
customers, negative quantities, missing delivery dates) so the silver layer has work to do.

Usage:  python -m rx_lakehouse.generate --out data/raw --days 90 --seed 7
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

THERAPEUTIC_CLASSES = {
    "Cardiovascular": (4.0, 40.0),
    "Diabetes": (8.0, 350.0),
    "Oncology": (400.0, 6500.0),
    "Anti-infective": (3.0, 60.0),
    "Respiratory": (15.0, 280.0),
    "Pain Management": (2.0, 45.0),
    "Immunology": (900.0, 5200.0),
    "Vaccines": (20.0, 180.0),
}
COLD_CHAIN = {"Oncology", "Immunology", "Vaccines", "Diabetes"}
FORMS = ["tablet", "capsule", "vial", "inhaler", "pen"]
MANUFACTURERS = ["Northwind Pharma", "Contoso Labs", "Fabrikam Bio", "Litware Generics", "Tailspin Rx"]
CUSTOMER_TYPES = {"Retail Pharmacy": 0.55, "Hospital": 0.2, "Clinic": 0.15, "Specialty Pharmacy": 0.1}
DCS = [("DC01", "Columbus", "OH"), ("DC02", "Dallas", "TX"), ("DC03", "Sacramento", "CA"),
       ("DC04", "Orlando", "FL"), ("DC05", "Kansas City", "MO")]
STATES_BY_DC = {"DC01": ["OH", "PA", "MI", "IN"], "DC02": ["TX", "OK", "LA"],
                "DC03": ["CA", "NV", "OR"], "DC04": ["FL", "GA", "AL"], "DC05": ["MO", "KS", "NE", "IA"]}


def _ndc(rng: np.random.Generator) -> str:
    return f"{rng.integers(10000, 99999)}-{rng.integers(1000, 9999)}-{rng.integers(10, 99)}"


def make_products(rng: np.random.Generator, n: int = 240) -> pd.DataFrame:
    rows = []
    classes = list(THERAPEUTIC_CLASSES)
    for i in range(n):
        tc = classes[i % len(classes)]
        lo, hi = THERAPEUTIC_CLASSES[tc]
        rows.append({
            "ndc": _ndc(rng),
            "product_name": f"{tc.split()[0][:4].upper()}-{i:03d} {rng.choice(FORMS)}",
            "therapeutic_class": tc,
            "manufacturer": rng.choice(MANUFACTURERS),
            "unit_cost": round(float(rng.uniform(lo, hi)), 2),
            "is_controlled": bool(tc == "Pain Management" and rng.random() < 0.6),
            "requires_cold_chain": tc in COLD_CHAIN and bool(rng.random() < 0.8),
        })
    return pd.DataFrame(rows).drop_duplicates("ndc")


def make_customers(rng: np.random.Generator, n: int = 400) -> pd.DataFrame:
    types = list(CUSTOMER_TYPES)
    probs = list(CUSTOMER_TYPES.values())
    rows = []
    for i in range(n):
        dc_id = DCS[i % len(DCS)][0]
        ctype = rng.choice(types, p=probs)
        rows.append({
            "customer_id": f"C{i:05d}",
            "customer_name": f"{ctype} #{i:04d}",
            "customer_type": ctype,
            "state": rng.choice(STATES_BY_DC[dc_id]),
            "home_dc_id": dc_id,
            "onboarded_date": (date(2022, 1, 1) + timedelta(days=int(rng.integers(0, 900)))).isoformat(),
        })
    return pd.DataFrame(rows)


def make_order_lines(rng: np.random.Generator, products: pd.DataFrame, customers: pd.DataFrame,
                     start: date, days: int) -> pd.DataFrame:
    ndcs = products["ndc"].to_numpy()
    # A few fast movers dominate volume, like real drug demand.
    popularity = rng.pareto(1.6, len(ndcs)) + 0.05
    popularity /= popularity.sum()
    unit_cost = dict(zip(products["ndc"], products["unit_cost"], strict=True))
    rows = []
    order_seq = 0
    for d in range(days):
        day = start + timedelta(days=d)
        weekday_factor = 0.45 if day.weekday() >= 5 else 1.0
        n_orders = int(rng.poisson(260 * weekday_factor))
        for _ in range(n_orders):
            order_seq += 1
            cust = customers.iloc[int(rng.integers(0, len(customers)))]
            order_ts = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(
                minutes=int(rng.integers(6 * 60, 20 * 60)))
            promised = day + timedelta(days=1 if cust["customer_type"] != "Clinic" else 2)
            for line_no in range(1, int(rng.integers(1, 6)) + 1):
                ndc = str(rng.choice(ndcs, p=popularity))
                qty = int(rng.integers(1, 24 if unit_cost[ndc] < 200 else 4))
                # Fill rate dips for one DC mid-period (a supply disruption the analysis should find).
                short_p = 0.04
                if cust["home_dc_id"] == "DC02" and 35 <= d <= 49:
                    short_p = 0.22
                shipped = qty if rng.random() > short_p else int(rng.integers(0, qty))
                late_days = int(rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 1, 2]))
                if cust["home_dc_id"] == "DC02" and 35 <= d <= 49:
                    late_days += int(rng.random() < 0.3)
                delivered = promised + timedelta(days=late_days) if shipped > 0 else None
                rows.append({
                    "order_id": f"SO{order_seq:07d}",
                    "line_no": line_no,
                    "customer_id": cust["customer_id"],
                    "dc_id": cust["home_dc_id"],
                    "ndc": ndc,
                    "order_ts": order_ts.isoformat(),
                    "qty_ordered": qty,
                    "qty_shipped": shipped,
                    "unit_price": round(unit_cost[ndc] * float(rng.uniform(1.04, 1.12)), 2),
                    "promised_date": promised.isoformat(),
                    "delivered_date": delivered.isoformat() if delivered else None,
                })
    df = pd.DataFrame(rows)
    return _inject_dirt(rng, df)


def _inject_dirt(rng: np.random.Generator, df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    dupes = df.sample(frac=0.015, random_state=int(rng.integers(0, 1_000_000)))  # resent lines
    df = pd.concat([df, dupes], ignore_index=True)
    bad = rng.choice(n, size=max(1, n // 400), replace=False)
    df.loc[bad, "ndc"] = df.loc[bad, "ndc"].str.replace("-", "", regex=False).str[:9]  # malformed NDC
    unknown = rng.choice(n, size=max(1, n // 600), replace=False)
    df.loc[unknown, "customer_id"] = "C99999"
    neg = rng.choice(n, size=max(1, n // 800), replace=False)
    df.loc[neg, "qty_ordered"] = -df.loc[neg, "qty_ordered"]
    return df.sample(frac=1.0, random_state=1).reset_index(drop=True)


def make_inventory(rng: np.random.Generator, products: pd.DataFrame, start: date, days: int) -> pd.DataFrame:
    rows = []
    for dc_id, _, _ in DCS:
        for ndc in products["ndc"]:
            level = float(rng.integers(40, 900))
            for d in range(0, days, 7):  # weekly snapshots
                level = max(0.0, level + float(rng.normal(-40, 50)))
                if level < 150 and rng.random() < 0.85:  # purchase order received
                    level += float(rng.integers(400, 900))
                if dc_id == "DC02" and 35 <= d <= 49:
                    level *= 0.35
                rows.append({"snapshot_date": (start + timedelta(days=d)).isoformat(),
                             "dc_id": dc_id, "ndc": ndc, "on_hand_units": int(level)})
    return pd.DataFrame(rows)


def generate(out: Path, days: int = 90, seed: int = 7, start: date = date(2026, 6, 1)) -> dict[str, int]:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)
    products = make_products(rng)
    customers = make_customers(rng)
    lines = make_order_lines(rng, products, customers, start, days)
    inventory = make_inventory(rng, products, start, days)

    products.to_csv(out / "products.csv", index=False)
    customers.to_csv(out / "customers.csv", index=False)
    pd.DataFrame(DCS, columns=["dc_id", "city", "state"]).to_csv(out / "distribution_centers.csv", index=False)
    inventory.to_csv(out / "inventory_snapshots.csv", index=False)
    with open(out / "order_lines.jsonl", "w") as fh:  # the "API feed" arrives as JSON lines
        for rec in lines.astype(object).where(lines.notna(), None).to_dict(orient="records"):
            fh.write(json.dumps(rec) + "\n")
    return {"products": len(products), "customers": len(customers),
            "order_lines": len(lines), "inventory_snapshots": len(inventory)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw")
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args()
    print(json.dumps(generate(Path(a.out), a.days, a.seed)))


if __name__ == "__main__":
    main()
