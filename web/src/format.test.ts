import { cell, pct, shortDate, usd } from "./format";

describe("format", () => {
  it("formats rates, money and dates", () => {
    expect(pct(0.9784)).toBe("97.8%");
    expect(pct(null)).toBe("–");
    expect(usd(31_430_000)).toBe("$31.4M");
    expect(usd(5400)).toBe("$5K");
    expect(shortDate("2026-07-06")).toBe("Jul 6");
  });

  it("formats ask-result cells by column name", () => {
    expect(cell("on_time_rate", 0.57)).toBe("57.0%");
    expect(cell("revenue", 2_500_000)).toBe("$2.5M");
    expect(cell("order_lines", 1200)).toBe("1,200");
    expect(cell("days_of_supply", 2.456)).toBe("2.46");
    expect(cell("dc_id", "DC02")).toBe("DC02");
  });
});
