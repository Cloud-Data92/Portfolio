"""
Render a Tableau-style delivery operations dashboard from the KPI model that
case studies 02 and 08 produce.

Builds an HTML dashboard laid out the way the real Tableau workbook is specified
in TABLEAU.md — KPI tiles, market x fiscal-week heatmap, priority-score bars,
on-time trend vs. rolling baseline, and an exception table — then screenshots it.

All data is synthetic and seeded, so the output is reproducible.

Run: python3 make_dashboard.py        (requires: pip install playwright)
"""

import random
import statistics
from pathlib import Path

random.seed(11)  # same seed as make_heatmap.py so the markets agree

HERE = Path(__file__).parent
MARKETS = [f"Market {i:02d}" for i in range(1, 11)]
WEEKS = [f"FW{w}" for w in range(26, 34)]

# ── Palette (validated sequential + status ramps) ───────────────────────────
SEQ = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7",
       "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
GOOD, WARN, CRIT = "#0ca30c", "#fab219", "#d03b3b"


def lerp_hex(ramp, t):
    t = max(0.0, min(1.0, t))
    pos = t * (len(ramp) - 1)
    i = int(pos)
    if i >= len(ramp) - 1:
        return ramp[-1]
    f = pos - i
    a, b = ramp[i].lstrip("#"), ramp[i + 1].lstrip("#")
    out = [round(int(a[j:j+2], 16) + (int(b[j:j+2], 16) - int(a[j:j+2], 16)) * f)
           for j in (0, 2, 4)]
    return "#%02x%02x%02x" % tuple(out)


# ── Synthetic KPI model (mirrors case study 08's output shape) ──────────────
util, ontime = {}, {}
for m in MARKETS:
    base_u = random.uniform(48, 88)
    trend_u = random.uniform(-0.8, 2.2)
    base_o = random.uniform(0.86, 0.975)
    for i, w in enumerate(WEEKS):
        util[(m, w)] = max(30, min(104, base_u + trend_u * i + random.gauss(0, 4)))
        ontime[(m, w)] = max(0.72, min(0.995, base_o + random.gauss(0, 0.018)
                                       - (0.02 if util[(m, w)] > 95 else 0)))

last = WEEKS[-1]
priority = {}
for m in MARKETS:
    u = util[(m, last)] / 100
    o = ontime[(m, last)]
    priority[m] = round(100 * (0.45 * min(u, 1.15) + 0.55 * (1 - o) * 4), 1)

fleet_ontime = [statistics.mean(ontime[(m, w)] for m in MARKETS) for w in WEEKS]
fleet_util = statistics.mean(util[(m, last)] for m in MARKETS)
fleet_ot_now = fleet_ontime[-1]
fleet_ot_prev4 = statistics.mean(fleet_ontime[-5:-1])
failure_rate = statistics.mean(
    max(0.005, 0.055 - (ontime[(m, last)] - 0.9) * 0.25) for m in MARKETS)
routes = 1284


def tile(label, value, delta=None, status=None):
    color = {"good": GOOD, "warn": WARN, "crit": CRIT}.get(status, "#0b0b0b")
    d = ""
    if delta is not None:
        up = delta >= 0
        dc = GOOD if up else CRIT
        d = (f'<div class="delta" style="color:{dc}">'
             f'{"▲" if up else "▼"} {abs(delta):.1f} pts vs. prior 4-wk</div>')
    return (f'<div class="tile"><div class="tl">{label}</div>'
            f'<div class="tv" style="color:{color}">{value}</div>{d}</div>')


# ── Heatmap ─────────────────────────────────────────────────────────────────
heat_rows = []
for m in MARKETS:
    cells = []
    for w in WEEKS:
        v = util[(m, w)]
        bg = lerp_hex(SEQ, (v - 30) / 75)
        fg = "#ffffff" if (v - 30) / 75 > 0.55 else "#0b0b0b"
        cells.append(f'<td class="hc" style="background:{bg};color:{fg}">{v:.0f}</td>')
    heat_rows.append(f'<tr><th class="rh">{m}</th>{"".join(cells)}</tr>')
heat_head = "".join(f'<th class="ch">{w}</th>' for w in WEEKS)

# ── Priority bars ───────────────────────────────────────────────────────────
top = sorted(priority.items(), key=lambda kv: -kv[1])[:8]
pmax = max(v for _, v in top)
bars = []
for m, v in top:
    status = CRIT if v >= 64 else (WARN if v >= 52 else "#2a78d6")
    bars.append(
        f'<div class="brow"><div class="blab">{m}</div>'
        f'<div class="btrack"><div class="bfill" style="width:{100*v/pmax:.1f}%;'
        f'background:{status}"></div></div><div class="bval">{v:.0f}</div></div>')

# ── On-time trend sparkline (SVG) ───────────────────────────────────────────
W, H, PAD = 460, 130, 22
lo, hi = min(fleet_ontime) - 0.01, max(fleet_ontime) + 0.008
def px(i): return PAD + i * (W - 2 * PAD) / (len(WEEKS) - 1)
def py(v): return H - PAD - (v - lo) / (hi - lo) * (H - 2 * PAD)

pts = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(fleet_ontime))
base = [statistics.mean(fleet_ontime[max(0, i-4):i]) if i else fleet_ontime[0]
        for i in range(len(WEEKS))]
bpts = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(base))
dots = "".join(f'<circle cx="{px(i):.1f}" cy="{py(v):.1f}" r="3.5" fill="#2a78d6" '
               f'stroke="#fcfcfb" stroke-width="2"/>'
               for i, v in enumerate(fleet_ontime))
xlabs = "".join(f'<text x="{px(i):.1f}" y="{H-6}" class="ax" text-anchor="middle">'
                f'{w}</text>' for i, w in enumerate(WEEKS))

# ── Exception table (the exception taxonomy from case study 08) ─────────────
exc = []
for m in MARKETS:
    u, o = util[(m, last)], ontime[(m, last)]
    if u > 95:
        exc.append((m, "Over capacity", f"{u:.0f}% utilization", CRIT))
    elif o < 0.88:
        exc.append((m, "Service risk", f"{100*o:.1f}% on-time", CRIT))
    elif u < 55:
        exc.append((m, "Underutilized", f"{u:.0f}% utilization", WARN))
exc = exc[:4] or [("—", "No exceptions", "all markets nominal", GOOD)]
exc_rows = "".join(
    f'<tr><td class="em">{m}</td>'
    f'<td><span class="pill" style="background:{c}1a;color:{c}">{s}</span></td>'
    f'<td class="ed">{d}</td></tr>' for m, s, d, c in exc)

html = f"""<div class="db">
<div class="hdr">
  <div>
    <div class="h1">Delivery Operations — Market Performance</div>
    <div class="h2">Fiscal weeks {WEEKS[0]}–{WEEKS[-1]} · all delivery markets · synthetic demo data</div>
  </div>
  <div class="filters">
    <div class="fl"><span class="flk">Fiscal week</span><span class="flv">{last} ▾</span></div>
    <div class="fl"><span class="flk">Delivery type</span><span class="flv">All ▾</span></div>
    <div class="fl"><span class="flk">Market</span><span class="flv">All ▾</span></div>
  </div>
</div>

<div class="tiles">
  {tile("On-time rate", f"{100*fleet_ot_now:.1f}%", 100*(fleet_ot_now-fleet_ot_prev4))}
  {tile("Slot utilization", f"{fleet_util:.0f}%")}
  {tile("Failure rate", f"{100*failure_rate:.1f}%", status="warn")}
  {tile("Routes completed", f"{routes:,}")}
</div>

<div class="grid">
  <div class="card wide">
    <div class="ct">Slot utilization by market and fiscal week</div>
    <div class="cs">Sequential ramp · darker = closer to capacity</div>
    <table class="heat"><tr><th></th>{heat_head}</tr>{"".join(heat_rows)}</table>
    <div class="legend"><span class="lgl">30%</span>
      <span class="lgbar"></span><span class="lgl">105%</span></div>
  </div>

  <div class="card">
    <div class="ct">Market priority score</div>
    <div class="cs">Weighted volume · operations · capacity · quality</div>
    <div class="bars">{"".join(bars)}</div>
  </div>

  <div class="card">
    <div class="ct">Fleet on-time rate vs. rolling baseline</div>
    <div class="cs">Solid = weekly actual · dashed = prior 4-week mean</div>
    <svg viewBox="0 0 {W} {H}" class="spark">
      <polyline points="{bpts}" fill="none" stroke="#898781" stroke-width="1.5"
                stroke-dasharray="4 3"/>
      <polyline points="{pts}" fill="none" stroke="#2a78d6" stroke-width="2"/>
      {dots}{xlabs}
    </svg>
  </div>

  <div class="card wide">
    <div class="ct">Exceptions requiring action</div>
    <div class="cs">Classified by the capacity/service taxonomy</div>
    <table class="exc">{exc_rows}</table>
  </div>
</div>
<div class="foot">Built from the KPI model in case studies 02 and 08 · layout specified in TABLEAU.md</div>
</div>"""

CSS = """
*{margin:0;padding:0;box-sizing:border-box}
body{background:#f9f9f7;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",
Roboto,Helvetica,Arial,sans-serif;color:#0b0b0b;-webkit-font-smoothing:antialiased}
.db{width:1240px;padding:22px}
.hdr{display:flex;justify-content:space-between;align-items:flex-end;
margin-bottom:16px;border-bottom:2px solid #e1e0d9;padding-bottom:12px}
.h1{font-size:19px;font-weight:700;letter-spacing:-.01em}
.h2{font-size:12px;color:#52514e;margin-top:3px}
.filters{display:flex;gap:8px}
.fl{background:#fcfcfb;border:1px solid #e1e0d9;border-radius:5px;padding:5px 10px;
font-size:11px;display:flex;flex-direction:column;gap:1px;min-width:104px}
.flk{color:#898781;font-size:9.5px;text-transform:uppercase;letter-spacing:.05em}
.flv{color:#0b0b0b;font-weight:600}
.tiles{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}
.tile{background:#fcfcfb;border:1px solid #e1e0d9;border-radius:7px;padding:13px 16px}
.tl{font-size:10.5px;color:#898781;text-transform:uppercase;letter-spacing:.06em;
font-weight:600}
.tv{font-size:29px;font-weight:700;letter-spacing:-.02em;margin-top:3px;line-height:1.05}
.delta{font-size:10.5px;margin-top:3px;font-weight:600}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.card{background:#fcfcfb;border:1px solid #e1e0d9;border-radius:7px;padding:14px 16px}
.card.wide{grid-column:1/-1}
.ct{font-size:13px;font-weight:700}
.cs{font-size:10.5px;color:#898781;margin-bottom:11px}
.heat{border-collapse:separate;border-spacing:2px;width:100%}
.ch{font-size:10.5px;color:#52514e;font-weight:600;padding-bottom:3px}
.rh{font-size:10.5px;color:#52514e;font-weight:500;text-align:right;
padding-right:8px;white-space:nowrap;width:1%}
.hc{text-align:center;font-size:10.5px;padding:5px 0;border-radius:3px;
font-variant-numeric:tabular-nums}
.legend{display:flex;align-items:center;gap:7px;margin-top:9px;justify-content:flex-end}
.lgl{font-size:9.5px;color:#898781}
.lgbar{width:132px;height:7px;border-radius:4px;
background:linear-gradient(90deg,#cde2fb,#3987e5,#0d366b)}
.bars{display:flex;flex-direction:column;gap:7px}
.brow{display:flex;align-items:center;gap:9px}
.blab{font-size:10.5px;color:#52514e;width:66px;flex-shrink:0}
.btrack{flex:1;height:15px;background:#f0efec;border-radius:3px;overflow:hidden}
.bfill{height:100%;border-radius:3px}
.bval{font-size:10.5px;font-weight:700;width:24px;text-align:right;
font-variant-numeric:tabular-nums}
.spark{width:100%;height:130px}
.ax{font-size:9px;fill:#898781}
.exc{width:100%;border-collapse:collapse}
.exc tr{border-bottom:1px solid #f0efec}
.exc td{padding:7px 0;font-size:11.5px;vertical-align:middle}
.em{font-weight:600;width:88px}
.pill{padding:2.5px 9px;border-radius:11px;font-size:10px;font-weight:700}
.ed{color:#52514e;text-align:right;font-variant-numeric:tabular-nums}
.foot{margin-top:12px;font-size:10px;color:#898781;text-align:right}
"""

page = f"<!doctype html><html><head><meta charset='utf-8'><style>{CSS}</style>" \
       f"</head><body>{html}</body></html>"

tmp = HERE / "_dashboard.html"
tmp.write_text(page)

from playwright.sync_api import sync_playwright  # noqa: E402

# Use a system-installed Chromium when present (avoids re-downloading browsers).
_CANDIDATES = [
    "/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
    "/usr/bin/chromium",
    "/usr/bin/google-chrome",
]
_exe = next((c for c in _CANDIDATES if Path(c).exists()), None)

with sync_playwright() as p:
    b = p.chromium.launch(executable_path=_exe) if _exe else p.chromium.launch()
    pg = b.new_page(viewport={"width": 1240, "height": 1000}, device_scale_factor=2)
    pg.goto(tmp.resolve().as_uri())
    pg.wait_for_timeout(400)
    pg.locator(".db").screenshot(path=str(HERE / "tableau_dashboard.png"))
    b.close()

tmp.unlink()
print(f"wrote {HERE / 'tableau_dashboard.png'}")
