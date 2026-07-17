"""
Render the example Market x Fiscal-Week slot-utilization heatmap (highlight table)
used in the README. All data is synthetic (seeded); market names are generic.

The style mirrors the Tableau KPI heatmaps described in the case studies:
sequential single-hue ramp (magnitude), value labels in cells, muted chrome.

Run: python3 make_heatmap.py   (requires matplotlib)
"""

import random
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, Normalize

random.seed(11)

# Sequential blue ramp, steps 100 -> 700 (validated light-mode palette)
RAMP = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7",
        "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"

MARKETS = [f"Market {i:02d}" for i in range(1, 11)]
WEEKS = [f"FW{w}" for w in range(26, 34)]

# Synthetic utilization %: each market gets a base level plus weekly noise,
# a couple of markets trend upward toward capacity.
values = []
for m_idx, _ in enumerate(MARKETS):
    base = random.uniform(48, 88)
    trend = random.uniform(-0.8, 2.2)
    row = []
    for w_idx, _ in enumerate(WEEKS):
        v = base + trend * w_idx + random.gauss(0, 4)
        row.append(max(30, min(104, v)))
    values.append(row)

cmap = LinearSegmentedColormap.from_list("seq_blue", RAMP)
norm = Normalize(vmin=30, vmax=105)

fig, ax = plt.subplots(figsize=(9.2, 5.2), dpi=200)
fig.patch.set_facecolor(SURFACE)
ax.set_facecolor(SURFACE)

for i, market in enumerate(MARKETS):
    for j, week in enumerate(WEEKS):
        v = values[i][j]
        color = cmap(norm(v))
        # 2px-equivalent surface gap between cells
        ax.add_patch(plt.Rectangle((j + 0.03, i + 0.03), 0.94, 0.94,
                                   facecolor=color, edgecolor="none"))
        # ink flips to white on dark cells
        label_color = "#ffffff" if norm(v) > 0.55 else INK
        ax.text(j + 0.5, i + 0.5, f"{v:.0f}", ha="center", va="center",
                fontsize=8.5, color=label_color)

ax.set_xlim(0, len(WEEKS))
ax.set_ylim(len(MARKETS), 0)
ax.set_xticks([j + 0.5 for j in range(len(WEEKS))])
ax.set_xticklabels(WEEKS, fontsize=9, color=INK_SECONDARY)
ax.set_yticks([i + 0.5 for i in range(len(MARKETS))])
ax.set_yticklabels(MARKETS, fontsize=9, color=INK_SECONDARY)
ax.tick_params(length=0)
for spine in ax.spines.values():
    spine.set_visible(False)

ax.set_title("Slot Utilization % by Market and Fiscal Week",
             fontsize=12, color=INK, loc="left", pad=14, fontweight="bold")
ax.text(0, len(MARKETS) + 0.9,
        "Synthetic demo data — layout mirrors the production Tableau KPI heatmap",
        fontsize=8, color=INK_MUTED)

# compact colorbar as the legend for the sequential scale
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02)
cbar.outline.set_visible(False)
cbar.ax.tick_params(labelsize=8, color=INK_MUTED, labelcolor=INK_SECONDARY, length=0)
cbar.set_label("utilization %", fontsize=8, color=INK_SECONDARY)

fig.tight_layout()
out = Path(__file__).parent / "kpi_heatmap.png"
fig.savefig(out, facecolor=SURFACE, bbox_inches="tight")
print(f"wrote {out}")
