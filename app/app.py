"""Retail Demand Intelligence — Streamlit dashboard over the Gold layer.

Data flow:  Gold Delta tables ──(notebooks/04_analysis_queries.py)──► app/data/*.csv ──► this app
The app never talks to Databricks. Each CSV is pre-aggregated slightly finer than the charts
(keeping invoice_year / Channel / Category as columns), so sidebar filters are applied in pandas
*before* aggregation — one filter selection re-aggregates every chart that depends on it.

Run locally:  streamlit run app/app.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA = Path(__file__).parent / "data"  # works locally and on Streamlit Community Cloud

st.set_page_config(page_title="Retail Demand Intelligence", page_icon="🛒", layout="wide")

# ── Visual system ────────────────────────────────────────────────────────────────────────────
# Channels are nominal → first three categorical slots (validated all-pairs, colour-blind safe).
# Years are ordered → two steps of one blue ramp. Magnitudes (heatmaps) → single-hue sequential.
# Colour is bound to the entity, so filtering a channel out never repaints the survivors.
CHANNEL_COLORS = {"In-Store": "#2a78d6", "Online": "#eb6834", "Mobile App": "#1baf7a"}
CHANNEL_ORDER = list(CHANNEL_COLORS)
YEAR_COLORS = {"2024": "#86b6ef", "2025": "#1c5cab"}
SERIES_1 = "#2a78d6"
MUTED = "#898781"
INK, INK_2 = "#0b0b0b", "#52514e"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
SEQ_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
DIVERGING = {"neg": "#e34948", "pos": "#2a78d6"}
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def style(fig: go.Figure, height: int = 380, legend: bool = True) -> go.Figure:
    """Shared chart chrome: recessive hairline grid, system sans, thin marks, unified hover."""
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=56, b=8),
        font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', size=13, color=INK_2),
        title_font=dict(size=16, color=INK),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1, title_text=""),
        hoverlabel=dict(bgcolor="white", font_size=13),
    )
    fig.update_xaxes(showgrid=False, linecolor=AXIS, ticks="", title_font_color=INK_2)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor=AXIS, title_font_color=INK_2)
    return fig


def chart_footer(source: str, interpretation: str, table: pd.DataFrame | None = None):
    st.caption(f"💡 **Interpretation:** {interpretation}")
    st.caption(f"Source: {source}")
    if table is not None:
        with st.expander("View data"):
            st.dataframe(table, hide_index=True, width="stretch")


def money(x: float) -> str:
    return f"${x / 1e6:,.2f}M" if abs(x) >= 1e6 else f"${x / 1e3:,.1f}K"


# ── Data ─────────────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load(name: str, **kw) -> pd.DataFrame:
    return pd.read_csv(DATA / f"{name}.csv", **kw)


channel_month = load("channel_month")
hourly = load("hourly_channel")
cat_channel = load("category_channel")
store_month = load("store_month")
cat_weekly = load("category_weekly", parse_dates=["week_start"])
reorder = load("reorder_alerts", dtype={"StockCode": str})

# ── Sidebar filters ──────────────────────────────────────────────────────────────────────────
st.sidebar.header("Filters")
all_years = sorted(channel_month["invoice_year"].unique())
years = st.sidebar.multiselect("Year", all_years, default=all_years)
channels = st.sidebar.multiselect("Channel", CHANNEL_ORDER, default=CHANNEL_ORDER)
all_cats = sorted(cat_channel["Category"].unique())
categories = st.sidebar.multiselect("Category (Charts 4, 6, reorder table)", all_cats, default=all_cats)
all_regions = sorted(store_month["Region"].unique())
regions = st.sidebar.multiselect("Region (Chart 5)", all_regions, default=all_regions)
st.sidebar.divider()
anomaly_cat = st.sidebar.selectbox("Category for anomaly scan (Chart 7)", all_cats,
                                   index=all_cats.index("Gifts & Seasonal"))
st.sidebar.caption("Year + Channel drive Charts 1, 2, 3, 4, 6 and the KPI row.")

if not years or not channels:
    st.warning("Select at least one year and one channel.")
    st.stop()

# Apply the shared filters once; every chart below aggregates from these frames.
cm = channel_month[channel_month["invoice_year"].isin(years) & channel_month["Channel"].isin(channels)]
hc = hourly[hourly["invoice_year"].isin(years) & hourly["Channel"].isin(channels)]
cc = cat_channel[cat_channel["invoice_year"].isin(years) & cat_channel["Channel"].isin(channels)
                 & cat_channel["Category"].isin(categories)]
sm = store_month[store_month["invoice_year"].isin(years) & store_month["Region"].isin(regions)]

# ── Header + KPI row ─────────────────────────────────────────────────────────────────────────
st.title("Retail Demand Intelligence")
st.markdown(
    "Category performance, demand patterns, store outliers and inventory risk across "
    "60 stores, 3 channels and 12 categories (Jan 2024 – Dec 2025)."
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Net revenue", money(cm["revenue"].sum()))
k2.metric("Units kept (net)", f"{cm['units'].sum():,.0f}")
k3.metric("Sales lines", f"{cm['lines'].sum():,.0f}")
k4.metric("Line return rate", f"{cm['return_lines'].sum() / cm['lines'].sum():.1%}")
st.caption("KPIs follow the Year and Channel filters · Source: retail_gold.retail_analytics")
st.divider()

# ── Chart 1 · Revenue by Channel ─────────────────────────────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    d = cm.groupby(["Channel", "invoice_year"], as_index=False)["revenue"].sum()
    d["Year"] = d["invoice_year"].astype(str)
    fig = px.bar(d, x="Channel", y="revenue", color="Year", barmode="group",
                 category_orders={"Channel": [c for c in CHANNEL_ORDER if c in channels], "Year": sorted(d["Year"].unique())},
                 color_discrete_map=YEAR_COLORS,
                 labels={"revenue": "Net revenue ($)", "Channel": "Sales channel"},
                 title="Chart 1 · Revenue by Channel")
    fig.update_traces(marker_cornerradius=4, hovertemplate="%{x} · %{fullData.name}<br>$%{y:,.0f}<extra></extra>")
    fig.update_layout(bargap=0.3, bargroupgap=0.08)
    fig.update_yaxes(tickprefix="$", tickformat="~s")
    st.plotly_chart(style(fig), width="stretch")
    tot = d.groupby("Channel")["revenue"].sum().sort_values(ascending=False)
    chart_footer(
        "retail_gold.retail_analytics (Q1)",
        f"{tot.index[0]} is the largest channel at {money(tot.iloc[0])} "
        f"({tot.iloc[0] / tot.sum():.1%} of revenue in the current selection).",
        d.pivot(index="Channel", columns="Year", values="revenue").round(2).reset_index(),
    )

# ── Chart 2 · Monthly Revenue Trend ──────────────────────────────────────────────────────────
with c2:
    d = cm.groupby(["invoice_year", "invoice_month"], as_index=False)["revenue"].sum()
    d["Year"] = d["invoice_year"].astype(str)
    fig = px.line(d, x="invoice_month", y="revenue", color="Year", markers=True,
                  color_discrete_map=YEAR_COLORS,
                  labels={"invoice_month": "Month", "revenue": "Net revenue ($)"},
                  title="Chart 2 · Monthly Revenue Trend")
    fig.update_traces(line_width=2, marker_size=8, hovertemplate="$%{y:,.0f}")
    fig.update_xaxes(tickmode="array", tickvals=list(range(1, 13)), ticktext=MONTH_ABBR)
    fig.update_yaxes(tickprefix="$", tickformat="~s")
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(style(fig), width="stretch")
    peak = d.loc[d["revenue"].idxmax()]
    trough = d.loc[d["revenue"].idxmin()]
    chart_footer(
        "retail_gold.retail_analytics (Q2)",
        f"Peak month is {MONTH_ABBR[int(peak.invoice_month) - 1]} {int(peak.invoice_year)} "
        f"({money(peak.revenue)}), {peak.revenue / trough.revenue:.1f}× the weakest month "
        f"({MONTH_ABBR[int(trough.invoice_month) - 1]} {int(trough.invoice_year)}) — demand is strongly Q4-weighted.",
        d.pivot(index="invoice_month", columns="Year", values="revenue").round(2).reset_index(),
    )

# ── Chart 3 · Hour-of-Day Demand Curve ───────────────────────────────────────────────────────
c3, c4 = st.columns(2)
with c3:
    d = (hc.groupby(["invoice_hour", "Channel"], as_index=False)["revenue"].sum()
           .set_index(["invoice_hour", "Channel"])
           .reindex(pd.MultiIndex.from_product([range(24), [c for c in CHANNEL_ORDER if c in channels]],
                                               names=["invoice_hour", "Channel"]), fill_value=0)
           .reset_index())
    fig = px.line(d, x="invoice_hour", y="revenue", color="Channel",
                  color_discrete_map=CHANNEL_COLORS,
                  category_orders={"Channel": CHANNEL_ORDER},
                  labels={"invoice_hour": "Hour of day (local)", "revenue": "Net revenue ($)"},
                  title="Chart 3 · Hour-of-Day Demand Curve")
    fig.update_traces(line_width=2, hovertemplate="$%{y:,.0f}")
    fig.update_xaxes(dtick=2, range=[-0.3, 23.3])
    fig.update_yaxes(tickprefix="$", tickformat="~s")
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(style(fig), width="stretch")
    peaks = d.loc[d.groupby("Channel")["revenue"].idxmax()]
    peak_txt = "; ".join(f"{r.Channel} peaks at {int(r.invoice_hour):02d}:00" for r in peaks.itertuples())
    instore = d[(d["Channel"] == "In-Store") & (d["revenue"] > 0)]["invoice_hour"]
    hours_txt = f" In-Store only trades {instore.min():02d}:00–{instore.max():02d}:59, so night demand is digital-only." if len(instore) else ""
    chart_footer("retail_gold.retail_analytics (Q3)", peak_txt + "." + hours_txt,
                 d.pivot(index="invoice_hour", columns="Channel", values="revenue").round(2).reset_index())

# ── Chart 4 · Top 10 Revenue Categories ──────────────────────────────────────────────────────
with c4:
    d = (cc.groupby("Category", as_index=False)[["revenue", "units"]].sum()
           .sort_values("revenue", ascending=False).head(10))
    fig = px.bar(d.iloc[::-1], x="revenue", y="Category", orientation="h",
                 labels={"revenue": "Net revenue ($)", "Category": ""},
                 title="Chart 4 · Top 10 Revenue Categories")
    fig.update_traces(marker_color=SERIES_1, marker_cornerradius=4,
                      hovertemplate="%{y}<br>$%{x:,.0f}<extra></extra>")
    fig.update_xaxes(tickprefix="$", tickformat="~s", showgrid=True, gridcolor=GRID)
    fig.update_yaxes(showgrid=False)
    fig.update_layout(bargap=0.35)
    st.plotly_chart(style(fig, legend=False), width="stretch")
    if len(d):
        top = d.iloc[0]
        rev_per_unit = (d["revenue"] / d["units"])
        chart_footer(
            "retail_gold.retail_analytics (Q4)",
            f"{top.Category} leads with {money(top.revenue)} on only {top.units:,.0f} units "
            f"(${top.revenue / top.units:,.0f}/unit vs a top-10 median of ${rev_per_unit.median():,.0f}) — "
            "a high-ticket category, not a high-volume one.",
            d.round(2),
        )

# ── Chart 5 · Store Performance Heatmap ──────────────────────────────────────────────────────
c5, c6 = st.columns(2)
with c5:
    if sm.empty:
        st.info("No stores match the Region filter.")
    else:
        p = sm.pivot_table(index="Region", columns="StoreType", values="monthly_revenue", aggfunc="sum")
        fig = px.imshow(p, color_continuous_scale=SEQ_BLUE, aspect="auto", text_auto="$.3s",
                        labels={"x": "Store type", "y": "Region", "color": "Revenue ($)"},
                        title="Chart 5 · Store Performance Heatmap")
        fig.update_traces(xgap=2, ygap=2, hovertemplate="%{y} · %{x}<br>$%{z:,.0f}<extra></extra>")
        fig.update_coloraxes(colorbar_tickprefix="$", colorbar_tickformat="~s")
        st.plotly_chart(style(fig, legend=False), width="stretch")
        best = p.stack().idxmax()
        outliers = (sm.groupby(["StoreName", "Region"])
                      .apply(lambda g: g["monthly_revenue"].sum() / g["regional_median_revenue"].sum() - 1, include_groups=False)
                      .sort_values())
        chart_footer(
            "retail_gold.store_performance (Q5)",
            f"{best[0]} {best[1]} stores generate the most revenue ({money(p.loc[best])}). "
            f"Biggest under-performer vs its regional median: {outliers.index[0][0]} ({outliers.iloc[0]:+.0%}); "
            f"biggest out-performer: {outliers.index[-1][0]} ({outliers.iloc[-1]:+.0%}).",
            p.round(2).reset_index(),
        )

# ── Chart 6 · Return Rate Heatmap ────────────────────────────────────────────────────────────
with c6:
    if cc.empty:
        st.info("No categories match the filters.")
    else:
        # Rates are re-aggregated from counts — never averaged — so any filter combination stays exact.
        r = cc.groupby(["Category", "Channel"], as_index=False)[["return_lines", "lines"]].sum()
        r["return_rate"] = r["return_lines"] / r["lines"]
        p = (r.pivot(index="Category", columns="Channel", values="return_rate")
               .reindex(columns=[c for c in CHANNEL_ORDER if c in channels]))
        p = p.loc[p.mean(axis=1).sort_values(ascending=False).index]
        fig = px.imshow(p, color_continuous_scale=SEQ_BLUE, zmin=0, zmax=0.25, aspect="auto", text_auto=".1%",
                        labels={"x": "Channel", "y": "", "color": "Return rate"},
                        title="Chart 6 · Return Rate Heatmap")
        fig.update_traces(xgap=2, ygap=2, hovertemplate="%{y} · %{x}<br>%{z:.1%} of lines returned<extra></extra>")
        fig.update_coloraxes(colorbar_tickformat=".0%")
        st.plotly_chart(style(fig, height=460, legend=False), width="stretch")
        worst = r.loc[r["return_rate"].idxmax()]
        by_ch = r.groupby("Channel")[["return_lines", "lines"]].sum()
        by_ch = (by_ch["return_lines"] / by_ch["lines"]).sort_values()
        chart_footer(
            "retail_gold.retail_analytics (Q6)",
            f"{worst.Category} via {worst.Channel} has the highest return rate ({worst.return_rate:.1%}). "
            f"Across categories, {by_ch.index[-1]} returns {by_ch.iloc[-1] / by_ch.iloc[0]:.1f}× as often as {by_ch.index[0]}.",
            r.assign(return_rate=r["return_rate"].round(4)),
        )

# ── Chart 7 · Anomaly Highlight ──────────────────────────────────────────────────────────────
st.divider()
w = cat_weekly[cat_weekly["Category"] == anomaly_cat].sort_values("week_start").copy()
# Baseline = mean of the 4 *preceding* weeks (excluding the current one, so a spike cannot inflate its own baseline).
w["baseline"] = w["weekly_revenue"].shift(1).rolling(4).mean()
w["ratio"] = w["weekly_revenue"] / w["baseline"]
# The final week (starting 2025-12-29) only contains Mon–Wed; exclude it from detection.
last_full = pd.Timestamp("2025-12-31") - pd.Timedelta(days=6)
w["partial_week"] = w["week_start"] > last_full
wv = w[w["week_start"].dt.year.isin(years)]

if wv.empty:
    st.info("No weeks in the selected years.")
else:
    cand = wv[~wv["partial_week"] & wv["ratio"].notna()]
    a = cand.loc[cand["ratio"].idxmax()]
    fig = go.Figure()
    fig.add_scatter(x=wv["week_start"], y=wv["weekly_revenue"], name="Weekly revenue",
                    mode="lines", line=dict(color=SERIES_1, width=2),
                    hovertemplate="Week of %{x|%b %d, %Y}<br>$%{y:,.0f}<extra>Weekly revenue</extra>")
    fig.add_scatter(x=wv["week_start"], y=wv["rolling_4wk_avg_revenue"], name="4-week rolling avg",
                    mode="lines", line=dict(color=MUTED, width=2),
                    hovertemplate="$%{y:,.0f}<extra>4-wk rolling avg</extra>")
    fig.add_scatter(x=[a.week_start], y=[a.weekly_revenue], mode="markers", showlegend=False,
                    marker=dict(size=11, color=SERIES_1, line=dict(color=SURFACE, width=2)), hoverinfo="skip")
    fig.add_annotation(x=a.week_start, y=a.weekly_revenue, showarrow=True, arrowhead=2, arrowwidth=1.5,
                       arrowcolor=INK_2, ax=-140, ay=-10, xanchor="right", align="right",
                       font=dict(color=INK, size=13),
                       text=f"<b>Week of {a.week_start:%b %d, %Y}</b><br>{money(a.weekly_revenue)} · "
                            f"{a.ratio:.1f}× the prior 4-week avg")
    fig.update_layout(title=f"Chart 7 · Anomaly Highlight — {anomaly_cat} weekly revenue", hovermode="x unified")
    fig.update_xaxes(title_text="Week starting (Monday)")
    fig.update_yaxes(title_text="Net revenue ($)", tickprefix="$", tickformat="~s",
                     range=[0, wv["weekly_revenue"].max() * 1.15])
    st.plotly_chart(style(fig, height=420), width="stretch")
    mean_wk = w.loc[~w["partial_week"], "weekly_revenue"].mean()
    vs_mean = a.weekly_revenue / mean_wk
    if a.ratio >= 2 and vs_mean >= 2:  # a true anomaly: far above both its recent run-rate and its normal level
        headline = (f"The week of {a.week_start:%B %d, %Y} is an anomaly for {anomaly_cat}: {money(a.weekly_revenue)}, "
                    f"{a.ratio:.1f}× the preceding 4-week average and {vs_mean:.1f}× the category's average week.")
    else:
        headline = (f"No pronounced spike for {anomaly_cat} in the selected period — the largest jump is the week of "
                    f"{a.week_start:%B %d, %Y} at {a.ratio:.1f}× its prior 4-week average ({vs_mean:.1f}× an average week).")
    chart_footer(
        "retail_gold.category_trends (Q7)",
        headline + " Method: weekly revenue ÷ mean of the 4 prior weeks; anomaly if ≥ 2× that and ≥ 2× the category's "
        "average week; final partial week excluded.",
        wv[["week_start", "weekly_revenue", "rolling_4wk_avg_revenue", "baseline", "ratio"]].round({"baseline": 2, "ratio": 2}),
    )

# ── Supporting analyses: Q8 reorder alerts + Q9 YoY ──────────────────────────────────────────
st.divider()
q8, q9 = st.columns(2)
with q8:
    st.subheader("SKUs at reorder point")
    rf = reorder[reorder["invoice_year"].isin(years) & reorder["Channel"].isin(channels)
                 & reorder["Category"].isin(categories)]
    t = (rf.groupby(["StockCode", "Description", "Category", "ReorderPoint", "StockOnHand"], as_index=False)
           [["net_sold", "revenue"]].sum().sort_values("net_sold").head(20))
    st.dataframe(t, hide_index=True, width="stretch", height=420)
    n_skus = rf["StockCode"].nunique()
    st.caption(f"💡 **Interpretation:** {n_skus} SKUs sit at or below their reorder point "
               "(StockOnHand ≤ ReorderPoint). Listed are the 20 slowest sellers — candidates to *not* reorder; "
               "fast sellers on the full list are stock-out risks. SKUs with unknown reorder points are excluded, not assumed safe.")
    st.caption("Source: retail_gold.retail_analytics (Q8)")

with q9:
    yoy = (channel_month[channel_month["Channel"].isin(channels)]
           .pivot_table(index="invoice_month", columns="invoice_year", values="revenue", aggfunc="sum"))
    if {2024, 2025} <= set(yoy.columns):
        yoy["yoy_pct"] = (yoy[2025] - yoy[2024]) / yoy[2024] * 100
        yoy = yoy.reset_index()
        yoy["Month"] = [MONTH_ABBR[m - 1] for m in yoy["invoice_month"]]
        fig = go.Figure(go.Bar(
            x=yoy["Month"], y=yoy["yoy_pct"],
            marker_color=[DIVERGING["pos"] if v >= 0 else DIVERGING["neg"] for v in yoy["yoy_pct"]],
            marker_cornerradius=4,
            text=[f"{v:+.1f}%" for v in yoy["yoy_pct"]], textposition="outside", textfont_color=INK_2,
            hovertemplate="%{x}: %{y:+.1f}%<extra></extra>"))
        fig.update_layout(title="Year-over-year revenue change, 2025 vs 2024", bargap=0.35)
        fig.update_yaxes(title_text="YoY change (%)", ticksuffix="%", zeroline=True, zerolinecolor=AXIS)
        fig.update_xaxes(title_text="Month")
        st.plotly_chart(style(fig, height=420, legend=False), width="stretch")
        total = (yoy[2025].sum() - yoy[2024].sum()) / yoy[2024].sum()
        neg = yoy.loc[yoy["yoy_pct"] < 0, "Month"].tolist()
        st.caption(f"💡 **Interpretation:** Revenue grew {total:+.1%} year over year for the selected channels"
                   + (f"; only {', '.join(neg)} declined" if neg else "")
                   + (" — December's decline is the base effect of the one-off Dec 2024 spike (see Chart 7)"
                      if "Dec" in neg else "")
                   + ". (Follows the Channel filter.)")
        st.caption("Source: retail_gold.retail_analytics (Q9)")
    else:
        st.info("YoY needs both 2024 and 2025 in the data.")
