"""Streamlit dashboard. It NEVER starts Spark: it only reads the small aggregated
files that the Spark pipeline saved in results/tables/.

Run:  streamlit run dashboard/app.py
(Smoke-test data: set CA3_SAMPLE=1 first, or use scripts/run_dashboard.*  --sample)
"""
import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402  (config only imports the standard library)

ROOT = Path(__file__).resolve().parent.parent
REAL_TABLES = ROOT / "results" / "tables"
SAMPLE_TABLES = ROOT / "results_sample" / "tables"
T = REAL_TABLES if (REAL_TABLES / "kpis.json").exists() else (
    SAMPLE_TABLES if (SAMPLE_TABLES / "kpis.json").exists() else config.TABLES_DIR
)
DAYS = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
MONTHS = {i: m for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
RISK_COLORS = {"Low": "#4caf50", "Moderate": "#ffc107", "High": "#ff7043", "Critical": "#b71c1c"}

st.set_page_config(page_title="Traffic Accident Analytics", page_icon="🚦", layout="wide")


# ------------------------------------------------------------------ loaders
@st.cache_data(show_spinner=False)
def read_csv(name):
    return pd.read_csv(T / f"{name}.csv")


@st.cache_data(show_spinner=False)
def read_parquet(name):
    df = pd.read_parquet(T / f"{name}.parquet")
    for c in ("State", "Weather_Group"):
        df[c] = df[c].astype("category")
    return df


@st.cache_data(show_spinner=False)
def read_json(name):
    return json.loads((T / f"{name}.json").read_text(encoding="utf-8"))


def exists(*names):
    return all((T / n).exists() for n in names)


def need(*files):
    if not exists(*files):
        st.warning("Results not found yet. Run the Spark pipeline first "
                   "(`python -m src.run_pipeline`) - see README.md. "
                   f"Missing: {[f for f in files if not (T / f).exists()]}")
        return False
    return True


sample_mode = False
# ------------------------------------------------------------------ header
st.title("🚦 Traffic Congestion & Road Accident Analytics")
st.caption("Apache Spark (PySpark, Spark SQL, MLlib) → aggregated "
           "results → Streamlit + Plotly")

if not need("kpis.json", "cube_time.parquet", "cube_hour.parquet"):
    st.info(f"Looking in: `{T}`")
    st.stop()

kpis = read_json("kpis")
cube_t, cube_h = read_parquet("cube_time"), read_parquet("cube_hour")
sample_mode = bool(kpis.get("sample_mode", False))
if sample_mode:
    st.error("SAMPLE MODE: showing SYNTHETIC random data used only to test the software. "
             "These numbers are NOT real findings.")

# ------------------------------------------------------------------ filters
st.sidebar.header("Filters")
states = st.sidebar.multiselect("State", sorted(cube_t["State"].unique()))
cities = st.sidebar.multiselect("City", sorted(cube_t["City"].dropna().unique())) if "City" in cube_t.columns else []
sevs = st.sidebar.multiselect("Severity", sorted(cube_t["Severity"].unique()))
wx = st.sidebar.multiselect("Weather group", sorted(cube_t["Weather_Group"].unique()))
y_min, y_max = int(cube_t["Year"].min()), int(cube_t["Year"].max())
years = (st.sidebar.slider("Year range", y_min, y_max, (y_min, y_max))
         if y_min < y_max else (y_min, y_max))
months = st.sidebar.multiselect("Month", sorted(cube_t["Month"].unique())) if "Month" in cube_t.columns else []
hours = st.sidebar.multiselect("Hour", sorted(cube_t["Hour"].unique())) if "Hour" in cube_t.columns else []
night = st.sidebar.multiselect("Time of day", ["Day", "Night"]) if "Is_Night" in cube_t.columns else []
st.sidebar.caption("Empty selection = all values. Filters apply to the trend charts; "
                   "hotspot, risk and ML sections are computed over the whole dataset.")


def apply(df):
    m = df["Year"].between(*years)
    if states:
        m &= df["State"].isin(states)
    if cities and "City" in df.columns:
        m &= df["City"].isin(cities)
    if sevs:
        m &= df["Severity"].isin(sevs)
    if wx:
        m &= df["Weather_Group"].isin(wx)
    if months and "Month" in df.columns:
        m &= df["Month"].isin(months)
    if hours and "Hour" in df.columns:
        m &= df["Hour"].isin(hours)
    if night and "Is_Night" in df.columns:
        values = [1 if value == "Night" else 0 for value in night]
        m &= df["Is_Night"].isin(values)
    return df[m]


ft, fh = apply(cube_t), apply(cube_h)

# ------------------------------------------------------------------ KPI cards
total = int(ft["accidents"].sum())
severe = int(ft.loc[ft["Severity"] >= 3, "accidents"].sum())
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Accidents (filtered)", f"{total:,}")
c2.metric("Severe Accidents (sev 3-4)", f"{severe:,}",
          f"{100 * severe / total:.1f}% of filtered" if total else None, delta_color="off")
c3.metric("Accident Hotspots (grid cells)", f"{kpis.get('hotspot_count', 'n/a'):,}"
          if isinstance(kpis.get("hotspot_count"), int) else "n/a")
c4.metric("Highest-Risk Location", kpis.get("highest_risk_location", "n/a"),
          f"score {kpis['highest_risk_score']:.1f}" if "highest_risk_score" in kpis else None,
          delta_color="off")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📊 Trends", "🗺️ Hotspots", "⚠️ Risk", "🛣️ Infrastructure", "🤖 ML Model"])

# ------------------------------------------------------------------ trends
with tab1:
    if total == 0:
        st.info("No accidents match the current filters.")
    else:
        a, b = st.columns(2)
        s = ft.groupby("Severity", observed=True)["accidents"].sum().reset_index()
        a.plotly_chart(px.bar(s, x="Severity", y="accidents", title="Accidents by Severity",
                              text_auto=",").update_xaxes(type="category"),
                       use_container_width=True)
        h = fh.groupby("Hour", observed=True)["accidents"].sum().reset_index()
        b.plotly_chart(px.bar(h, x="Hour", y="accidents", title="Accidents by Hour of Day"),
                       use_container_width=True)
        a, b = st.columns(2)
        d = fh.groupby("Day_Of_Week", observed=True)["accidents"].sum().reset_index()
        d["Day"] = d["Day_Of_Week"].map(DAYS)
        a.plotly_chart(px.bar(d, x="Day", y="accidents", title="Accidents by Day of Week"),
                       use_container_width=True)
        m = ft.groupby("Month", observed=True)["accidents"].sum().reset_index()
        m["Month_Name"] = m["Month"].map(MONTHS)
        b.plotly_chart(px.bar(m, x="Month_Name", y="accidents", title="Accidents by Month"),
                       use_container_width=True)
        w = (ft.groupby("Weather_Group", observed=True)["accidents"].sum()
               .reset_index().sort_values("accidents", ascending=False))
        st.plotly_chart(px.bar(w, x="Weather_Group", y="accidents",
                               title="Accidents by Weather Condition (grouped)"),
                        use_container_width=True)
        if need("hour_by_day_heatmap.csv"):
            hm = read_csv("hour_by_day_heatmap")
            hm["Day"] = hm["Day_Of_Week"].map(DAYS)
            piv = hm.pivot(index="Day", columns="Hour", values="accidents").reindex(list(DAYS.values()))
            st.plotly_chart(px.imshow(piv, aspect="auto", color_continuous_scale="YlOrRd",
                                      title="Accident heat-map: day of week × hour (all data)"),
                            use_container_width=True)
        with st.expander("Top states / cities (whole dataset)"):
            if exists("by_state.csv", "top_cities.csv"):
                x, y = st.columns(2)
                x.dataframe(read_csv("by_state").head(15), use_container_width=True)
                y.dataframe(read_csv("top_cities"), use_container_width=True)

# ------------------------------------------------------------------ hotspots
with tab2:
    if need("hotspots_top.csv"):
        hs = read_csv("hotspots_top")
        if states:
            hs = hs[hs["state"].isin(states)]
        st.write("Hotspot = ~1 km grid cell (lat/lng rounded to 2 decimals) in the top 1% of "
                 "cells by accident count. Bubble size = accidents, colour = average severity.")
        if hs.empty:
            st.info("No hotspots for this selection.")
        else:
            fig = px.scatter_mapbox(
                hs, lat="center_lat", lon="center_lng", size="accidents",
                color="avg_severity", color_continuous_scale="YlOrRd", size_max=28, zoom=3,
                hover_name="city", hover_data={"state": True, "accidents": True,
                                               "avg_severity": ":.2f", "rank": True,
                                               "center_lat": False, "center_lng": False},
                mapbox_style="open-street-map", height=600)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(hs[["rank", "city", "state", "accidents", "severe_accidents",
                             "avg_severity", "Grid_ID"]].head(25), use_container_width=True)

# ------------------------------------------------------------------ risk
with tab3:
    if need("risk_cells_top.csv", "risk_category_counts.csv", "risk_method.json"):
        rm = read_json("risk_method")
        st.write("**Project-defined analytical risk score** (not an official model): "
                 f"`{rm['formula']}` with weights {rm['weights']}. Only grid cells with ≥ "
                 f"{rm['min_cell_accidents']} accidents are scored; categories are relative "
                 "(percentiles P50 / P85 / P97 of the scores).")
        rc = read_csv("risk_category_counts")
        a, b = st.columns([1, 2])
        a.plotly_chart(px.bar(rc, x="risk_category", y="grid_cells", color="risk_category",
                              color_discrete_map=RISK_COLORS, title="Grid cells per risk category",
                              category_orders={"risk_category": config.RISK_CATEGORIES}),
                       use_container_width=True)
        rk = read_csv("risk_cells_top")
        b.plotly_chart(px.scatter_mapbox(
            rk, lat="center_lat", lon="center_lng", color="risk_category",
            color_discrete_map=RISK_COLORS, size="risk_score", size_max=14, zoom=3,
            hover_name="city", hover_data=["state", "accidents", "risk_score"],
            mapbox_style="open-street-map", height=450,
            title="Top-risk grid cells"), use_container_width=True)
        st.dataframe(rk[["rank", "city", "state", "accidents", "avg_severity",
                         "adverse_weather_pct", "night_pct", "risk_score",
                         "risk_category"]].head(25), use_container_width=True)

# ------------------------------------------------------------------ infrastructure
with tab4:
    if need("infrastructure_analysis.csv"):
        inf = read_csv("infrastructure_analysis")
        pres = inf[inf["present"] == 1].copy()
        tot = inf.groupby("feature")["accidents"].sum()
        pres["share_of_accidents_pct"] = (100 * pres["accidents"] / pres["feature"].map(tot)).round(2)
        a, b = st.columns(2)
        a.plotly_chart(px.bar(pres.sort_values("share_of_accidents_pct", ascending=False),
                              x="feature", y="share_of_accidents_pct",
                              title="% of accidents occurring at/near each road feature"),
                       use_container_width=True)
        inf["Feature present"] = inf["present"].map({1: "Present", 0: "Absent"})
        b.plotly_chart(px.bar(inf, x="feature", y="severe_pct", color="Feature present",
                              barmode="group", title="Severe-accident % (present vs absent)"),
                       use_container_width=True)
        st.caption("Note: this compares accident records only (no traffic-exposure data), so it "
                   "shows association, not causation.")
        if exists("day_vs_night.csv", "environment_by_severity.csv"):
            x, y = st.columns(2)
            x.dataframe(read_csv("day_vs_night"), use_container_width=True)
            y.dataframe(read_csv("environment_by_severity"), use_container_width=True)

# ------------------------------------------------------------------ ML
with tab5:
    if need("model_metrics.json", "model_comparison.csv", "confusion_matrix.csv",
            "per_class_metrics.csv"):
        mm = read_json("model_metrics")
        best = mm["best_metrics"]
        st.write(f"Models use Spark MLlib with a common leakage-free feature pipeline and "
                 f"class-weighted training. Target mode: **{best['mode']}**, seed {best['seed']}. "
                 f"Train rows: {best['train_rows']:,} | Test rows: {best['test_rows']:,}")
        st.success(f"Best performing model by macro F1: **{mm['best_model']}**")
        cols = st.columns(5)
        cols[0].metric("Accuracy", f"{best['accuracy']:.3f}")
        cols[1].metric("Macro F1", f"{best['macro_f1']:.3f}")
        cols[2].metric("Weighted F1", f"{best['weighted_f1']:.3f}")
        cols[3].metric("Weighted Precision", f"{best['weighted_precision']:.3f}")
        cols[4].metric("Weighted Recall", f"{best['weighted_recall']:.3f}")
        if "auc_roc" in best:
            st.write(f"AUC-ROC: **{best['auc_roc']:.3f}** | AUC-PR: **{best['auc_pr']:.3f}**")
        comparison = read_csv("model_comparison")
        st.plotly_chart(px.bar(
            comparison.melt(id_vars=["model"],
                            value_vars=["accuracy", "weighted_precision", "weighted_recall",
                                        "weighted_f1", "macro_f1"],
                            var_name="metric", value_name="score"),
            x="model", y="score", color="metric", barmode="group",
            range_y=[0, 1], title="Model comparison"),
            use_container_width=True)
        a, b = st.columns(2)
        cm = pd.read_csv(T / "confusion_matrix.csv", index_col=0)
        norm = st.radio("Confusion matrix", ["Counts", "Row-normalised (recall)"], horizontal=True)
        z = cm if norm == "Counts" else cm.div(cm.sum(axis=1), axis=0).round(3)
        a.plotly_chart(px.imshow(z, text_auto=True, color_continuous_scale="Blues",
                                 labels=dict(x="Predicted", y="Actual", color="")),
                       use_container_width=True)
        b.subheader("Per-class metrics")
        b.dataframe(read_csv("per_class_metrics"), use_container_width=True)
        if exists("feature_importance.csv"):
            fi = read_csv("feature_importance").head(15).iloc[::-1]
            st.plotly_chart(px.bar(fi, x="importance", y="feature", orientation="h",
                                   title=f"Top feature importances ({mm['best_model']})"),
                            use_container_width=True)
        st.caption("Accuracy alone is misleading on imbalanced data - compare it with the "
                   "majority-class baseline and read per-class recall/F1.")
