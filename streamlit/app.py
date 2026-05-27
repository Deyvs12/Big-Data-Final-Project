"""Streamlit dashboard for the mobility analytics project.

Reads aggregated collections from MongoDB and renders them across 7 tabs.
Connection params come from the MONGO_URI / MONGO_DB env vars.
"""
from __future__ import annotations

import logging
import os

import folium
import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient
from streamlit_folium import st_folium

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:admin@mobility-mongodb:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "mobility")

ZONE_CENTROIDS = {
    "Manhattan Centro":     (-73.9900, 40.7700),
    "Manhattan Norte":      (-73.9450, 40.8350),
    "Manhattan Sur":        (-73.9950, 40.7250),
    "Brooklyn Norte":       (-73.9600, 40.7000),
    "Brooklyn Sur":         (-73.9500, 40.6200),
    "Queens Centro":        (-73.8600, 40.7350),
    "Queens Norte":         (-73.8550, 40.7850),
    "Bronx":                (-73.8450, 40.8600),
    "Aeropuerto LaGuardia": (-73.8700, 40.7700),
    "Aeropuerto JFK":       (-73.7900, 40.6500),
    "Staten Island":        (-74.1550, 40.5700),
    "Aeropuerto Newark":    (-74.1750, 40.6950),
}


st.set_page_config(
    page_title="NYC Mobility Analytics",
    page_icon=":taxi:",
    layout="wide",
)


@st.cache_resource
def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)


@st.cache_data(ttl=120)
def load_collection(name: str) -> pd.DataFrame:
    client = get_mongo_client()
    try:
        docs = list(client[MONGO_DB][name].find({}, {"_id": 0}))
    except Exception as exc:
        logger.error("Mongo read failed for %s: %s", name, exc)
        return pd.DataFrame()
    return pd.DataFrame(docs)


def metric_card(label: str, value, suffix: str = "") -> None:
    st.metric(label, f"{value}{suffix}")


def overview_tab():
    st.subheader("Resumen del pipeline")
    cols = st.columns(4)

    demand_h = load_collection("demand_by_hour")
    revenue_pt = load_collection("revenue_by_payment_type")
    revenue_z = load_collection("revenue_by_zone")
    duration_h = load_collection("avg_duration_by_hour")

    if demand_h.empty:
        st.warning("Aún no hay datos en MongoDB. Ejecuta el DAG `mobility_pipeline` primero.")
        return

    total_trips = int(demand_h["trips"].sum())
    total_revenue = float(revenue_pt["total_fare"].sum()) if not revenue_pt.empty else 0.0
    avg_dur = float(duration_h["avg_duration_min"].mean()) if not duration_h.empty else 0.0
    zones_count = revenue_z["pickup_zone"].nunique() if not revenue_z.empty else 0

    with cols[0]:
        metric_card("Viajes totales", f"{total_trips:,}")
    with cols[1]:
        metric_card("Ingresos totales", f"${total_revenue:,.0f}")
    with cols[2]:
        metric_card("Duración promedio", f"{avg_dur:.1f}", " min")
    with cols[3]:
        metric_card("Zonas activas", zones_count)

    st.divider()
    st.markdown("**Tablas disponibles en MongoDB** (colección `mobility`)")
    st.code(
        "demand_by_hour, demand_by_day, top_zones_origin, top_zones_destination,\n"
        "revenue_by_zone, avg_duration_by_hour, avg_duration_by_zone,\n"
        "revenue_by_payment_type"
    )


def demand_by_hour_tab():
    st.subheader("Demanda por hora del día")
    df = load_collection("demand_by_hour")
    if df.empty:
        st.info("Sin datos.")
        return
    df = df.sort_values("hour_of_day")
    fig = px.line(df, x="hour_of_day", y="trips", markers=True,
                  title="Curva de demanda diaria",
                  labels={"hour_of_day": "Hora", "trips": "Viajes"})
    fig.update_xaxes(dtick=1)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)


def demand_by_day_tab():
    st.subheader("Demanda por día de la semana")
    df = load_collection("demand_by_day")
    if df.empty:
        st.info("Sin datos.")
        return
    df = df.sort_values("day_of_week")
    fig = px.bar(df, x="day_name", y="trips", color="is_weekend",
                 title="Viajes por día (laborable vs. fin de semana)",
                 labels={"day_name": "Día", "trips": "Viajes",
                         "is_weekend": "Fin de semana"})
    st.plotly_chart(fig, use_container_width=True)

    wd = df[~df["is_weekend"]]["trips"].sum()
    we = df[df["is_weekend"]]["trips"].sum()
    c1, c2 = st.columns(2)
    c1.metric("Días laborales", f"{wd:,}")
    c2.metric("Fin de semana", f"{we:,}")
    st.dataframe(df, use_container_width=True, hide_index=True)


def top_zones_tab():
    st.subheader("Top 10 zonas")
    a = load_collection("top_zones_origin")
    b = load_collection("top_zones_destination")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Origen**")
        if a.empty:
            st.info("Sin datos.")
        else:
            fig = px.bar(a, x="trips", y="pickup_zone", orientation="h",
                         title="Top zonas de origen")
            fig.update_yaxes(categoryorder="total ascending")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(a, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Destino**")
        if b.empty:
            st.info("Sin datos.")
        else:
            fig = px.bar(b, x="trips", y="dropoff_zone", orientation="h",
                         title="Top zonas de destino")
            fig.update_yaxes(categoryorder="total ascending")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(b, use_container_width=True, hide_index=True)


def revenue_tab():
    st.subheader("Ingresos")
    by_zone = load_collection("revenue_by_zone")
    by_pay = load_collection("revenue_by_payment_type")

    st.markdown("**Por zona de origen**")
    if by_zone.empty:
        st.info("Sin datos.")
    else:
        fig = px.bar(by_zone.sort_values("total_fare", ascending=False),
                     x="pickup_zone", y="total_fare",
                     title="Ingresos totales por zona",
                     labels={"total_fare": "Ingresos ($)"})
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(by_zone, use_container_width=True, hide_index=True)

    st.markdown("**Por tipo de pago**")
    if by_pay.empty:
        st.info("Sin datos.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.pie(by_pay, values="total_fare", names="payment_type",
                         title="Distribución de ingresos por tipo de pago")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.bar(by_pay, x="payment_type", y="avg_fare",
                         title="Ingreso promedio por viaje",
                         labels={"avg_fare": "Ingreso promedio ($)"})
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(by_pay, use_container_width=True, hide_index=True)


def duration_tab():
    st.subheader("Duración y distancia")
    by_hour = load_collection("avg_duration_by_hour")
    by_zone = load_collection("avg_duration_by_zone")

    st.markdown("**Por hora**")
    if by_hour.empty:
        st.info("Sin datos.")
    else:
        fig = px.line(by_hour.sort_values("hour_of_day"),
                      x="hour_of_day",
                      y=["avg_duration_min", "avg_distance_km"],
                      markers=True,
                      title="Duración y distancia promedio por hora")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(by_hour, use_container_width=True, hide_index=True)

    st.markdown("**Por zona**")
    if by_zone.empty:
        st.info("Sin datos.")
    else:
        fig = px.scatter(by_zone, x="avg_distance_km", y="avg_duration_min",
                         size="trips", color="pickup_zone",
                         hover_name="pickup_zone",
                         title="Duración vs. distancia por zona")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(by_zone, use_container_width=True, hide_index=True)


def heatmap_tab():
    st.subheader("Mapa de actividad por zona")
    df = load_collection("revenue_by_zone")
    if df.empty:
        st.info("Sin datos.")
        return

    df = df.copy()
    df["lon"] = df["pickup_zone"].map(lambda z: ZONE_CENTROIDS.get(z, (None, None))[0])
    df["lat"] = df["pickup_zone"].map(lambda z: ZONE_CENTROIDS.get(z, (None, None))[1])
    df = df.dropna(subset=["lon", "lat"])

    if df.empty:
        st.warning("Las zonas en MongoDB no coinciden con los centroides conocidos.")
        return

    # Folium = Leaflet tiles, no WebGL needed (works in any browser/VM).
    m = folium.Map(location=[40.74, -73.95], zoom_start=10, tiles="OpenStreetMap")

    max_trips = max(df["trips"].max(), 1)
    for _, row in df.iterrows():
        # Marker radius in pixels: 8 (smallest zone) to 40 (busiest)
        radius_px = 8 + (row["trips"] / max_trips) * 32
        popup_html = (
            f"<b>{row['pickup_zone']}</b><br>"
            f"Viajes: {int(row['trips']):,}<br>"
            f"Ingresos: ${row['total_fare']:,.0f}<br>"
            f"Ticket promedio: ${row['avg_fare']:.2f}"
        )
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=radius_px,
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=row["pickup_zone"],
            color="#ff6400",
            weight=1.5,
            fill=True,
            fill_color="#ff6400",
            fill_opacity=0.55,
        ).add_to(m)

    st_folium(m, width=None, height=500, returned_objects=[])

    st.caption("🟠 Tamaño del círculo proporcional al volumen de viajes. "
               "Haz click en una zona para ver detalles.")

    st.markdown("**Detalles**")
    st.dataframe(df[["pickup_zone", "trips", "total_fare", "avg_fare"]],
                 use_container_width=True, hide_index=True)


def main():
    st.title(":taxi: NYC Mobility Analytics")
    st.caption(
        f"Datos desde MongoDB ({MONGO_DB}). Actualiza con el DAG "
        "`mobility_pipeline` en Airflow."
    )

    tabs = st.tabs([
        "Resumen",
        "Demanda por hora",
        "Demanda por día",
        "Top zonas",
        "Ingresos",
        "Duración y distancia",
        "Mapa de calor",
    ])
    with tabs[0]: overview_tab()
    with tabs[1]: demand_by_hour_tab()
    with tabs[2]: demand_by_day_tab()
    with tabs[3]: top_zones_tab()
    with tabs[4]: revenue_tab()
    with tabs[5]: duration_tab()
    with tabs[6]: heatmap_tab()


if __name__ == "__main__":
    main()
