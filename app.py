import re
import requests
import pandas as pd
import streamlit as st

API_BASE = "https://isp.beans.ai/enterprise/v1/lists"

# ====== 在这里放你的 Basic Authorization ======
AUTH_HEADER = "Basic ZTk4NzEyNDU3Y2VkNDVlOjY1MzUzNTYyMzQzOTMxMzYzNjMxMzQzMDM0MzMzM4MzMzM="

HEADERS = {
    "Authorization": AUTH_HEADER,
    "Accept": "application/json"
}


# ------------------ API ------------------

def get_json(endpoint, params=None):
    url = f"{API_BASE}/{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_routes():
    return get_json("routes", {
        "updatedAfter": 0,
        "includeToday": "true"
    })


def fetch_metrics(account_buid):
    return get_json("routes_metrics", {
        "csvExtraAccountBuidsList": account_buid
    })


def fetch_warehouses():
    return get_json("warehouses", {
        "updatedAfter": 0
    })


def fetch_assignees():
    return get_json("assignees", {
        "updatedAfter": 0
    })


# ------------------ 工具函数 ------------------

def parse_dsp(route_name):
    m = re.search(r"-([A-Z]{2,6})-[A-Z0-9]+$", route_name)
    return m.group(1) if m else ""


def compute_counts(stop_metrics, t):
    planned = 0
    actual = 0

    for item in stop_metrics or []:
        if item.get("type") != t:
            continue

        pc = int(item.get("packageCount") or 0)
        planned += pc

        if item.get("status") == "finished":
            actual += pc

    return f"{planned}/{actual}"


# ------------------ 主逻辑 ------------------

def build_dataframe(account_buid):

    routes_data = fetch_routes()
    metrics_data = fetch_metrics(account_buid)
    wh_data = fetch_warehouses()
    asg_data = fetch_assignees()

    routes = routes_data.get("route", []) or routes_data.get("routes", [])
    metrics_list = metrics_data.get("routesMetrics", []) or []
    warehouses = wh_data.get("warehouse", []) or wh_data.get("warehouses", [])
    assignees = asg_data.get("assignee", []) or asg_data.get("assignees", [])

    metrics_by_route = {m["listRouteId"]: m for m in metrics_list if "listRouteId" in m}
    wh_by_id = {w["listWarehouseId"]: w for w in warehouses if "listWarehouseId" in w}
    asg_by_id = {a["listAssigneeId"]: a for a in assignees if "listAssigneeId" in a}

    rows = []

    for r in routes:

        route_id = r.get("listRouteId")
        route_name = r.get("name", "")
        date_str = r.get("dateStr", "")

        # 起始地址
        wh_id = (r.get("warehouse") or {}).get("listWarehouseId")
        start_address = ""
        if wh_id in wh_by_id:
            start_address = wh_by_id[wh_id].get("formattedAddress", "")

        # 司机
        asg_id = (r.get("assignee") or {}).get("listAssigneeId")
        driver = ""
        if asg_id in asg_by_id:
            driver = asg_by_id[asg_id].get("name", "")

        # DSP
        dsp = parse_dsp(route_name)
        dsp_driver = f"{dsp}-{driver}".strip("-")

        # Metrics
        stop_metrics = (metrics_by_route.get(route_id) or {}).get("stopMetrics", [])

        delivery = compute_counts(stop_metrics, "dropoff")
        pickup = compute_counts(stop_metrics, "pickup")

        rows.append({
            "route_code": route_name,
            "date": date_str,
            "start_address": start_address,
            "dsp_driver": dsp_driver,
            "delivery_count": delivery,
            "pickup_count": pickup
        })

    return pd.DataFrame(rows)


# ------------------ Streamlit UI ------------------

st.title("Route Ops Export")

account_buid = st.text_input("Account BUID")

if st.button("Generate Report"):

    try:
        df = build_dataframe(account_buid)
        st.success("Done")

        st.dataframe(df)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            csv,
            "routes_ops.csv",
            "text/csv"
        )

    except Exception as e:
        st.error(str(e))
