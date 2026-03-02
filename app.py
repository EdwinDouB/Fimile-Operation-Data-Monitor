import io
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import requests
import streamlit as st

# -----------------------------
# Config (env only)
# -----------------------------
API_URL_TEMPLATE = os.getenv(
    "KPI_API_URL_TEMPLATE",
    "https://isp.beans.ai/enterprise/v1/lists/status_logs"
    "?tracking_id={tracking_id}&readable=true"
    "&include_pod=true&include_item=true",
)
API_TOKEN = os.getenv("KPI_API_TOKEN", "")
API_TIMEOUT_SECONDS = int(os.getenv("KPI_API_TIMEOUT_SECONDS", "20"))

# Reference endpoints for filtering (routes/warehouses/DSP lists)
ROUTES_URL = os.getenv(
    "KPI_ROUTES_URL",
    "https://isp.beans.ai/enterprise/v1/lists/routes?updatedAfter=0&includeToday=true",
)
WAREHOUSES_URL = os.getenv(
    "KPI_WAREHOUSES_URL",
    "https://isp.beans.ai/enterprise/v1/lists/warehouses?updatedAfter=0",
)
DSPS_URL = os.getenv(
    "KPI_DSPS_URL",
    "https://isp.beans.ai/enterprise/v1/lists/thirdparty_companies",
)

# Output (keep your original columns + add filter-related columns)
OUTPUT_COLUMNS = [
    "trakcing_id",
    "shipperName",
    "created_time",
    "first_scanned_time",
    "last_scanned_time",
    "out_for_delivery_time",
    "attempted_time",
    "failed_route",
    "delivered_time",
    "success_route",
    "创建到入库时间",
    "库内停留时间",
    "尝试配送时间",
    "送达时间",
    "整体配送时间",
    # added (for filtering & visibility)
    "route_no",
    "dateStr",
    "warehouseId",
    "warehouseName",
    "dspName",
    "driverName",
]


# -----------------------------
# Helpers
# -----------------------------
def _auth_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if API_TOKEN:
        token = API_TOKEN.strip()
        if token.lower().startswith("basic ") or token.lower().startswith("bearer "):
            headers["Authorization"] = token
        else:
            headers["Authorization"] = f"Bearer {token}"
    return headers


def api_get_json(url: str, session: requests.Session) -> dict[str, Any]:
    resp = session.get(url, headers=_auth_headers(), timeout=API_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def normalize_tracking_ids(raw_ids: list[str], uppercase: bool = False) -> tuple[list[str], list[str], Counter]:
    cleaned: list[str] = []
    for value in raw_ids:
        item = str(value).strip()
        if not item:
            continue
        cleaned.append(item.upper() if uppercase else item)

    counter = Counter(cleaned)
    unique_ids: list[str] = []
    seen: set[str] = set()
    for item in cleaned:
        if item not in seen:
            seen.add(item)
            unique_ids.append(item)
    return cleaned, unique_ids, counter


def split_text_ids(text: str) -> list[str]:
    if not text:
        return []
    return [x for x in re.split(r"[\s,]+", text) if x]


def read_uploaded_ids(uploaded_file) -> list[str]:
    if uploaded_file is None:
        return []
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str)
        elif name.endswith(".xlsx"):
            df = pd.read_excel(uploaded_file, dtype=str)
        else:
            return []
    except Exception:
        return []

    if df.empty:
        return []

    preferred = [c for c in df.columns if str(c).lower() in {"tracking_id", "trackingid", "trakcing_id"}]
    if preferred:
        series = df[preferred[0]].dropna()
        return series.astype(str).tolist()

    values: list[str] = []
    for col in df.columns:
        values.extend(df[col].dropna().astype(str).tolist())
    return values


def to_local_dt(ts_millis: Any) -> datetime | None:
    if ts_millis is None:
        return None
    try:
        millis = int(ts_millis)
        return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).astimezone()
    except (ValueError, TypeError, OSError):
        return None


def fmt_dt(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def diff_hours(end_dt: datetime | None, start_dt: datetime | None) -> str:
    if not end_dt or not start_dt:
        return ""
    return f"{(end_dt - start_dt).total_seconds() / 3600:.2f}"


def parse_route(description: Any) -> str:
    text = "" if description is None else str(description)
    match = re.search(r"route[:：\s-]*(.+)$", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def normalize_events(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    if not isinstance(payload, dict):
        return []

    root = payload
    for key in ("data", "result", "response"):
        if isinstance(root.get(key), dict):
            root = root[key]
            break

    candidates = [
        root.get("listItemReadableStatusLogs"),
        root.get("listItemStatusLogs"),
        root.get("status_logs"),
        root.get("statusLogs"),
        root.get("logs"),
        root.get("events"),
        root.get("trackingEvents"),
        root.get("history"),
        root.get("checkpoints"),
    ]
    for events in candidates:
        if isinstance(events, list):
            return [e for e in events if isinstance(e, dict)]
    return []


def event_type(event: dict[str, Any]) -> str:
    for key in ("type", "eventType", "status"):
        val = event.get(key)
        if val:
            return str(val).strip().lower().replace("_", "-")

    log_item = event.get("logItem")
    if isinstance(log_item, dict):
        for key in ("type", "eventType", "status"):
            val = log_item.get(key)
            if val:
                return str(val).strip().lower().replace("_", "-")

    log_obj = event.get("log")
    if isinstance(log_obj, dict):
        for key in ("type", "eventType", "status"):
            val = log_obj.get(key)
            if val:
                return str(val).strip().lower().replace("_", "-")

    return ""


def event_ts(event: dict[str, Any]) -> int | None:
    pod = event.get("pod")
    if isinstance(pod, dict) and pod.get("podTimestampEpoch") is not None:
        try:
            return int(float(pod.get("podTimestampEpoch")) * 1000)
        except (TypeError, ValueError):
            pass

    log_item = event.get("logItem")
    if isinstance(log_item, dict):
        log_item_pod = log_item.get("pod")
        if isinstance(log_item_pod, dict) and log_item_pod.get("podTimestampEpoch") is not None:
            try:
                return int(float(log_item_pod.get("podTimestampEpoch")) * 1000)
            except (TypeError, ValueError):
                pass
        for key in ("tsMillis", "timestamp", "ts", "timeMillis"):
            val = log_item.get(key)
            try:
                if val is not None:
                    return int(val)
            except (ValueError, TypeError):
                continue

    log_obj = event.get("log")
    if isinstance(log_obj, dict):
        log_pod = log_obj.get("pod")
        if isinstance(log_pod, dict) and log_pod.get("podTimestampEpoch") is not None:
            try:
                return int(float(log_pod.get("podTimestampEpoch")) * 1000)
            except (TypeError, ValueError):
                pass
        for key in ("tsMillis", "timestamp", "ts", "timeMillis"):
            val = log_obj.get(key)
            try:
                if val is not None:
                    return int(val)
            except (ValueError, TypeError):
                continue

    for key in ("tsMillis", "timestamp", "ts", "timeMillis"):
        val = event.get(key)
        try:
            if val is not None:
                return int(val)
        except (ValueError, TypeError):
            continue

    return None


def first_event_by_predicate(events: list[dict[str, Any]], predicate) -> dict[str, Any] | None:
    filtered = [e for e in events if predicate(e)]
    if not filtered:
        return None
    with_ts = [(event_ts(e), idx, e) for idx, e in enumerate(filtered)]
    with_ts.sort(key=lambda x: (10**18 if x[0] is None else x[0], x[1]))
    return with_ts[0][2]


def last_event_by_predicate(events: list[dict[str, Any]], predicate) -> dict[str, Any] | None:
    filtered = [e for e in events if predicate(e)]
    if not filtered:
        return None
    with_ts = [(event_ts(e), idx, e) for idx, e in enumerate(filtered)]
    with_ts.sort(key=lambda x: (-1 if x[0] is None else x[0], x[1]))
    return with_ts[-1][2]


def extract_shipper_name_from_events(events: list[dict[str, Any]]) -> str:
    for event in events:
        item = event.get("item")
        if isinstance(item, dict):
            name = item.get("shipperName")
            if name:
                return str(name)
    return ""


def best_route_no(failed_route: str, success_route: str) -> str:
    # prefer success route, fallback to failed route
    r = (success_route or "").strip()
    if r:
        return r
    return (failed_route or "").strip()


def fetch_tracking_data(tracking_id: str, session: requests.Session) -> dict[str, Any]:
    if not API_URL_TEMPLATE:
        raise RuntimeError("KPI_API_URL_TEMPLATE 未配置")

    if "{tracking_id}" in API_URL_TEMPLATE:
        url = API_URL_TEMPLATE.format(tracking_id=tracking_id)
    else:
        parsed = urlparse(API_URL_TEMPLATE)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["tracking_id"] = tracking_id
        url = urlunparse(parsed._replace(query=urlencode(query)))

    resp = session.get(url, headers=_auth_headers(), timeout=API_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="export")
    return output.getvalue()


# -----------------------------
# Reference data loaders (for filter UI & route mapping)
# -----------------------------
@st.cache_data(ttl=300)
def load_routes() -> list[dict[str, Any]]:
    with requests.Session() as s:
        data = api_get_json(ROUTES_URL, s)
    routes = data.get("route")
    return [r for r in routes if isinstance(r, dict)] if isinstance(routes, list) else []


@st.cache_data(ttl=300)
def load_warehouses() -> list[dict[str, Any]]:
    with requests.Session() as s:
        data = api_get_json(WAREHOUSES_URL, s)
    whs = data.get("warehouse")
    return [w for w in whs if isinstance(w, dict)] if isinstance(whs, list) else []


@st.cache_data(ttl=300)
def load_dsps() -> list[dict[str, Any]]:
    with requests.Session() as s:
        data = api_get_json(DSPS_URL, s)
    dsps = data.get("companies")
    return [d for d in dsps if isinstance(d, dict)] if isinstance(dsps, list) else []


def build_route_index(routes: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    """
    Index by routeNo:
      {
        "CA-IE01-02/28-DX-ALEX": {
           "dateStr": "...",
           "warehouseId": "...",
           "warehouseName": "...",
           "dspName": "...",
           "driverName": "..."
        }
      }
    """
    idx: dict[str, dict[str, str]] = {}
    for r in routes:
        route_no = str(r.get("routeNo") or "").strip()
        if not route_no:
            continue

        wh = r.get("warehouse") if isinstance(r.get("warehouse"), dict) else {}
        warehouse_id = str(wh.get("listWarehouseId") or "").strip()
        warehouse_name = str(wh.get("name") or wh.get("formattedAddress") or "").strip()

        date_str = str(r.get("dateStr") or "").strip()

        dsp_name = str(r.get("companyName") or "").strip()

        # driver fields vary; try a few
        driver_name = str(
            r.get("assigneeName")
            or r.get("driverName")
            or r.get("assignee")
            or ""
        ).strip()

        idx[route_no] = {
            "dateStr": date_str,
            "warehouseId": warehouse_id,
            "warehouseName": warehouse_name,
            "dspName": dsp_name,
            "driverName": driver_name,
        }
    return idx


def build_row(tracking_id: str, payload: dict[str, Any], route_index: dict[str, dict[str, str]]) -> dict[str, str]:
    events = normalize_events(payload)
    shipper_name = extract_shipper_name_from_events(events)

    created_evt = first_event_by_predicate(events, lambda e: event_type(e) == "label")

    scanned_predicate = lambda e: (
        (desc := str(e.get("description", "")).strip().lower()).startswith("scan at")
        or desc.startswith("scanned at")
    )
    first_scanned_evt = first_event_by_predicate(events, scanned_predicate)
    last_scanned_evt = last_event_by_predicate(events, scanned_predicate)

    ofd_evt = first_event_by_predicate(events, lambda e: event_type(e) in {"out-for-delivery", "ofd", "outfordelivery"})
    fail_evt = first_event_by_predicate(events, lambda e: event_type(e) in {"fail", "failed", "failure"})
    success_evt = first_event_by_predicate(events, lambda e: event_type(e) in {"success", "delivered"})

    created_time = to_local_dt(event_ts(created_evt) if created_evt else None)
    first_scanned_time = to_local_dt(event_ts(first_scanned_evt) if first_scanned_evt else None)
    last_scanned_time = to_local_dt(event_ts(last_scanned_evt) if last_scanned_evt else None)
    out_for_delivery_time = to_local_dt(event_ts(ofd_evt) if ofd_evt else None)
    attempted_time = to_local_dt(event_ts(fail_evt) if fail_evt else None)
    delivered_time = to_local_dt(event_ts(success_evt) if success_evt else None)

    failed_route = parse_route(fail_evt.get("description")) if fail_evt else ""
    success_route = parse_route(success_evt.get("description")) if success_evt else ""

    route_no = best_route_no(failed_route, success_route)
    route_meta = route_index.get(route_no, {}) if route_no else {}

    row: dict[str, str] = {
        "trakcing_id": tracking_id,
        "shipperName": str(
            shipper_name
            or payload.get("shipperName")
            or payload.get("data", {}).get("shipperName")
            or payload.get("result", {}).get("shipperName")
            or payload.get("response", {}).get("shipperName")
            or ""
        ),
        "created_time": fmt_dt(created_time),
        "first_scanned_time": fmt_dt(first_scanned_time),
        "last_scanned_time": fmt_dt(last_scanned_time),
        "out_for_delivery_time": fmt_dt(out_for_delivery_time),
        "attempted_time": fmt_dt(attempted_time),
        "failed_route": failed_route,
        "delivered_time": fmt_dt(delivered_time),
        "success_route": success_route,
        "创建到入库时间": diff_hours(first_scanned_time, created_time),
        "库内停留时间": diff_hours(out_for_delivery_time, first_scanned_time),
        "尝试配送时间": diff_hours(attempted_time, out_for_delivery_time),
        "送达时间": diff_hours(delivered_time, out_for_delivery_time),
        "整体配送时间": diff_hours(delivered_time, created_time),
        # added
        "route_no": route_no,
        "dateStr": str(route_meta.get("dateStr", "")),
        "warehouseId": str(route_meta.get("warehouseId", "")),
        "warehouseName": str(route_meta.get("warehouseName", "")),
        "dspName": str(route_meta.get("dspName", "")),
        "driverName": str(route_meta.get("driverName", "")),
    }
    return row


def empty_row(tracking_id: str) -> dict[str, str]:
    row = {col: "" for col in OUTPUT_COLUMNS}
    row["trakcing_id"] = tracking_id
    return row


def apply_filters(
    df: pd.DataFrame,
    date_str: str,
    warehouse_id: str,
    dsp_name: str,
    include_unknown: bool,
) -> pd.DataFrame:
    """
    Filter rows by dateStr / warehouseId / dspName.
    If include_unknown=True: rows with missing meta fields are kept.
    """
    out = df.copy()

    def _match(col: str, expected: str) -> pd.Series:
        if not expected or expected == "__ALL__":
            return pd.Series([True] * len(out), index=out.index)
        if include_unknown:
            return (out[col].astype(str) == expected) | (out[col].astype(str).str.strip() == "")
        return out[col].astype(str) == expected

    # dateStr exact match
    if date_str and date_str != "__ALL__":
        out = out[_match("dateStr", date_str)]

    if warehouse_id and warehouse_id != "__ALL__":
        out = out[_match("warehouseId", warehouse_id)]

    # dspName: exact match
    if dsp_name and dsp_name != "__ALL__":
        out = out[_match("dspName", dsp_name)]

    return out


# -----------------------------
# App
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="Tracking Export (Filtered)", layout="wide")
    st.title("Tracking Export (手动运单号 + 日期/仓库/DSP 筛选输出)")

    if not API_TOKEN:
        st.warning("未检测到 KPI_API_TOKEN（env）。如果接口需要鉴权，请先配置。")

    # Load reference lists for dropdowns
    with st.spinner("加载 routes / warehouses / DSP 列表..."):
        routes = load_routes()
        warehouses = load_warehouses()
        dsps = load_dsps()

    route_index = build_route_index(routes)

    # Build dropdown options
    date_values = sorted({str(r.get("dateStr")) for r in routes if r.get("dateStr")}, reverse=True)
    wh_options = []
    wh_id_to_label: dict[str, str] = {}
    for w in warehouses:
        wh_id = str(w.get("listWarehouseId") or "").strip()
        label = f'{w.get("name","")} | {w.get("formattedAddress", w.get("address",""))} | {wh_id}'
        if wh_id:
            wh_options.append(wh_id)
            wh_id_to_label[wh_id] = label

    dsp_name_values = sorted({str(d.get("companyName")) for d in dsps if d.get("companyName")})

    st.subheader("A) 筛选条件（用于筛选输出的条目，不自动抓运单号）")
    cfa, cfb, cfc, cfd = st.columns([2, 3, 3, 2])

    picked_date = cfa.selectbox("日期 dateStr", options=["__ALL__"] + date_values, index=0)
    picked_wh = cfb.selectbox(
        "仓库 Warehouse",
        options=["__ALL__"] + wh_options,
        format_func=lambda x: "全部" if x == "__ALL__" else wh_id_to_label.get(x, x),
    )
    picked_dsp = cfc.selectbox("DSP", options=["__ALL__"] + dsp_name_values, index=0)
    include_unknown = cfd.checkbox("保留无路由信息的运单", value=False)

    st.divider()

    st.subheader("B) 手动输入 Tracking IDs")
    mode = st.radio("输入方式", ["上传文件", "文本粘贴"], horizontal=True)
    raw_ids: list[str] = []
    if mode == "上传文件":
        file = st.file_uploader("上传 CSV 或 XLSX", type=["csv", "xlsx"])
        raw_ids = read_uploaded_ids(file)
    else:
        text = st.text_area("粘贴 Tracking IDs（支持换行/逗号/空格分隔）", height=180)
        raw_ids = split_text_ids(text)

    cleaned, dedup_ids, counter = normalize_tracking_ids(raw_ids, uppercase=False)
    duplicate_ids = [k for k, v in counter.items() if v > 1]

    c1, c2, c3 = st.columns(3)
    c1.metric("input_count", len(cleaned))
    c2.metric("unique_count", len(dedup_ids))
    c3.metric("duplicate_count", len(cleaned) - len(dedup_ids))

    if duplicate_ids:
        with st.expander("重复 Tracking IDs"):
            st.write(duplicate_ids)

    st.subheader("C) Fetch / Export（先拉取所有输入运单，再按 日期/仓库/DSP 筛选输出）")

    if "result_df" not in st.session_state:
        st.session_state["result_df"] = None
    if "filtered_df" not in st.session_state:
        st.session_state["filtered_df"] = None
    if "failures" not in st.session_state:
        st.session_state["failures"] = []

    if st.button("Fetch / Export", type="primary", disabled=not dedup_ids):
        rows_by_id: dict[str, dict[str, str]] = {}
        failures: list[dict[str, str]] = []

        progress = st.progress(0)
        status = st.empty()

        with requests.Session() as session:
            total = len(dedup_ids)
            for idx, tracking_id in enumerate(dedup_ids, start=1):
                status.text(f"处理中：{idx}/{total} - {tracking_id}")
                try:
                    payload = fetch_tracking_data(tracking_id, session)
                    rows_by_id[tracking_id] = build_row(tracking_id, payload, route_index)
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else "N/A"
                    failures.append({"tracking_id": tracking_id, "reason": f"HTTP {code}"})
                    rows_by_id[tracking_id] = empty_row(tracking_id)
                except Exception as e:
                    failures.append({"tracking_id": tracking_id, "reason": str(e)})
                    rows_by_id[tracking_id] = empty_row(tracking_id)

                progress.progress(idx / total)

        ordered_rows = [rows_by_id[tid] for tid in dedup_ids]
        result_df = pd.DataFrame(ordered_rows, columns=OUTPUT_COLUMNS)

        # Apply filters AFTER fetching
        filtered_df = apply_filters(
            result_df,
            date_str=picked_date,
            warehouse_id=picked_wh,
            dsp_name=picked_dsp,
            include_unknown=include_unknown,
        )

        st.session_state["result_df"] = result_df
        st.session_state["filtered_df"] = filtered_df
        st.session_state["failures"] = failures

        status.text("处理完成")

    result_df: pd.DataFrame | None = st.session_state.get("result_df")
    filtered_df: pd.DataFrame | None = st.session_state.get("filtered_df")
    failures: list[dict[str, str]] = st.session_state.get("failures", [])

    if result_df is not None and filtered_df is not None:
        st.subheader("D) 结果统计")
        s1, s2, s3 = st.columns(3)
        s1.metric("拉取总数", len(result_df))
        s2.metric("筛选后条目数", len(filtered_df))
        s3.metric("失败数量", len(failures))

        if failures:
            st.error("以下 tracking_id 请求失败")
            fail_df = pd.DataFrame(failures)
            st.dataframe(fail_df, use_container_width=True)
            st.download_button(
                "下载失败列表 CSV",
                data=fail_df.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

        st.subheader("E) 筛选结果预览")
        st.dataframe(filtered_df.head(100), use_container_width=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S')")
        csv_data = filtered_df.to_csv(index=False).encode("utf-8-sig")

        xlsx_data = None
        try:
            xlsx_data = df_to_excel_bytes(filtered_df)
        except Exception:
            st.warning("当前环境缺少 Excel 依赖，已提供 CSV 下载。")

        c_csv, c_xlsx = st.columns(2)
        c_csv.download_button(
            "下载 CSV（筛选后）",
            data=csv_data,
            file_name=f"export_filtered_{stamp}.csv",
            mime="text/csv",
        )
        if xlsx_data is not None:
            c_xlsx.download_button(
                "下载 Excel（筛选后）",
                data=xlsx_data,
                file_name=f"export_filtered_{stamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with st.expander("查看未筛选的全量结果（调试用）"):
            st.dataframe(result_df.head(100), use_container_width=True)

        with st.expander("说明：筛选依据"):
            st.write(
                "- 这里的 日期/仓库/DSP 筛选是通过运单 status_logs 里解析到的 route_no（success_route/failed_route）"
                " 再去 routes 列表做 routeNo -> (dateStr, warehouseId, dspName, driverName) 的映射。\n"
                "- 如果某些运单无法解析 route 或 routes 列表里没有该 routeNo，会导致 dateStr/warehouseId/dspName 为空。\n"
                "- 你可以用“保留无路由信息的运单”决定是否让这些空值运单也出现在筛选结果里。"
            )


if __name__ == "__main__":
    main()
