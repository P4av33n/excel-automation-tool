import streamlit as st
import polars as pl
import pandas as pd
import io, time, os, hashlib

# ══════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════
st.set_page_config(
    page_title="CAS Pro – Excel Automation",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════
#  CSS
# ══════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif;}
.block-container{padding-top:1.2rem;padding-bottom:2rem;max-width:100%;}
[data-testid="stSidebar"]{background:#1e1b4b;}
[data-testid="stSidebar"] p,[data-testid="stSidebar"] span,[data-testid="stSidebar"] label{color:#e0e7ff;}
[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{color:#a5b4fc;font-weight:700;}
[data-testid="stSidebar"] hr{border-color:#3730a3;}
[data-testid="stSidebar"] input[type="number"]{color:#a5b4fc!important;font-weight:600!important;background:rgba(255,255,255,0.05)!important;border:1px solid rgba(165,180,252,0.2)!important;}
[data-testid="metric-container"]{background:rgba(79,70,229,0.1);border:1px solid rgba(79,70,229,0.2);border-radius:14px;padding:16px 20px;box-shadow:0 4px 15px rgba(0,0,0,.1);transition:transform .15s;}
[data-testid="metric-container"]:hover{transform:translateY(-2px);background:rgba(79,70,229,0.15);}
[data-testid="stMetricLabel"] p{color:#818cf8;font-size:.72rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;}
[data-testid="stMetricValue"]{color:#a5b4fc!important;font-size:1.5rem;font-weight:800;}
[data-testid="stDownloadButton"]>button{background:linear-gradient(135deg,#059669,#10b981)!important;color:white!important;font-weight:800!important;font-size:1rem!important;border:none!important;border-radius:10px!important;box-shadow:0 4px 18px rgba(16,185,129,.40)!important;width:100%;}
[data-testid="stDownloadButton"]>button:hover{box-shadow:0 8px 28px rgba(16,185,129,.55)!important;}
[data-testid="baseButton-primary"]{background:linear-gradient(135deg,#4f46e5,#7c3aed)!important;border:none!important;border-radius:9px!important;color:white!important;font-weight:600!important;}
.header-banner{background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 55%,#0d9488 100%);padding:24px 30px;border-radius:16px;margin-bottom:16px;box-shadow:0 8px 32px rgba(79,70,229,.25);}
.header-banner h1{color:white;margin:0;font-size:1.85rem;font-weight:800;}
.header-banner p{color:rgba(255,255,255,.82);margin:5px 0 0;font-size:.9rem;}
.cas-card{background:rgba(79,70,229,0.08);border:1px solid rgba(79,70,229,0.2);border-radius:14px;padding:20px 22px;box-shadow:0 4px 12px rgba(0,0,0,.08);transition:transform .15s;}
.cas-card:hover{transform:translateY(-3px);background:rgba(79,70,229,0.12);}
.cas-card h4{color:#818cf8;font-size:.95rem;font-weight:700;margin:0 0 5px;}
.cas-card p{color:inherit;opacity:0.9;font-size:.85rem;margin:0;line-height:1.55;}
.colleague-note{font-size:.82rem;color:#065f46;background:#d1fae5;border-left:4px solid #10b981;border-radius:0 8px 8px 0;padding:9px 12px;margin-top:8px;}
.stat-table{width:100%;border-collapse:collapse;font-size:.87rem;}
.stat-table th{background:#4f46e5;color:white;padding:8px 12px;text-align:left;font-weight:600;}
.stat-table tr:nth-child(even) td{background:rgba(79,70,229,0.05);}
.stat-table td{padding:7px 12px;color:inherit;border-bottom:1px solid rgba(79,70,229,0.1);}
.row-badge{display:inline-block;background:#e0e7ff;color:#3730a3;border-radius:20px;padding:3px 12px;font-size:.8rem;font-weight:600;}
/* ── Hide Streamlit Cloud fork/github toolbar ── */
[data-testid="stToolbar"] {visibility: hidden !important; display: none !important;}
.stAppToolbar {visibility: hidden !important; display: none !important;}
header[data-testid="stHeader"] a[href*="github"] {display: none !important;}
button[title="Fork this app"] {display: none !important;}
/* ── Always show sidebar collapse/expand toggle ── */
[data-testid="collapsedControl"] {display: flex !important; visibility: visible !important; opacity: 1 !important;}
section[data-testid="stSidebarCollapsedControl"] {display: flex !important; visibility: visible !important; opacity: 1 !important;}
button[data-testid="baseButton-header"] {display: flex !important; visibility: visible !important;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════
for k, v in [('scratchpad_data', pd.DataFrame()), ('export_ready', False),
             ('last_file', None), ('page_num', 0),
             ('analytics_result', None), ('analytics_type', None)]:
    if k not in st.session_state:
        st.session_state[k] = v

os.makedirs("exports", exist_ok=True)
PAGE_SIZE = 100   # small page → browser never freezes


# ══════════════════════════════════════════════════════
#  CACHED FUNCTIONS  (all use _df to skip hashing)
# ══════════════════════════════════════════════════════
@st.cache_data(show_spinner=False)
def load_data(file_bytes: bytes, file_name: str):
    t0 = time.perf_counter()
    size_mb = len(file_bytes) / (1024 * 1024)
    try:
        buf = io.BytesIO(file_bytes)
        df  = pl.read_csv(buf) if file_name.lower().endswith('.csv') else pl.read_excel(buf)
        df  = df.with_columns(pl.Series("S.No.", range(1, df.height + 1)))
        return df, round(time.perf_counter() - t0, 3), round(size_mb, 2)
    except Exception:
        return None, 0, round(size_mb, 2)

@st.cache_data(show_spinner=False)
def get_page(_df: pl.DataFrame, page: int, page_size: int, meta: str) -> pd.DataFrame:
    """Slices one page and converts only that slice to pandas."""
    return _df.slice(page * page_size, page_size).to_pandas()

@st.cache_data(show_spinner=False)
def filter_data(_df: pl.DataFrame, query: str, meta: str) -> pl.DataFrame:
    if not query or len(query) < 3:
        return _df
    try:
        mask = pl.any_horizontal(
            [pl.col(c).cast(pl.Utf8).str.contains(f"(?i){query}") for c in _df.columns]
        )
        return _df.filter(mask)
    except Exception:
        return _df

@st.cache_data(show_spinner=False)
def build_export(_df: pl.DataFrame, fmt: str, start: int, end: int, meta: str):
    chunk = _df.slice(start, max(1, end - start))
    buf   = io.BytesIO()
    if "XLSX" in fmt:
        # Use native Polars write_excel for orders of magnitude faster writing than Pandas
        chunk.write_excel(buf, worksheet="CAS_Export")
        mime, ext = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"
    else:
        # CSV is MUCH faster directly from Polars
        buf.write(chunk.write_csv().encode('utf-8-sig'))
        mime, ext = "text/csv", "csv"
    buf.seek(0)
    return buf.getvalue(), f"CAS_Export.{ext}", mime

@st.cache_data(show_spinner=False)
def compute_health(_df: pl.DataFrame, meta: str):
    stats = pd.DataFrame({
        "Column":   _df.columns,
        "Type":     [str(_df[c].dtype) for c in _df.columns],
        "Nulls":    [_df[c].null_count() for c in _df.columns],
        "% Miss":   [round(_df[c].null_count() / _df.height * 100, 1) for c in _df.columns],
        "Unique":   [_df[c].n_unique() for c in _df.columns],
    })
    mem_bytes = _df.estimated_size()
    return stats, mem_bytes

@st.cache_data(show_spinner=False)
def compute_corr(_df: pl.DataFrame, cols: list, sample_n: int, meta: str) -> pd.DataFrame:
    return _df.select(cols).tail(sample_n).to_pandas().corr().round(3)

@st.cache_data(show_spinner=False)
def compute_trend(_df: pl.DataFrame, cols: list, start: int, end: int, meta: str):
    return _df.select(cols).slice(start, max(1, end-start)).to_pandas()

@st.cache_data(show_spinner=False)
def compute_dist(_df: pl.DataFrame, col: str, sample_n: int, meta: str):
    s = _df[col].drop_nulls().sample(sample_n, seed=42).to_pandas()
    q1, q3 = s.quantile(.25), s.quantile(.75)
    iqr = q3 - q1
    return s, int(((s < q1-1.5*iqr) | (s > q3+1.5*iqr)).sum())

@st.cache_data(show_spinner=False)
def compute_top(_df: pl.DataFrame, col: str, n: int, meta: str):
    top = _df.sort(col, descending=True).head(n).to_pandas().reset_index(drop=True)
    bot = _df.sort(col, descending=False).head(n).to_pandas().reset_index(drop=True)
    return top, bot


# ══════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════
st.markdown("""
<div class="header-banner">
  <h1>📊 CAS Pro &mdash; Excel Automation Tool</h1>
  <p>High-performance workbench &nbsp;·&nbsp; CSV &amp; XLSX &nbsp;·&nbsp; 500,000+ rows &nbsp;·&nbsp; Corporate Accounting Solutions</p>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════
#  FILE UPLOAD
# ══════════════════════════════════════════════════════
uploaded = st.file_uploader("Upload your file — CSV or Excel (.csv, .xlsx, .xls)",
                            type=["csv","xlsx","xls"])

if uploaded is None:
    st.markdown("---")
    st.markdown("### 🚀 What CAS Pro Does")
    c1, c2, c3 = st.columns(3)
    c1.markdown('<div class="cas-card"><h4>⚡ Hyper-Speed Loading</h4><p>Polars engine loads 500k+ rows in seconds. Displays file size, load time &amp; MB/s.</p></div>', unsafe_allow_html=True)
    c2.markdown('<div class="cas-card"><h4>⬇️ Reliable Download</h4><p>XLSX or CSV export — works for you <i>and</i> every colleague on the shared URL.</p></div>', unsafe_allow_html=True)
    c3.markdown('<div class="cas-card"><h4>📊 Smart Analytics</h4><p>Data Health · Trends · Correlations · Distribution · Outlier Detection · Ranker.</p></div>', unsafe_allow_html=True)
    st.markdown("---")
    st.stop()


# ══════════════════════════════════════════════════════
#  LOAD
# ══════════════════════════════════════════════════════
file_bytes = uploaded.getvalue()
file_name  = uploaded.name

with st.spinner("⚡ Loading..."):
    df_raw, load_ms, f_size_mb = load_data(file_bytes, file_name)

if df_raw is None:
    st.error("❌ Cannot parse file. Check it is a valid CSV or Excel."); st.stop()

if st.session_state.last_file != file_name:
    st.session_state.last_file   = file_name
    st.session_state.export_ready = False
    st.session_state.page_num    = 0
    st.session_state.analytics_result = None
    st.session_state.scratchpad_data  = pd.DataFrame()
    all_cols = list(df_raw.columns)
    for v in ["Range Index","View S.No."]:
        if v not in all_cols: all_cols.append(v)
    st.session_state.column_order = all_cols
    st.session_state.pop('target_col', None)

# ── Metrics ──────────────────────────────────────────
speed = f_size_mb / load_ms if load_ms > 0 else 0
m1,m2,m3,m4 = st.columns(4)
m1.metric("📦 File Size",  f"{f_size_mb:.2f} MB")
m2.metric("⏱ Load Time",  f"{load_ms:.3f} s")
m3.metric("📋 Total Rows", f"{df_raw.height:,}")
m4.metric("🚀 MB/s",       f"{speed:.1f}")
st.markdown("")

num_cols = [c for c in df_raw.columns if df_raw[c].dtype in (pl.Int32,pl.Int64,pl.Float32,pl.Float64)]
cat_cols = [c for c in df_raw.columns if df_raw[c].dtype == pl.Utf8 and c != "S.No."]


# ══════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🛠 Controls")

    with st.expander("📐 Columns", expanded=False):
        if ('target_col' not in st.session_state or
                st.session_state.target_col not in st.session_state.column_order):
            st.session_state.target_col = st.session_state.column_order[0]
        st.session_state.target_col = st.selectbox(
            "Move:", options=st.session_state.column_order,
            index=st.session_state.column_order.index(st.session_state.target_col)
        )
        b1, b2 = st.columns(2)
        idx = st.session_state.column_order.index(st.session_state.target_col)
        if b1.button("⬆ Up") and idx > 0:
            st.session_state.column_order[idx], st.session_state.column_order[idx-1] = \
                st.session_state.column_order[idx-1], st.session_state.column_order[idx]
            st.rerun()
        if b2.button("⬇ Down") and idx < len(st.session_state.column_order)-1:
            st.session_state.column_order[idx], st.session_state.column_order[idx+1] = \
                st.session_state.column_order[idx+1], st.session_state.column_order[idx]
            st.rerun()
        selected_columns = st.multiselect(
            "Active Columns", options=st.session_state.column_order,
            default=[c for c in st.session_state.column_order if c not in ["Range Index","View S.No."]]
        )

    st.divider()
    st.markdown("### 🎯 Export Range")
    start_row = st.number_input("Start Row", min_value=0, max_value=max(0, df_raw.height-1), value=0, step=1)
    end_row   = st.number_input("End Row",   min_value=1, max_value=df_raw.height, value=min(1000, df_raw.height), step=1)
    if end_row <= start_row: end_row = start_row + 1
    st.caption(f"**{end_row - start_row:,}** rows in range")
    if st.checkbox("Export ALL rows", value=False):
        end_row = df_raw.height
        st.caption(f"⚠️ All **{df_raw.height:,}** rows will export — may be slow for XLSX")

    st.divider()
    st.markdown("### 📥 Export")
    exp_fmt = st.radio("Format", ["CSV (Text)", "XLSX (Excel)"], horizontal=True)

    ext_default = "csv" if "CSV" in exp_fmt else "xlsx"
    custom_name = st.text_input("📝 File Name", value=f"CAS_Export.{ext_default}", placeholder="Enter file name...")
    # Ensure the extension matches the selected format
    if not custom_name.endswith(f".{ext_default}"):
        custom_name = custom_name.rsplit(".", 1)[0] + f".{ext_default}" if "." in custom_name else custom_name + f".{ext_default}"

    if st.button("🚀 Prepare Export", use_container_width=True, type="primary"):
        st.session_state.export_ready = True

    if st.session_state.export_ready:
        with st.spinner("Building buffer (Fast Polars Engine)..."):
            cols_export = [c for c in selected_columns if c in df_raw.columns]
            df_export = df_raw.select(cols_export) if cols_export else df_raw
            meta = f"{file_name}_{df_raw.height}_{start_row}_{end_row}_{exp_fmt}_{len(cols_export)}"
            data_bytes, f_name, m_type = build_export(df_export, exp_fmt, start_row, end_row, meta)
        kb = len(data_bytes)/1024
        st.success(f"✅ Ready — {kb:.1f} KB")
        st.download_button(
            label=f"⬇️  Download {custom_name}",
            data=data_bytes, file_name=custom_name, mime=m_type,
            use_container_width=True,
            key=f"dl_{hashlib.md5(data_bytes[:200]).hexdigest()}"
        )


# ══════════════════════════════════════════════════════
#  MAIN TABS
# ══════════════════════════════════════════════════════
tab_wb, tab_scratch, tab_analytics = st.tabs(
    ["🗂 Data Workbench", "✏️ Scratchpad", "📊 Analytics"]
)


# ────────────────────────────────────────────────────
# TAB 1: DATA WORKBENCH — paginated, lazy
# ────────────────────────────────────────────────────
with tab_wb:
    w1, w2 = st.columns([3,1])
    with w1:
        sq = st.text_input("🔍 Filter (3+ chars):", "", placeholder="Type to filter…", key="sq")
    with w2:
        view_mode = st.selectbox("View", ["All Data","Top 10","Bottom 10","Custom Range"], key="vm")

    # Apply filter only when meaningful
    df_work = filter_data(df_raw, sq, file_name) if sq and len(sq)>=3 else df_raw

    total = df_work.height
    total_pages = max(1,(total + PAGE_SIZE - 1) // PAGE_SIZE)
    pg = max(0, min(st.session_state.page_num, total_pages-1))

    if view_mode == "Top 10":
        df_slice = df_work.head(10).to_pandas()
        footer   = f"Top 10 rows"
    elif view_mode == "Bottom 10":
        df_slice = df_work.tail(10).to_pandas()
        footer   = f"Bottom 10 rows"
    elif view_mode == "Custom Range":
        df_slice = df_work.slice(start_row, max(1, end_row-start_row)).to_pandas()
        footer   = f"Rows {start_row}–{end_row}"
    else:
        # PAGINATED — only convert one page
        df_slice = get_page(df_work, pg, PAGE_SIZE, f"{file_name}_{sq}_{pg}")
        # Pagination controls
        pn1, pn2, pn3 = st.columns([1,1,4])
        if pn1.button("◀ Prev", disabled=(pg==0)):
            st.session_state.page_num = pg - 1; st.rerun()
        if pn2.button("Next ▶", disabled=(pg>=total_pages-1)):
            st.session_state.page_num = pg + 1; st.rerun()
        pn3.markdown(f"<span class='row-badge'>Page {pg+1}/{total_pages} · {total:,} rows total</span>", unsafe_allow_html=True)
        footer = f"Showing {len(df_slice)} of {total:,} rows"

    if st.button("📋 → Scratchpad", type="secondary"):
        st.session_state.scratchpad_data = df_slice.copy()
        st.toast("✅ Sent to Scratchpad!")

    cols_show = [c for c in selected_columns if c in df_slice.columns]
    if cols_show:
        st.data_editor(df_slice[cols_show], use_container_width=True,
                       hide_index=True, disabled=True, height=380)
        st.caption(footer)
    else:
        st.warning("Select columns in the sidebar.")


# ────────────────────────────────────────────────────
# TAB 2: SCRATCHPAD
# ────────────────────────────────────────────────────
with tab_scratch:
    st.markdown("#### ✏️ Calculation Scratchpad")
    if st.session_state.scratchpad_data.empty:
        s1, s2 = st.columns(2)
        if s1.button("🆕 Blank Sheet"):
            st.session_state.scratchpad_data = pd.DataFrame({"Label":["Item 1"],"Value":[0.0],"Notes":[""]})
            st.rerun()
        if s2.button("📥 Load Current Page"):
            st.session_state.scratchpad_data = df_slice[cols_show].copy() if cols_show else df_slice.copy()
            st.rerun()
    else:
        edited = st.data_editor(st.session_state.scratchpad_data,
                                use_container_width=True, num_rows="dynamic",
                                key="sp_editor", height=350)
        st.session_state.scratchpad_data = edited
        nums = edited.select_dtypes(include='number')
        if not nums.empty:
            st.markdown("**📐 Quick Stats**")
            st.dataframe(nums.describe().round(2), use_container_width=True)
        if st.button("🗑 Clear"):
            st.session_state.scratchpad_data = pd.DataFrame(); st.rerun()


# ────────────────────────────────────────────────────
# TAB 3: ANALYTICS — button-gated (heavy ops only on click)
# ────────────────────────────────────────────────────
with tab_analytics:
    @st.fragment
    def analytics_suite_fragment():
        st.markdown("#### 📊 Analytics Suite")
        st.info("ℹ️ Select an analysis type and click **Run Analysis** — results are cached for speed.", icon="💡")

    a_type = st.radio("Analysis Type:",
        ["🏥 Data Health","📈 Trend","🔗 Correlations","📦 Distribution","🏆 Ranker"],
        horizontal=True, key="a_type"
    )

    # ── Bug Fix: Reset result if type changed ────────
    if "prev_a_type" not in st.session_state: st.session_state.prev_a_type = a_type
    if st.session_state.prev_a_type != a_type:
        st.session_state.analytics_result = None
        st.session_state.prev_a_type = a_type

    # ── Per-analysis parameters ─────────────────────
    # ── Per-analysis parameters ─────────────────────
    if a_type == "📈 Trend":
        sel_t = st.multiselect("Columns:", num_cols, default=num_cols[:min(2,len(num_cols))])
        chart_t = st.radio("Chart:", ["Area","Line","Bar"], horizontal=True)
        t_start = st.number_input("Start Index", 0, df_raw.height-1, 0)
        t_end   = st.number_input("End Index", 1, df_raw.height, min(t_start+500, df_raw.height))
        st.caption(f"Visualizing {t_end - t_start:,} rows")
    elif a_type == "🔗 Correlations":
        sel_c  = st.multiselect("Columns:", num_cols, default=num_cols[:min(6,len(num_cols))], key="a_sel_c")
        sn_c   = st.slider("Sample:", 100, min(10000, df_raw.height), min(2000, df_raw.height), 100, key="sn_c")
    elif a_type == "📦 Distribution":
        dist_col = st.selectbox("Column:", num_cols, key="dist_col")
        sn_d     = st.slider("Sample:", 100, min(30000, df_raw.height), min(5000, df_raw.height), 100, key="sn_d")
    elif a_type == "🏆 Ranker":
        rank_col = st.selectbox("Rank by:", num_cols, key="rank_col")
        rank_n   = st.slider("Top/Bottom N:", 5, 100, 10, key="rank_n")
        grp_col  = st.selectbox("Group by:", cat_cols, key="grp_col") if cat_cols else None

    if st.button("▶ Run Analysis", type="primary"):
        st.session_state.analytics_type   = a_type
        st.session_state.analytics_result = "pending"
        # Snapshot parameters to session state to prevent NameErrors if radio changes
        if a_type == "📈 Trend":
            st.session_state.params = {"cols": sel_t, "chart": chart_t, "start": t_start, "end": t_end}
        elif a_type == "🔗 Correlations":
            st.session_state.params = {"cols": sel_c, "sn": sn_c}
        elif a_type == "📦 Distribution":
            st.session_state.params = {"col": dist_col, "sn": sn_d}
        elif a_type == "🏆 Ranker":
            st.session_state.params = {"col": rank_col, "n": rank_n, "grp": grp_col}

    # ── Render results ───────────────────────────────
    if st.session_state.analytics_result == "pending":
        at = st.session_state.analytics_type
        meta = f"{file_name}_{df_raw.height}"

        if at == "🏥 Data Health":
            with st.spinner("Computing health..."):
                health, mem = compute_health(df_raw, meta)
            
            c1,c2,c3 = st.columns(3)
            c1.metric("Memory Usage", f"{mem/(1024*1024):.2f} MB")
            c2.metric("Total Cells",  f"{df_raw.height * df_raw.width:,}")
            c3.metric("Missing Count", f"{health['Nulls'].sum():,}")
            
            rows_html = ""
            for _, r in health.iterrows():
                p = r["% Miss"]
                bg,fg = ("#d1fae5","#065f46") if p==0 else (("#fef9c3","#854d0e") if p<5 else ("#fee2e2","#991b1b"))
                rows_html += (f"<tr><td>{r['Column']}</td><td>{r['Type']}</td><td>{r['Nulls']}</td>"
                              f"<td style='background:{bg};color:{fg};font-weight:700;padding:6px 10px;border-radius:5px'>{p}%</td>"
                              f"<td>{r['Unique']:,}</td></tr>")
            st.markdown(f"<table class='stat-table'><tr><th>Column</th><th>Type</th><th>Nulls</th><th>% Missing</th><th>Unique</th></tr>{rows_html}</table>", unsafe_allow_html=True)

        elif at == "📈 Trend":
            p = st.session_state.params
            with st.spinner("Slicing data..."):
                s_df = compute_trend(df_raw, p["cols"], p["start"], p["end"], f"{meta}_{p['start']}_{p['end']}")
                for c in p["cols"]:
                    st.markdown(f"**{c}**")
                    if p["chart"]=="Line": st.line_chart(s_df[c])
                    elif p["chart"]=="Area": st.area_chart(s_df[c])
                    else: st.bar_chart(s_df[c])

        elif at == "🔗 Correlations":
            p = st.session_state.params
            if len(p["cols"]) >= 2:
                with st.spinner("Computing..."):
                    corr = compute_corr(df_raw, p["cols"], p["sn"], f"{meta}_{','.join(p['cols'])}_{p['sn']}")
                def _cc(v):
                    if v>=.7:  return "#166534","#bbf7d0"
                    if v>=.3:  return "#854d0e","#fef9c3"
                    if v<=-.3: return "#991b1b","#fee2e2"
                    return "#374151","#f1f5f9"
                hdr = "".join(f"<th>{c}</th>" for c in corr.columns)
                bdy = ""
                for lbl, row in corr.iterrows():
                    cells = "".join(f"<td style='background:{_cc(v)[1]};color:{_cc(v)[0]};font-weight:600;padding:6px 10px;text-align:center'>{v}</td>" for v in row)
                    bdy += f"<tr><th style='padding:6px 10px;text-align:left'>{lbl}</th>{cells}</tr>"
                st.markdown(f"<div style='overflow-x:auto'><table class='stat-table'><tr><th></th>{hdr}</tr>{bdy}</table></div>", unsafe_allow_html=True)
                st.caption(f"Based on {p['sn']:,} rows sample")
            else:
                st.warning("Select at least 2 columns.")

        elif at == "📦 Distribution":
            p = st.session_state.params
            with st.spinner("Computing..."):
                s, out = compute_dist(df_raw, p["col"], p["sn"], f"{meta}_{p['col']}_{p['sn']}")
            d1,d2,d3,d4,d5 = st.columns(5)
            d1.metric("Mean",   f"{s.mean():.2f}")
            d2.metric("Median", f"{s.median():.2f}")
            d3.metric("Std",    f"{s.std():.2f}")
            d4.metric("Min",    f"{s.min():.2f}")
            d5.metric("Max",    f"{s.max():.2f}")
            st.area_chart(s.value_counts().sort_index().head(60))
            pct = out/p["sn"]*100
            c,b = ("#991b1b","#fee2e2") if pct>5 else ("#065f46","#d1fae5")
            st.markdown(f"<div style='background:{b};color:{c};border-radius:10px;padding:12px 18px;font-weight:600'>🔎 IQR Outliers: <b>{out:,}</b> / {p['sn']:,} ({pct:.1f}%) {'⚠️ High' if pct>5 else '✔ Clean'}</div>", unsafe_allow_html=True)

        elif at == "🏆 Ranker":
            p = st.session_state.params
            with st.spinner("Ranking..."):
                top, bot = compute_top(df_raw, p["col"], p["n"], f"{meta}_{p['col']}_{p['n']}")
            r1, r2 = st.columns(2)
            r1.markdown(f"**🏆 Top {p['n']}**")
            r1.dataframe(top, use_container_width=True)
            r2.markdown(f"**📉 Bottom {p['n']}**")
            r2.dataframe(bot, use_container_width=True)
            if p["grp"]:
                with st.spinner("Grouping..."):
                    grp = (df_raw.group_by(p["grp"])
                                 .agg(pl.mean(p["col"]).alias("Average"))
                                 .sort("Average",descending=True)
                                 .head(20).to_pandas()) # Limit to 20 for performance
                st.markdown(f"**Avg `{p['col']}` by `{p['grp']}` (Top 20)**")
                st.bar_chart(grp.set_index(p["grp"]))

    analytics_suite_fragment()
