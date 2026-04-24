"""Databricks SQL connection layer for Delta table operations.

Supports two modes:
  - Databricks App runtime: auto-configured via DATABRICKS_HOST / _TOKEN env vars
  - Local development: configure via env vars or .streamlit/secrets.toml

When DB is not configured, the app falls back to in-memory mock data.
"""

import os
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Table configuration — defaults from env, overridable via sidebar
# ---------------------------------------------------------------------------
_DEFAULT_CATALOG = os.getenv("DATABRICKS_CATALOG", "dtl_dev")
_DEFAULT_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "sol_risk_solution_demo")

TABLE_DEFAULTS = {
    "manual_acc": f"{_DEFAULT_CATALOG}.{_DEFAULT_SCHEMA}.manual_acc_data_changes",
    "f406_account": f"{_DEFAULT_CATALOG}.landing_ads_f400.f406_ads_risk_uni_pt_data",
    "trn_classified": f"{_DEFAULT_CATALOG}.{_DEFAULT_SCHEMA}.trn_classified_12m_mini",
    "trn_validation": f"{_DEFAULT_CATALOG}.{_DEFAULT_SCHEMA}.trn_validation",
}


def get_table(key: str) -> str:
    """Return the current fully-qualified table name for *key*.

    Reads from session state (set by the sidebar configurator) with a
    fallback to the env-based default. Empty or whitespace-only overrides
    (e.g. when the user clears the sidebar input) are ignored so we never
    emit SQL like ``SELECT ... FROM `` with a blank table name.
    """
    default = TABLE_DEFAULTS[key]
    override = st.session_state.get(f"tbl_{key}", default)
    if not isinstance(override, str) or not override.strip():
        return default
    return override.strip()


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _streamlit_secrets_file_exists() -> bool:
    """Avoid touching `st.secrets` when no secrets.toml exists (prevents Streamlit UI noise on Databricks Apps)."""
    candidates = (
        Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml",
        Path("/home/app/.streamlit/secrets.toml"),
        Path("/app/python/source_code/.streamlit/secrets.toml"),
    )
    return any(p.is_file() for p in candidates)


def _get_connection_params() -> dict:
    """Resolve connection params from env vars or Streamlit secrets."""
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")

    if not all([host, token, http_path]) and _streamlit_secrets_file_exists():
        try:
            host = host or st.secrets["DATABRICKS_HOST"]
            token = token or st.secrets["DATABRICKS_TOKEN"]
            http_path = http_path or st.secrets["DATABRICKS_HTTP_PATH"]
        except (KeyError, FileNotFoundError, AttributeError):
            pass

    return {
        "server_hostname": host,
        "access_token": token,
        "http_path": http_path,
    }


def is_db_configured() -> bool:
    """True when all required connection parameters are present."""
    params = _get_connection_params()
    return all(v for v in params.values())


def get_current_user() -> str:
    """Resolve the authenticated user's email from Databricks Apps headers.

    Databricks Apps forward the authenticated identity via request headers.
    We check the common variants in priority order and fall back to a local
    username so the app is usable during development.
    """
    try:
        headers = st.context.headers  # type: ignore[attr-defined]
    except Exception:
        headers = {}

    for key in (
        "X-Forwarded-Email",
        "X-Forwarded-Preferred-Username",
        "X-Forwarded-User",
    ):
        val = headers.get(key) if headers else None
        if val:
            return str(val).strip()

    return os.getenv("USER") or os.getenv("USERNAME") or "local-dev"


def render_connection_debug(table_keys: list[str]) -> None:
    """Render a debug expander showing connection status, table config, and a test query."""
    import os as _os

    db_ok = is_db_configured()
    params = _get_connection_params()

    with st.expander("Connection & Table Debug", expanded=True):
        st.subheader("Connection")
        for env_key in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"):
            _set = bool(str(_os.getenv(env_key, "")).strip())
            st.markdown(f"- `{env_key}`: **{'set' if _set else 'missing'}**")
        st.markdown(f"- `secrets.toml` found: **{_streamlit_secrets_file_exists()}**")
        st.markdown(f"- **`is_db_configured()`**: `{db_ok}`")
        if params["server_hostname"]:
            st.markdown(f"- Host: `{params['server_hostname']}`")

        st.subheader("Tables")
        for key in table_keys:
            st.markdown(f"- **{key}**: `{get_table(key)}`")

        st.subheader("Test Queries")
        if not db_ok:
            st.warning("DB not configured — skipping test query (using mock data).")
        else:
            for key in table_keys:
                tbl = get_table(key)
                if not tbl:
                    st.error(
                        f"Table name for **{key}** is empty — check the "
                        "sidebar 'Table Configuration' override."
                    )
                    continue
                try:
                    result = _read(f"SELECT COUNT(*) AS cnt FROM {tbl}")
                    cnt = result["cnt"].iloc[0]
                    st.success(f"`{tbl}` — **{cnt}** rows")
                except Exception as exc:
                    st.error(f"Query failed on `{tbl}`:\n```\n{exc}\n```")

            if "trn_classified" in table_keys:
                trn_tbl = get_table("trn_classified")
                try:
                    date_range = _read(
                        f"SELECT MIN(SNAP_DATE) AS min_dt, MAX(SNAP_DATE) AS max_dt FROM {trn_tbl}"
                    )
                    st.info(
                        f"SNAP_DATE range: **{date_range['min_dt'].iloc[0]}** → "
                        f"**{date_range['max_dt'].iloc[0]}**"
                    )
                except Exception as exc:
                    st.warning(f"Could not read SNAP_DATE range: {exc}")

            if "manual_acc" in table_keys and "f406_account" in table_keys:
                try:
                    acc_tbl = get_table("manual_acc")
                    f406_tbl = get_table("f406_account")
                    test_join = _read(
                        f"SELECT m.IBAN, f.RC_NUM "
                        f"FROM {acc_tbl} m "
                        f"LEFT JOIN {f406_tbl} f ON f.UNI_PT_KEY = m.UNI_PT_KEY "
                        f"WHERE m.IS_ACTIVE = 1 LIMIT 5"
                    )
                    st.success(
                        f"manual_acc × f406 JOIN OK — {len(test_join)} active rows sampled"
                    )
                except Exception as exc:
                    st.error(f"manual_acc × f406 JOIN test failed:\n```\n{exc}\n```")

            if "trn_classified" in table_keys and "trn_validation" in table_keys:
                val_tbl = get_table("trn_validation")
                trn_tbl = get_table("trn_classified")
                try:
                    test_join = _read(
                        f"SELECT t.ACC_TRN_KEY FROM {trn_tbl} t "
                        f"LEFT JOIN {val_tbl} v ON t.ACC_TRN_KEY = v.ACC_TRN_KEY "
                        f"LIMIT 5"
                    )
                    st.success(f"trn × trn_validation JOIN OK — {len(test_join)} rows")
                except Exception as exc:
                    st.error(f"trn × trn_validation JOIN test failed:\n```\n{exc}\n```")


@contextmanager
def _cursor():
    """Yield a Databricks SQL cursor, closing connection on exit."""
    from databricks import sql as dbsql

    params = _get_connection_params()
    conn = dbsql.connect(**params)
    try:
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()
    finally:
        conn.close()


def _read(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute a SELECT and return a DataFrame."""
    with _cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def _write(sql: str, params: dict | None = None) -> None:
    """Execute a write statement (INSERT, MERGE, etc.)."""
    with _cursor() as cur:
        cur.execute(sql, params or {})


# ---------------------------------------------------------------------------
# Screen 1 — Manual Account Data (manual_acc_data_changes_new)
#
# Storage contract
# ----------------
#   * The physical Delta table is SCD-2: each logical account may have many
#     historical rows; at most one row per IBAN has IS_ACTIVE = 1.
#   * VALID_FROM  -> when the row became active
#     VALID_TO    -> when the row was superseded (NULL while active)
#     IS_ACTIVE   -> 1 for the current row, 0 for history
#   * CREATED_AT / UPDATED_AT / CREATED_BY are audit mirrors populated by this
#     app (see databricks/migrations/001_alter_manual_acc_data_changes_new.sql).
#   * RC_NUM is NOT stored on manual_acc; it is resolved at read time via a
#     LEFT JOIN against f406_ads_risk_uni_pt_data on UNI_PT_KEY.
#
# Save semantics (one atomic Delta MERGE)
# ---------------------------------------
#   Using the classic "double source" SCD-2 pattern:
#     * source row A carries merge_iban = :IBAN  -> WHEN MATCHED closes the
#       currently active row for that IBAN (IS_ACTIVE = 0, VALID_TO = now).
#     * source row B carries merge_iban = NULL   -> never matches, falls into
#       WHEN NOT MATCHED and inserts the brand-new active row.
#   A guard on the INSERT branch (s.merge_iban IS NULL) prevents the close-row
#   source from ever producing a second INSERT when no active row pre-exists.
# ---------------------------------------------------------------------------

_MANUAL_ACC_BASE_COLUMNS = [
    "IBAN", "UNI_PT_KEY", "PT_TP_ID", "ICO_NUM",
    "PARTY_CAT", "PARTY_SUBCAT", "PARTY_SUBCAT_VALIDITY",
    "PURPOSE_CAT", "PURPOSE_SUBCAT", "PURPOSE_SUBCAT_VALIDITY",
    "CREDIT_PURPOSE_FLAG", "BLACK_LIST_FLAG",
    "SRC", "VALID_FROM", "VALID_TO", "IS_ACTIVE",
    "CREATED_AT", "UPDATED_AT", "CREATED_BY",
]


def _manual_acc_read_sql(where_clause: str, show_history: bool) -> str:
    """Build the Screen-1 SELECT that joins f406 for RC_NUM and filters SCD-2."""
    acc_table = get_table("manual_acc")
    f406_table = get_table("f406_account")
    active_filter = "" if show_history else "AND m.IS_ACTIVE = 1"
    return f"""
        WITH f406_latest AS (
            SELECT UNI_PT_KEY, RC_NUM
            FROM (
                SELECT
                    UNI_PT_KEY,
                    RC_NUM,
                    ROW_NUMBER() OVER (
                        PARTITION BY UNI_PT_KEY
                        ORDER BY SNAP_DATE DESC
                    ) AS rn
                FROM {f406_table}
                WHERE UNI_PT_KEY IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            m.IBAN,
            m.UNI_PT_KEY,
            m.PT_TP_ID,
            m.ICO_NUM,
            f.RC_NUM,
            m.PARTY_CAT,
            m.PARTY_SUBCAT,
            m.PARTY_SUBCAT_VALIDITY,
            m.PURPOSE_CAT,
            m.PURPOSE_SUBCAT,
            m.PURPOSE_SUBCAT_VALIDITY,
            m.CREDIT_PURPOSE_FLAG,
            m.BLACK_LIST_FLAG,
            m.SRC,
            m.IS_ACTIVE,
            m.VALID_FROM,
            m.VALID_TO,
            m.CREATED_AT,
            m.UPDATED_AT,
            m.CREATED_BY
        FROM {acc_table} m
        LEFT JOIN f406_latest f
               ON f.UNI_PT_KEY = m.UNI_PT_KEY
        WHERE {where_clause}
          {active_filter}
        ORDER BY m.UPDATED_AT DESC NULLS LAST, m.VALID_FROM DESC NULLS LAST
        LIMIT 1000
    """


def fetch_manual_acc_data(
    iban: str = "",
    ico: str = "",
    rc: str = "",
    show_history: bool = False,
) -> pd.DataFrame:
    """Query Screen-1 account rows with optional partial-match filters.

    Active-only by default; flip *show_history* to include closed SCD-2 rows.
    RC_NUM is resolved via LEFT JOIN with f406 on UNI_PT_KEY.
    """
    conditions = ["1=1"]
    params: dict = {}

    if iban.strip():
        conditions.append("UPPER(m.IBAN) LIKE UPPER(%(iban)s)")
        params["iban"] = f"%{iban.strip()}%"
    if ico.strip():
        conditions.append("m.ICO_NUM LIKE %(ico)s")
        params["ico"] = f"%{ico.strip()}%"
    if rc.strip():
        conditions.append("f.RC_NUM LIKE %(rc)s")
        params["rc"] = f"%{rc.strip()}%"

    sql = _manual_acc_read_sql(" AND ".join(conditions), show_history)
    return _read(sql, params)


def save_manual_acc_record(record: dict) -> None:
    """Persist a Screen-1 form submission as an SCD-2 change on manual_acc.

    One atomic MERGE: closes the currently active row for the IBAN (if any)
    and inserts the new active row. SRC is forced to 'MANUAL'. VALID_FROM /
    CREATED_AT / UPDATED_AT are stamped server-side with current_timestamp().

    Required keys in *record*:
        IBAN, UNI_PT_KEY, PT_TP_ID, ICO_NUM,
        PARTY_CAT, PARTY_SUBCAT, PARTY_SUBCAT_VALIDITY,
        PURPOSE_CAT, PURPOSE_SUBCAT, PURPOSE_SUBCAT_VALIDITY,
        CREDIT_PURPOSE_FLAG, BLACK_LIST_FLAG, CREATED_BY
    """
    sql = f"""
        MERGE INTO {get_table('manual_acc')} AS t
        USING (
            SELECT
                %(IBAN)s AS merge_iban,
                %(IBAN)s AS IBAN,
                CAST(%(UNI_PT_KEY)s AS BIGINT)            AS UNI_PT_KEY,
                %(PT_TP_ID)s                              AS PT_TP_ID,
                %(ICO_NUM)s                               AS ICO_NUM,
                %(PARTY_CAT)s                             AS PARTY_CAT,
                %(PARTY_SUBCAT)s                          AS PARTY_SUBCAT,
                CAST(%(PARTY_SUBCAT_VALIDITY)s   AS DECIMAL(38,0)) AS PARTY_SUBCAT_VALIDITY,
                %(PURPOSE_CAT)s                           AS PURPOSE_CAT,
                %(PURPOSE_SUBCAT)s                        AS PURPOSE_SUBCAT,
                CAST(%(PURPOSE_SUBCAT_VALIDITY)s AS DECIMAL(38,0)) AS PURPOSE_SUBCAT_VALIDITY,
                CAST(%(CREDIT_PURPOSE_FLAG)s AS INT)      AS CREDIT_PURPOSE_FLAG,
                CAST(%(BLACK_LIST_FLAG)s     AS INT)      AS BLACK_LIST_FLAG,
                %(CREATED_BY)s                            AS CREATED_BY
            UNION ALL
            SELECT
                CAST(NULL AS STRING) AS merge_iban,
                %(IBAN)s AS IBAN,
                CAST(%(UNI_PT_KEY)s AS BIGINT)            AS UNI_PT_KEY,
                %(PT_TP_ID)s                              AS PT_TP_ID,
                %(ICO_NUM)s                               AS ICO_NUM,
                %(PARTY_CAT)s                             AS PARTY_CAT,
                %(PARTY_SUBCAT)s                          AS PARTY_SUBCAT,
                CAST(%(PARTY_SUBCAT_VALIDITY)s   AS DECIMAL(38,0)) AS PARTY_SUBCAT_VALIDITY,
                %(PURPOSE_CAT)s                           AS PURPOSE_CAT,
                %(PURPOSE_SUBCAT)s                        AS PURPOSE_SUBCAT,
                CAST(%(PURPOSE_SUBCAT_VALIDITY)s AS DECIMAL(38,0)) AS PURPOSE_SUBCAT_VALIDITY,
                CAST(%(CREDIT_PURPOSE_FLAG)s AS INT)      AS CREDIT_PURPOSE_FLAG,
                CAST(%(BLACK_LIST_FLAG)s     AS INT)      AS BLACK_LIST_FLAG,
                %(CREATED_BY)s                            AS CREATED_BY
        ) AS s
        ON t.IBAN = s.merge_iban AND t.IS_ACTIVE = 1
        WHEN MATCHED THEN UPDATE SET
            t.IS_ACTIVE  = 0,
            t.VALID_TO   = current_timestamp(),
            t.UPDATED_AT = current_timestamp()
        WHEN NOT MATCHED AND s.merge_iban IS NULL THEN INSERT (
            IBAN, UNI_PT_KEY, PT_TP_ID, ICO_NUM,
            PARTY_CAT, PARTY_SUBCAT, PARTY_SUBCAT_VALIDITY,
            PURPOSE_CAT, PURPOSE_SUBCAT, PURPOSE_SUBCAT_VALIDITY,
            CREDIT_PURPOSE_FLAG, BLACK_LIST_FLAG,
            SRC, VALID_FROM, VALID_TO, IS_ACTIVE,
            CREATED_AT, UPDATED_AT, CREATED_BY,
            PRIMARYMD5
        ) VALUES (
            s.IBAN, s.UNI_PT_KEY, s.PT_TP_ID, s.ICO_NUM,
            s.PARTY_CAT, s.PARTY_SUBCAT, s.PARTY_SUBCAT_VALIDITY,
            s.PURPOSE_CAT, s.PURPOSE_SUBCAT, s.PURPOSE_SUBCAT_VALIDITY,
            s.CREDIT_PURPOSE_FLAG, s.BLACK_LIST_FLAG,
            'MANUAL', current_timestamp(), NULL, 1,
            current_timestamp(), current_timestamp(), s.CREATED_BY,
            md5(concat_ws('|',
                s.IBAN,
                CAST(s.UNI_PT_KEY AS STRING),
                CAST(unix_micros(current_timestamp()) AS STRING)
            ))
        )
    """
    _write(sql, record)


# ---------------------------------------------------------------------------
# Screen 2 — Transaction Labeling
# ---------------------------------------------------------------------------

def fetch_trn_for_labeling(
    pay_tp: str = "All",
    src_iban: str = "",
    src_ico: str = "",
    src_rc: str = "",
    dest_iban: str = "",
    dest_ico: str = "",
    dest_rc: str = "",
    purpose_subcat: str = "All",
    date_from: date | None = None,
    date_to: date | None = None,
    last_val_date: date | None = None,
    uncertain_only: bool = False,
    num_rows: int = 50,
) -> pd.DataFrame:
    """Fetch random transactions joined with their latest validation record."""
    conditions = ["1=1"]
    params: dict = {}

    if pay_tp != "All":
        conditions.append("t.PAY_TP_ID = %(pay_tp)s")
        params["pay_tp"] = pay_tp

    _text_filters = [
        ("src_iban", src_iban, "t.SRC_IBAN"),
        ("src_ico", src_ico, "t.SRC_ICO_NUM"),
        ("src_rc", src_rc, "t.SRC_RC_NUM"),
        ("dest_iban", dest_iban, "t.DEST_IBAN"),
        ("dest_ico", dest_ico, "t.DEST_ICO_NUM"),
        ("dest_rc", dest_rc, "t.DEST_RC_NUM"),
    ]
    for key, val, col in _text_filters:
        if val and val.strip():
            conditions.append(f"UPPER({col}) LIKE UPPER(%({key})s)")
            params[key] = f"%{val.strip()}%"

    if purpose_subcat != "All":
        conditions.append("t.PURPOSE_SUBCAT = %(purpose_subcat)s")
        params["purpose_subcat"] = purpose_subcat

    if date_from:
        conditions.append("CAST(t.SNAP_DATE AS DATE) >= %(date_from)s")
        params["date_from"] = str(date_from)
    if date_to:
        conditions.append("CAST(t.SNAP_DATE AS DATE) <= %(date_to)s")
        params["date_to"] = str(date_to)

    if last_val_date:
        conditions.append(
            "(v.LAST_VALIDATED IS NULL "
            "OR CAST(v.LAST_VALIDATED AS DATE) <= %(last_val_date)s)"
        )
        params["last_val_date"] = str(last_val_date)

    if uncertain_only:
        conditions.append(
            "("
            "COALESCE(v.LAST_PURPOSE_SUBCAT, t.PURPOSE_SUBCAT) IS NULL "
            "OR COALESCE(v.LAST_PURPOSE_SUBCAT, t.PURPOSE_SUBCAT) = '' "
            "OR COALESCE(v.LAST_PURPOSE_SUBCAT, t.PURPOSE_SUBCAT) "
            "   IN ('unclassified_general', 'not_determinable')"
            ")"
        )

    params["num_rows"] = num_rows

    val_table = get_table('trn_validation')
    trn_table = get_table('trn_classified')

    sql = f"""
        WITH latest_validations AS (
            SELECT
                ACC_TRN_KEY,
                MAX(VALIDATION_TIME_STAMP)                       AS LAST_VALIDATED,
                MAX_BY(`USER`, VALIDATION_TIME_STAMP)            AS LAST_VALIDATED_BY,
                MAX_BY(PURPOSE_SUBCAT, VALIDATION_TIME_STAMP)    AS LAST_PURPOSE_SUBCAT
            FROM {val_table}
            GROUP BY ACC_TRN_KEY
        )
        SELECT
            t.*,
            v.LAST_VALIDATED,
            COALESCE(v.LAST_VALIDATED_BY, '')    AS LAST_VALIDATED_BY,
            COALESCE(v.LAST_PURPOSE_SUBCAT, '')  AS LAST_PURPOSE_SUBCAT
        FROM {trn_table} t
        LEFT JOIN latest_validations v ON t.ACC_TRN_KEY = v.ACC_TRN_KEY
        WHERE {' AND '.join(conditions)}
        ORDER BY RAND()
        LIMIT %(num_rows)s
    """
    return _read(sql, params)


def fetch_trn_validations(acc_trn_keys: list | None = None) -> pd.DataFrame:
    """Fetch validation history, optionally filtered to specific transaction keys."""
    if acc_trn_keys is not None and len(acc_trn_keys) == 0:
        return pd.DataFrame(
            columns=["ACC_TRN_KEY", "VALIDATION_TIME_STAMP", "USER", "PURPOSE_SUBCAT", "NOTE"]
        )

    sql = f"SELECT * FROM {get_table('trn_validation')}"
    params: dict = {}

    if acc_trn_keys is not None:
        placeholders = ", ".join(f"%(k{i})s" for i in range(len(acc_trn_keys)))
        sql += f" WHERE ACC_TRN_KEY IN ({placeholders})"
        for i, k in enumerate(acc_trn_keys):
            params[f"k{i}"] = k

    sql += " ORDER BY VALIDATION_TIME_STAMP DESC"
    return _read(sql, params)


def save_trn_validations(validations: list[dict]) -> None:
    """Batch-insert validation records into TRN_VALIDATION."""
    if not validations:
        return

    insert_sql = f"""
        INSERT INTO {get_table('trn_validation')}
            (ACC_TRN_KEY, VALIDATION_TIME_STAMP, `USER`, PURPOSE_SUBCAT, NOTE)
        VALUES
            (%(ACC_TRN_KEY)s, %(VALIDATION_TIME_STAMP)s, %(USER)s, %(PURPOSE_SUBCAT)s, %(NOTE)s)
    """
    with _cursor() as cur:
        for v in validations:
            cur.execute(insert_sql, v)
