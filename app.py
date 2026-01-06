# app.py
import io
import re
import csv
from datetime import date

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Cruce Compensaciones vs Reservas", layout="wide")

# -------------------------
# Helpers Originales
# -------------------------
def google_sheet_export_url(sheet_url: str, gid: str, export_format: str = "xlsx") -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("No pude extraer el spreadsheetId desde la URL.")
    spreadsheet_id = m.group(1)
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format={export_format}&gid={gid}"

def _normalize_colname(c: str) -> str:
    if c is None: return ""
    c = str(c).replace("\ufeff", "").strip()
    c = re.sub(r"\s+", " ", c)
    return c

def _colmap(df: pd.DataFrame) -> dict:
    return {(_normalize_colname(c).lower()): c for c in df.columns}

def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cmap = _colmap(df)
    for cand in candidates:
        key = _normalize_colname(cand).lower()
        if key in cmap: return cmap[key]
    for cand in candidates:
        key = _normalize_colname(cand).lower()
        for k, orig in cmap.items():
            if key == k or key in k: return orig
    return None

def read_uploaded_table(file) -> pd.DataFrame:
    name = file.name.lower()
    if name.endswith(".csv"):
        file.seek(0)
        raw = file.read()
        if isinstance(raw, str): raw_bytes = raw.encode("utf-8", errors="ignore")
        else: raw_bytes = raw
        raw_bytes = raw_bytes.replace(b"\x00", b"")
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                text = raw_bytes.decode(enc)
                break
            except Exception: text = None
        if text is None: text = raw_bytes.decode("utf-8", errors="ignore")
        try:
            df = pd.read_csv(io.StringIO(text), dtype=str, sep=None, engine="python", on_bad_lines="skip")
        except Exception:
            df = pd.read_csv(io.StringIO(text), dtype=str, sep=",", engine="python", on_bad_lines="skip")
        df.columns = [_normalize_colname(c) for c in df.columns]
        return df
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(file, dtype=str)
        df.columns = [_normalize_colname(c) for c in df.columns]
        return df
    else:
        raise ValueError("Formato no soportado.")

def to_datetime_series(s: pd.Series) -> pd.Series:
    if s is None: return pd.to_datetime(pd.Series([], dtype=str), errors="coerce")
    s2 = s.astype(str).replace({"nan": None, "None": None})
    return pd.to_datetime(s2, errors="coerce")

def clean_id(s: pd.Series) -> pd.Series:
    if s is None: return pd.Series([], dtype=str)
    return s.astype(str).str.strip().replace({"nan": "", "None": "", "none": "", "null": ""})

def build_output(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["Marca temporal"] = df.get("Fecha", pd.Series([""] * len(df))).fillna("")
    out["Dirección de correo electrónico"] = df.get("Dirección de correo electrónico", pd.Series([""] * len(df))).fillna("")
    out["Numero"] = df.get("Numero", pd.Series([""] * len(df))).fillna("")
    out["Correo registrado en Cabify para realizar la carga"] = df.get("Correo registrado en Cabify para realizar la carga", pd.Series([""] * len(df))).fillna("")
    
    total = df.get("Total Compensación", pd.Series([None] * len(df)))
    if total is None or total.isna().all():
        ms = pd.to_numeric(df.get("Monto Saldo", 0), errors="coerce").fillna(0)
        mt = pd.to_numeric(df.get("Monto Transferencia", 0), errors="coerce").fillna(0)
        total = ms + mt
    out["Monto a compensar"] = total.fillna("")
    
    out["Motivo compensación"] = df.get("Motivo compensación", pd.Series([""] * len(df))).fillna("")
    out["id_reserva"] = df.get("id_reserva", pd.Series([""] * len(df))).fillna("")
    out["Compensación Aeropuerto"] = df.get("Clasificación", pd.Series([""] * len(df))).fillna("")
    out["tm_start_local_at"] = df.get("tm_start_local_at", pd.Series(["Ingresar Manualmente"] * len(df))).fillna("Ingresar Manualmente")
    out["Fecha"] = df.get("Fecha_tm_start", pd.Series([""] * len(df))).fillna("")
    out["Hora"] = df.get("Hora_tm_start", pd.Series([""] * len(df))).fillna("")
    return out

def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Cruce") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()

# -------------------------
# UI
# -------------------------
st.title("Cruce: Compensaciones + Reservas (Journeys y Cancelados)")

with st.expander("1) Descarga directa desde Google Sheets", expanded=False):
    compensaciones_url = "https://docs.google.com/spreadsheets/d/1-RWE_IBcQTo9zJHnQuDxb_r6oxPjO-KiwRHT-jrCYIY/edit?gid=670710457"
    comp_xlsx = google_sheet_export_url(compensaciones_url, "670710457", "xlsx")
    st.link_button("Descargar Compensaciones (XLSX)", comp_xlsx)

st.subheader("2) Subir archivos")
colA, colB = st.columns(2)
with colA:
    comp_file = st.file_uploader("Sube Compensaciones", type=["csv", "xlsx", "xls"])
with colB:
    # REQUERIMIENTO: Aceptar uno o más archivos de reservas
    res_files = st.file_uploader("Sube archivos de Reservas (Journeys y Cancelados)", type=["csv"], accept_multiple_files=True)

if not comp_file or not res_files:
    st.stop()

# -------------------------
# Carga y Unificación
# -------------------------
df_comp = read_uploaded_table(comp_file)

list_res = []
for f in res_files:
    list_res.append(read_uploaded_table(f))
df_res_raw = pd.concat(list_res, ignore_index=True)

# -------------------------
# Mapeo y Lógica de Negocio
# -------------------------
# Columnas de Compensaciones
comp_cols_needed = {
    "Fecha": ["Fecha"], "Dirección de correo electrónico": ["Dirección de correo electrónico"],
    "Numero": ["Numero"], "Correo registrado en Cabify para realizar la carga": ["Correo registrado en Cabify para realizar la carga"],
    "Monto Saldo": ["Monto Saldo"], "Monto Transferencia": ["Monto Transferencia"],
    "Total Compensación": ["Total Compensación"], "Motivo compensación": ["Motivo compensación"],
    "id_reserva": ["id_reserva", "id reserva"], "Clasificación": ["Clasificación"]
}
for std, cands in comp_cols_needed.items():
    found = find_col(df_comp, cands)
    if found: df_comp = df_comp.rename(columns={found: std})

# Columnas de Reservas (detectar dinámicamente según el tipo de base)
res_id_col = find_col(df_res_raw, ["id_reservation_id", "Id Reserva", "id_reservation"])
res_modo_col = find_col(df_res_raw, ["Modo", "mode"])
res_desde_col = find_col(df_res_raw, ["F.Desde Aerop", "start_local_at", "tm_start_local_at"])
res_hacia_col = find_col(df_res_raw, ["F.Hacia Aerop"])

if not res_id_col:
    st.error("No se encontró columna de ID en Reservas.")
    st.stop()

# Lógica de Fecha (Requerimiento OneWay y selección de tm_start)
def extract_tm_start(row):
    modo = str(row.get(res_modo_col, "")).strip().lower()
    val_desde = str(row.get(res_desde_col, "")).strip()
    val_hacia = str(row.get(res_hacia_col, "")).strip()
    
    if modo == "oneway":
        if val_desde and val_desde.lower() != "nan": return val_desde
        if val_hacia and val_hacia.lower() != "nan": return val_hacia
    
    # Si no es OneWay o no hay datos, retornamos vacío para que build_output ponga "Ingresar Manualmente"
    return ""

df_res_raw["extracted_tm"] = df_res_raw.apply(extract_tm_start, axis=1)
df_res_raw["id_reserva_norm"] = clean_id(df_res_raw[res_id_col])

# Filtro de Motivos
allowed_motivos = {"Reserva no encuentra conductor o no llega el conductor", "Usuario pierde el vuelo"}
if "Motivo compensación" in df_comp.columns:
    df_comp_f = df_comp[df_comp["Motivo compensación"].astype(str).str.strip().isin(allowed_motivos)].copy()
else:
    df_comp_f = df_comp.copy()

df_comp_f["id_reserva_norm"] = clean_id(df_comp_f.get("id_reserva"))

# -------------------------
# Merge
# -------------------------
df_final = df_comp_f.merge(
    df_res_raw[["id_reserva_norm", "extracted_tm"]],
    on="id_reserva_norm",
    how="left"
)

# Renombrar para que build_output lo reconozca
df_final = df_final.rename(columns={"extracted_tm": "tm_start_local_at"})
df_final["tm_start_dt"] = to_datetime_series(df_final["tm_start_local_at"])

# Formateo de Fecha/Hora para el reporte
df_final["Fecha_tm_start"] = df_final["tm_start_dt"].dt.date.astype(str).replace("NaT", "")
df_final["Hora_tm_start"] = df_final["tm_start_dt"].dt.strftime("%H:%M:%S").replace("NaT", "")

# -------------------------
# Filtros UI y Salida
# -------------------------
st.subheader("3) Filtro por fecha")
valid_dt = df_final["tm_start_dt"].dropna()
min_dt = valid_dt.min() if len(valid_dt) else date.today()

colF1, colF2, colF3 = st.columns(3)
with colF1: include_manual = st.checkbox("Incluir 'Ingresar Manualmente'", value=True)
with colF2: start_d = st.date_input("Desde", value=min_dt)
with colF3: end_d = st.date_input("Hasta", value=date.today())

mask_range = (df_final["tm_start_dt"].dt.date >= start_d) & (df_final["tm_start_dt"].dt.date <= end_d)
mask_manual = df_final["tm_start_local_at"].fillna("").str.strip() == ""

if include_manual: df_filtered = df_final[mask_range | mask_manual].copy()
else: df_filtered = df_final[mask_range].copy()

output_df = build_output(df_filtered)
st.dataframe(output_df, use_container_width=True)

st.download_button(
    "Descargar Excel (Cruce)",
    data=to_excel_bytes(output_df),
    file_name="cruce_compensaciones_reservas.xlsx"
)
