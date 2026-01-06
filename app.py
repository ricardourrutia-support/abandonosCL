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
# Helpers
# -------------------------
def google_sheet_export_url(sheet_url: str, gid: str, export_format: str = "xlsx") -> str:
    """
    Build a direct export URL for a Google Sheet tab (gid).
    Works for users who are logged into Google and have access in their browser.
    """
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("No pude extraer el spreadsheetId desde la URL.")
    spreadsheet_id = m.group(1)
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format={export_format}&gid={gid}"


def _normalize_colname(c: str) -> str:
    # Normaliza: quita BOM, espacios extremos y colapsa espacios internos
    if c is None:
        return ""
    c = str(c).replace("\ufeff", "").strip()
    c = re.sub(r"\s+", " ", c)
    return c


def _colmap(df: pd.DataFrame) -> dict:
    # Mapa: nombre_normalizado_en_minuscula -> nombre_original
    return {(_normalize_colname(c).lower()): c for c in df.columns}


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Encuentra una columna aunque venga con espacios/BOM/cambios de mayúsculas.
    candidates: lista de nombres esperados (ej: ["id_reservation_id"]).
    """
    cmap = _colmap(df)
    for cand in candidates:
        key = _normalize_colname(cand).lower()
        if key in cmap:
            return cmap[key]

    # Fallback: match por "contiene" (por si viene como "id_reservation_id " o similar)
    for cand in candidates:
        key = _normalize_colname(cand).lower()
        for k, orig in cmap.items():
            if key == k:
                return orig
            if key in k:
                return orig
    return None


def read_uploaded_table(file) -> pd.DataFrame:
    """
    Reads CSV/XLSX uploaded file into DataFrame with best-effort parsing.
    """
    name = file.name.lower()
    if name.endswith(".csv"):
        # Intento 1: separador automático (engine python) + utf-8-sig
        file.seek(0)
        raw = file.read()
        if isinstance(raw, str):
            raw_bytes = raw.encode("utf-8", errors="ignore")
        else:
            raw_bytes = raw

        # limpia bytes problemáticos (a veces vienen \x00)
        raw_bytes = raw_bytes.replace(b"\x00", b"")

        # intentos de decodificación
        text = None
        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                text = raw_bytes.decode(enc)
                break
            except Exception:
                text = None
        if text is None:
            text = raw_bytes.decode("utf-8", errors="ignore")

        try:
            df = pd.read_csv(
                io.StringIO(text),
                dtype=str,
                sep=None,
                engine="python",
                on_bad_lines="skip",
            )
        except Exception:
            # Fallback 2: forzar separador coma
            df = pd.read_csv(
                io.StringIO(text),
                dtype=str,
                sep=",",
                engine="python",
                on_bad_lines="skip",
            )

        # Normaliza nombres de columnas
        df.columns = [_normalize_colname(c) for c in df.columns]
        return df

    elif name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(file, dtype=str)
        df.columns = [_normalize_colname(c) for c in df.columns]
        return df

    else:
        raise ValueError("Formato no soportado. Sube CSV o Excel (xlsx).")


def to_datetime_series(s: pd.Series) -> pd.Series:
    """
    Parse datetime with multiple common formats; returns pandas datetime (NaT if fails).
    """
    if s is None:
        return pd.to_datetime(pd.Series([], dtype=str), errors="coerce")

    s2 = s.astype(str).replace({"nan": None, "None": None})
    dt = pd.to_datetime(s2, errors="coerce") #, infer_datetime_format=True (deprecated in new pandas versions but ok to remove)
    return dt


def clean_id(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series([], dtype=str)
    return s.astype(str).str.strip().replace({"nan": "", "None": "", "none": "", "null": ""})


def build_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build final output columns in requested order:
    Marca temporal, Dirección de correo electrónico, Numero, Correo registrado..., Monto a compensar,
    Motivo compensación, id_reserva, Compensación Aeropuerto, tm_start_local_at, Fecha, Hora
    """
    out = pd.DataFrame()

    out["Marca temporal"] = df.get("Fecha", pd.Series([""] * len(df))).fillna("")
    out["Dirección de correo electrónico"] = df.get("Dirección de correo electrónico", pd.Series([""] * len(df))).fillna("")
    out["Numero"] = df.get("Numero", pd.Series([""] * len(df))).fillna("")
    out["Correo registrado en Cabify para realizar la carga"] = df.get(
        "Correo registrado en Cabify para realizar la carga", pd.Series([""] * len(df))
    ).fillna("")

    # Monto a compensar: prefer Total Compensación; else Monto Transferencia + Monto Saldo
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
st.title("Cruce: Compensaciones (Sheets) + Reservas (CSV manual)")

st.info(
    "Sobre el error 401: un Google Sheet privado no se puede descargar por backend sin autenticación "
    "(OAuth / service account). Por eso la app entrega links de descarga directa para que el usuario "
    "(logueado con acceso) lo baje en el navegador y luego lo suba aquí."
)

with st.expander("1) Descarga directa desde Google Sheets (usuarios con acceso)", expanded=True):
    compensaciones_url = "https://docs.google.com/spreadsheets/d/1-RWE_IBcQTo9zJHnQuDxb_r6oxPjO-KiwRHT-jrCYIY/edit?gid=670710457#gid=670710457"
    compensaciones_gid = "670710457"

    try:
        comp_xlsx = google_sheet_export_url(compensaciones_url, compensaciones_gid, "xlsx")
        comp_csv = google_sheet_export_url(compensaciones_url, compensaciones_gid, "csv")
        st.subheader("Compensaciones")
        st.link_button("Descargar Compensaciones (XLSX)", comp_xlsx)
        st.caption("Alternativa:")
        st.link_button("Descargar Compensaciones (CSV)", comp_csv)
    except Exception as e:
        st.warning(f"No pude construir el link de export para Compensaciones. Detalle: {e}")

st.divider()
st.subheader("2) Subir archivos para procesar")

colA, colB = st.columns(2)
with colA:
    comp_file = st.file_uploader(
        "Sube Compensaciones (CSV o XLSX) descargado del Sheet",
        type=["csv", "xlsx", "xls"],
        key="comp_file",
    )
with colB:
    # MODIFICADO: accept_multiple_files=True para permitir Journeys + Cancelados
    reservas_files = st.file_uploader(
        "Sube Reservas (Puedes seleccionar varios CSV: Journeys y Cancelados)",
        type=["csv", "xlsx"],
        key="reservas_file",
        accept_multiple_files=True
    )

if not comp_file or not reservas_files:
    st.stop()

# -------------------------
# Load
# -------------------------
try:
    df_comp = read_uploaded_table(comp_file)
    
    # MODIFICADO: Lógica para cargar y unificar múltiples archivos de reserva
    list_dfs = []
    for f in reservas_files:
        df_temp = read_uploaded_table(f)
        
        # --- Lógica específica para extraer fecha según Requerimiento ---
        # Buscamos columnas clave en este archivo específico
        col_id = find_col(df_temp, ["Id Reserva", "id_reservation_id", "reservation_id"])
        col_modo = find_col(df_temp, ["Modo", "mode"])
        col_desde = find_col(df_temp, ["F.Desde Aerop", "start_local_at", "tm_start_local_at"])
        col_hacia = find_col(df_temp, ["F.Hacia Aerop"])

        if col_id:
            # Normalizar ID para el futuro merge
            df_temp["id_reserva_norm"] = clean_id(df_temp[col_id])
            
            # Calcular 'tm_start_local_at' unificado
            # Si ya tiene la columna original 'tm_start_local_at' (base Journeys antigua), la usamos como base.
            # Si es base nueva (Cancelados), calculamos según Modo OneWay.
            
            def calcular_fecha(row):
                # 1. Intentar lógica OneWay (Base Cancelados)
                val_modo = str(row.get(col_modo, "")).strip().lower()
                if val_modo == "oneway":
                    val_desde = str(row.get(col_desde, "")).strip()
                    val_hacia = str(row.get(col_hacia, "")).strip()
                    
                    # Prioridad: Desde > Hacia
                    if val_desde and val_desde.lower() not in ["nan", "none", ""]:
                        return val_desde
                    if val_hacia and val_hacia.lower() not in ["nan", "none", ""]:
                        return val_hacia
                    # Si es OneWay pero no tiene fechas, devolver vacío (se irá a manual)
                    return ""
                
                # 2. Si no es OneWay, o si no hay columna Modo:
                # Verificamos si existe la columna de fecha directa (caso Base Journeys)
                # Si el archivo tiene "tm_start_local_at" o similar (detectado en col_desde) y NO tiene columna Modo, asumimos que es fecha válida
                if col_desde and not col_modo:
                     val_old = str(row.get(col_desde, "")).strip()
                     if val_old and val_old.lower() not in ["nan", "none", ""]:
                         return val_old
                
                # Si no cumple nada, vacío
                return ""

            df_temp["tm_start_local_at"] = df_temp.apply(calcular_fecha, axis=1)
            
            # Seleccionamos solo lo necesario para el concat
            list_dfs.append(df_temp[["id_reserva_norm", "tm_start_local_at"]])
    
    if list_dfs:
        df_res = pd.concat(list_dfs, ignore_index=True)
    else:
        st.error("No se pudieron leer datos válidos de los archivos de reserva.")
        st.stop()

except Exception as e:
    st.error(f"No pude leer los archivos: {e}")
    st.stop()

# -------------------------
# Detect/validate columns (robusto) - Compensaciones
# -------------------------
comp_cols_needed = {
    "Fecha": ["Fecha"],
    "Dirección de correo electrónico": ["Dirección de correo electrónico", "Direccion de correo electronico"],
    "Numero": ["Numero", "Número"],
    "Correo registrado en Cabify para realizar la carga": ["Correo registrado en Cabify para realizar la carga"],
    "Monto Saldo": ["Monto Saldo"],
    "Monto Transferencia": ["Monto Transferencia"],
    "Total Compensación": ["Total Compensación", "Total Compensacion"],
    "Motivo compensación": ["Motivo compensación", "Motivo compensacion"],
    "id_reserva": ["id_reserva", "id reserva", "id_reserva "],
    "Clasificación": ["Clasificación", "Clasificacion"],
}

# Renombramos a nombres estándar cuando existan
for std_name, candidates in comp_cols_needed.items():
    found = find_col(df_comp, candidates)
    if found and found != std_name:
        df_comp = df_comp.rename(columns={found: std_name})

missing_comp = [k for k in comp_cols_needed.keys() if k not in df_comp.columns]
if missing_comp:
    st.warning(f"En Compensaciones faltan columnas (igual intentaré procesar): {missing_comp}")

# -------------------------
# Filter compensaciones by Motivo compensación
# -------------------------
allowed_motivos = {
    "Reserva no encuentra conductor o no llega el conductor",
    "Usuario pierde el vuelo",
}

if "Motivo compensación" in df_comp.columns:
    df_comp["Motivo compensación"] = df_comp["Motivo compensación"].astype(str).str.strip()
    df_comp_f = df_comp[df_comp["Motivo compensación"].isin(allowed_motivos)].copy()
else:
    df_comp_f = df_comp.copy()
    st.warning("No encontré 'Motivo compensación'. No apliqué filtro por motivo.")

st.write("Registros Compensaciones (post filtro):", len(df_comp_f))

# -------------------------
# Normalize keys & parse tm_start
# -------------------------
df_comp_f["id_reserva_norm"] = clean_id(df_comp_f.get("id_reserva"))

# Limpieza de Reservas antes del Merge
df_res["id_reserva_norm"] = clean_id(df_res.get("id_reserva_norm"))

# --- FIX CRÍTICO: Eliminar duplicados en Reservas para evitar explosión de filas ---
# Si un ID aparece en Journeys y Cancelados, o varias veces en Cancelados, nos quedamos con el primero que tenga fecha válida preferiblemente,
# o simplemente el primero. Ordenamos para priorizar los que tengan fecha (no vacía).
df_res = df_res.sort_values(by="tm_start_local_at", ascending=False, na_position='last')
df_res = df_res.drop_duplicates(subset=["id_reserva_norm"], keep="first")

# Parsear fecha resultante
df_res["tm_start_dt"] = to_datetime_series(df_res.get("tm_start_local_at"))

# -------------------------
# Merge ONLY by reservation id
# -------------------------
df_final = df_comp_f.merge(
    df_res[["id_reserva_norm", "tm_start_local_at", "tm_start_dt"]],
    on="id_reserva_norm",
    how="left",
    suffixes=("", "_res"),
)

# Relleno manual:
# - Si id_reserva vacío -> manual
# - Si no hubo match -> manual
# - Si hubo match pero la lógica devolvió vacío -> manual
df_final["id_reserva_norm"] = df_final["id_reserva_norm"].fillna("").astype(str).str.strip()

df_final["tm_start_local_at"] = df_final["tm_start_local_at"].fillna("").astype(str).str.strip()
mask_manual = (df_final["id_reserva_norm"] == "") | (df_final["tm_start_local_at"] == "")
df_final.loc[mask_manual, "tm_start_local_at"] = "Ingresar Manualmente"
df_final.loc[mask_manual, "tm_start_dt"] = pd.NaT

# Fecha / Hora desde tm_start_dt
df_final["Fecha_tm_start"] = np.where(
    df_final["tm_start_dt"].notna(),
    df_final["tm_start_dt"].dt.date.astype(str),
    ""
)
df_final["Hora_tm_start"] = np.where(
    df_final["tm_start_dt"].notna(),
    df_final["tm_start_dt"].dt.strftime("%H:%M:%S"),
    ""
)

# -------------------------
# Date filter UI (tm_start_dt)
# -------------------------
st.subheader("3) Filtro por fecha (tm_start_local_at)")

valid_dt = df_final["tm_start_dt"].dropna()
min_dt = valid_dt.min() if len(valid_dt) else None
max_dt = valid_dt.max() if len(valid_dt) else None

colF1, colF2, colF3 = st.columns([1, 1, 1])
with colF1:
    include_manual_rows = st.checkbox("Incluir 'Ingresar Manualmente'", value=True)

with colF2:
    start_date = st.date_input(
        "Desde",
        value=min_dt.date() if min_dt is not None else date.today(),
    )
with colF3:
    end_date = st.date_input(
        "Hasta",
        value=max_dt.date() if max_dt is not None else date.today(),
    )

df_filtered = df_final.copy()

mask_in_range = (
    df_filtered["tm_start_dt"].notna()
    & (df_filtered["tm_start_dt"].dt.date >= start_date)
    & (df_filtered["tm_start_dt"].dt.date <= end_date)
)
mask_manual2 = df_filtered["tm_start_local_at"] == "Ingresar Manualmente"

if include_manual_rows:
    df_filtered = df_filtered[mask_in_range | mask_manual2].copy()
else:
    df_filtered = df_filtered[mask_in_range].copy()

st.write("Registros después de filtro:", len(df_filtered))

# -------------------------
# Output
# -------------------------
st.subheader("4) Resultado y descarga")

output_df = build_output(df_filtered)

st.dataframe(output_df, use_container_width=True, height=460)

excel_bytes = to_excel_bytes(output_df, sheet_name="Abandonos_Cruce")
st.download_button(
    "Descargar Excel (Cruce)",
    data=excel_bytes,
    file_name="cruce_compensaciones_reservas.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
