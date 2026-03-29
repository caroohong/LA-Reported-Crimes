import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR  = os.path.join(BASE_DIR, 'data', 'raw')
PROC_DIR = os.path.join(BASE_DIR, 'data', 'processed')

os.makedirs(PROC_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# PASO 1: EXTRACCIÓN
# ─────────────────────────────────────────────

print("=" * 60)
print("PASO 1: EXTRACCIÓN")
print("=" * 60)

# 1.1 Leer datos de crimen
print("\n[1.1] Leyendo crime_data_la.csv ...")
crime_path = os.path.join(RAW_DIR, 'crime_data_la.csv')
df = pd.read_csv(crime_path, low_memory=False, encoding='latin-1')
df.columns = df.columns.str.strip()
print(f"Registros cargados: {len(df):,}")
print(f"Columnas: {list(df.columns)}")
print(df.head())

# 1.2 Descargar feriados de EE.UU. 2020–2025
print("\n[1.2] Descargando feriados federales EE.UU. 2020–2025 ...")

def descargar_feriados(years=range(2020, 2026)):
    registros = []
    for year in years:
        url = f"https://date.nager.at/api/v3/publicholidays/{year}/US"
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for h in data:
                    registros.append({
                        'date': h['date'],
                        'localName': h['localName'],
                        'name': h['name']
                    })
                print(f"{year}: {len(data)} feriados descargados")
            else:
                print(f"{year}: status {resp.status_code}")
        except Exception as e:
            print(f"{year}: error al descargar — {e}")
    df_h = pd.DataFrame(registros)
    holidays_path = os.path.join(RAW_DIR, 'holidays_us.csv')
    df_h.to_csv(holidays_path, index=False)
    print(f"Total feriados guardados: {len(df_h):,} en {holidays_path}")
    return df_h

holidays_path = os.path.join(RAW_DIR, 'holidays_us.csv')
if os.path.exists(holidays_path):
    print(f"holidays_us.csv ya existe, cargando desde disco...")
    df_holidays = pd.read_csv(holidays_path)
    print(f"{len(df_holidays):,} feriados cargados")
else:
    df_holidays = descargar_feriados()

# ─────────────────────────────────────────────
# PASO 2: TRANSFORMACIÓN
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("PASO 2: TRANSFORMACIÓN")
print("=" * 60)

n_inicial = len(df)

# 2.1 Limpieza general
print("\n[2.1] Limpieza general ...")
df.columns = df.columns.str.strip().str.replace(' ', '_')

# Eliminar duplicados
n_antes = len(df)
df = df.drop_duplicates(subset='DR_NO')
n_dupes = n_antes - len(df)
print(f"Duplicados eliminados (DR_NO): {n_dupes:,}")

# Filtrar registros sin fecha de ocurrencia
n_antes = len(df)
df = df[df['DATE_OCC'].notna()]
print(f"Registros sin DATE_OCC eliminados: {n_antes - len(df):,}")
print(f"Registros tras limpieza general: {len(df):,}")

# 2.2 Transformación de fechas
print("\n[2.2] Transformando fechas ...")
# El CSV tiene fechas con formato: '03/01/2020 12:00:00 AM'
# Intentar varios formatos
for fmt in ['%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y']:
    converted = pd.to_datetime(df['DATE_OCC'], format=fmt, errors='coerce')
    n_ok = converted.notna().sum()
    if n_ok > len(df) * 0.9:
        df['DATE_OCC'] = converted
        print(f"DATE_OCC parseada con formato '{fmt}': {n_ok:,} OK")
        break
else:
    df['DATE_OCC'] = pd.to_datetime(df['DATE_OCC'], infer_datetime_format=True, errors='coerce')
    print(f"DATE_OCC parseada con inferencia: {df['DATE_OCC'].notna().sum():,} OK")

for fmt in ['%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y']:
    converted = pd.to_datetime(df['Date_Rptd'], format=fmt, errors='coerce')
    if converted.notna().sum() > len(df) * 0.9:
        df['Date_Rptd'] = converted
        break
else:
    df['Date_Rptd'] = pd.to_datetime(df['Date_Rptd'], infer_datetime_format=True, errors='coerce')

n_antes = len(df)
df = df[df['DATE_OCC'].notna()]
print(f"Registros con DATE_OCC inválida eliminados: {n_antes - len(df):,}")

# Validar fechas futuras
hoy = pd.Timestamp.today().normalize()
n_futuras = (df['DATE_OCC'] > hoy).sum()
if n_futuras > 0:
    print(f"Fechas futuras detectadas: {n_futuras:,} — se eliminan")
    df = df[df['DATE_OCC'] <= hoy]
else:
    print(f"Sin fechas futuras")

print(f"Registros tras transformación de fechas: {len(df):,}")

# 2.3 Transformación de hora (TIME_OCC)
print("\n[2.3] Transformando TIME_OCC ...")

def parsear_hora(val):
    try:
        s = str(int(float(val))).zfill(4)
        hora   = int(s[:2])
        minuto = int(s[2:])
        if 0 <= hora <= 23 and 0 <= minuto <= 59:
            return hora, minuto
    except Exception:
        pass
    return None, None

df[['hora', 'minuto']] = df['TIME_OCC'].apply(
    lambda x: pd.Series(parsear_hora(x))
)
n_hora_invalida = df['hora'].isna().sum()
print(f"Registros con hora inválida (hora=None): {n_hora_invalida:,}")

df['bloque_15min'] = (df['minuto'] // 15 * 15).where(df['minuto'].notna())

# Columna de tiempo combinada para fact_delitos: HH:MM:SS AM/PM (granularidad 15 min)
# Usa hora + bloque_15min (0/15/30/45) con segundos fijos en 00
_h  = df['hora'].fillna(0).astype(int)
_m  = df['bloque_15min'].fillna(0).astype(int)
_sfx = _h.map(lambda h: 'AM' if h < 12 else 'PM')
_h12 = (_h % 12).replace(0, 12)
df['hora_time'] = (_h12.astype(str).str.zfill(2) + ':' +
                   _m.astype(str).str.zfill(2) + ':00 ' + _sfx)
df.loc[df['hora'].isna(), 'hora_time'] = None

# 2.4 Campos temporales derivados
print("\n[2.4] Calculando campos temporales derivados ...")
df['anio']       = df['DATE_OCC'].dt.year
df['mes']        = df['DATE_OCC'].dt.month
df['nombre_mes'] = df['DATE_OCC'].dt.strftime('%B')
df['semana_anio']= df['DATE_OCC'].dt.isocalendar().week.astype(int)
df['dia_semana'] = df['DATE_OCC'].dt.dayofweek
df['nombre_dia'] = df['DATE_OCC'].dt.strftime('%A')
df['dia_mes']    = df['DATE_OCC'].dt.day
df['trimestre']  = df['DATE_OCC'].dt.quarter
df['es_finde']   = df['dia_semana'].isin([5, 6]).astype(int)

def rango_horario(h):
    if h is None or (isinstance(h, float) and np.isnan(h)):
        return 'Desconocido'
    h = int(h)
    if 0  <= h < 6:  return 'Madrugada'
    if 6  <= h < 12: return 'Mañana'
    if 12 <= h < 18: return 'Tarde'
    return 'Noche'

df['rango_horario'] = df['hora'].apply(rango_horario)
print(f"Rango horario calculado. Distribución:")
print(df['rango_horario'].value_counts().to_string(header=False))

# 2.5 Enriquecimiento con feriados
print("\n[2.5] Enriqueciendo con feriados ...")
df_holidays['date'] = pd.to_datetime(df_holidays['date'])
df_holidays = df_holidays.rename(columns={'date': 'DATE_OCC_date'})
df_holidays_dedup = df_holidays[['DATE_OCC_date', 'localName']].drop_duplicates(subset='DATE_OCC_date')

df['DATE_OCC_date'] = df['DATE_OCC'].dt.normalize()
df = df.merge(
    df_holidays_dedup.rename(columns={'localName': 'nombre_feriado'}),
    on='DATE_OCC_date',
    how='left'
)
df['es_feriado'] = df['nombre_feriado'].notna().astype(int)
n_feriado = df['es_feriado'].sum()
print(f"Registros en feriado: {n_feriado:,} ({n_feriado/len(df)*100:.1f}%)")

# 2.6 Limpieza de víctima
print("\n[2.6] Limpiando datos de víctima ...")
df['Vict_Age'] = pd.to_numeric(df['Vict_Age'], errors='coerce')
n_edad_invalida = ((df['Vict_Age'] < 0) | (df['Vict_Age'] > 120)).sum()
df.loc[df['Vict_Age'] < 0,   'Vict_Age'] = np.nan
df.loc[df['Vict_Age'] > 120, 'Vict_Age'] = np.nan
print(f"Edades invalidas (<0 o >120): {n_edad_invalida:,} -> NaN")

def rango_etario(age):
    if pd.isna(age): return 'Desconocido'
    if age < 18:  return 'Menor'
    if age < 30:  return '18-29'
    if age < 45:  return '30-44'
    if age < 60:  return '45-59'
    return '60+'

df['rango_etario'] = df['Vict_Age'].apply(rango_etario)

df['Vict_Sex'] = df['Vict_Sex'].astype(str).str.strip().str.upper()
n_sex_invalido = (~df['Vict_Sex'].isin(['F', 'M', 'X'])).sum()
df['Vict_Sex'] = df['Vict_Sex'].apply(lambda x: x if x in ['F', 'M', 'X'] else 'X')
print(f"Sexo con valor inválido corregido a 'X': {n_sex_invalido:,}")

DESCENT_MAP = {
    'A': 'Other Asian', 'B': 'Black', 'C': 'Chinese', 'D': 'Cambodian',
    'F': 'Filipino', 'G': 'Guamanian', 'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native', 'J': 'Japanese', 'K': 'Korean',
    'L': 'Laotian', 'O': 'Other', 'P': 'Pacific Islander', 'S': 'Samoan',
    'U': 'Hawaiian', 'V': 'Vietnamese', 'W': 'White', 'X': 'Unknown',
    'Z': 'Asian Indian'
}
df['Vict_Descent_Desc'] = df['Vict_Descent'].map(DESCENT_MAP).fillna('Unknown')
print(f"Descent mapeado. Top 5:")
print(df['Vict_Descent_Desc'].value_counts().head(5).to_string(header=False))

# 2.7 Limpieza de armas y lugares
print("\n[2.7] Limpiando armas y lugares ...")
n_sin_arma = df['Weapon_Desc'].isna().sum()
df['Weapon_Desc'] = df['Weapon_Desc'].fillna('No Weapon / Unknown')
df['Premis_Desc'] = df['Premis_Desc'].fillna('Unknown')
df['Status_Desc'] = df['Status_Desc'].fillna('Unknown')
print(f"Registros sin arma (Weapon_Desc vacio -> 'No Weapon / Unknown'): {n_sin_arma:,}")

print(f"\n  RESUMEN TRANSFORMACION: {n_inicial:,} -> {len(df):,} registros finales")

# ─────────────────────────────────────────────
# PASO 3: CARGA — DATA MARTS
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("PASO 3: CARGA — GENERANDO DATA MARTS")
print("=" * 60)

# 3.1 Dimensión Tiempo
print("\n[3.1] Generando dim_tiempo ...")
dim_tiempo = df[[
    'DATE_OCC', 'anio', 'trimestre', 'mes', 'nombre_mes',
    'semana_anio', 'dia_mes', 'dia_semana', 'nombre_dia',
    'es_finde', 'hora', 'minuto', 'bloque_15min', 'rango_horario',
    'es_feriado', 'nombre_feriado'
]].drop_duplicates().reset_index(drop=True)
out = os.path.join(PROC_DIR, 'dim_tiempo.csv')
dim_tiempo.to_csv(out, index=False)
print(f"{len(dim_tiempo):,} filas ->{out}")

# 3.2 Dimensión Área
print("\n[3.2] Generando dim_area ...")
dim_area = df[['AREA', 'AREA_NAME', 'Rpt_Dist_No']].drop_duplicates().reset_index(drop=True)
dim_area.columns = ['area_id', 'area_nombre', 'rpt_dist_no']
out = os.path.join(PROC_DIR, 'dim_area.csv')
dim_area.to_csv(out, index=False)
print(f"{len(dim_area):,} filas ->{out}")

# 3.3 Dimensión Delito
print("\n[3.3] Generando dim_delito ...")
col_gravedad = 'Part_1-2' if 'Part_1-2' in df.columns else 'Part_1_2'
dim_delito = df[['Crm_Cd', 'Crm_Cd_Desc', col_gravedad]].drop_duplicates().reset_index(drop=True)
dim_delito.columns = ['crm_cd', 'crm_desc', 'gravedad']
out = os.path.join(PROC_DIR, 'dim_delito.csv')
dim_delito.to_csv(out, index=False)
print(f"{len(dim_delito):,} filas ->{out}")

# 3.4 Dimensión Víctima
print("\n[3.4] Generando dim_victima ...")
dim_victima = df[['Vict_Age', 'rango_etario', 'Vict_Sex', 'Vict_Descent', 'Vict_Descent_Desc']].drop_duplicates().reset_index(drop=True)
out = os.path.join(PROC_DIR, 'dim_victima.csv')
dim_victima.to_csv(out, index=False)
print(f"{len(dim_victima):,} filas ->{out}")

# 3.5 Dimensión Lugar
print("\n[3.5] Generando dim_lugar ...")
dim_lugar = df[['Premis_Cd', 'Premis_Desc', 'LOCATION', 'Cross_Street', 'LAT', 'LON']].drop_duplicates().reset_index(drop=True)
out = os.path.join(PROC_DIR, 'dim_lugar.csv')
dim_lugar.to_csv(out, index=False)
print(f"  {len(dim_lugar):,} filas -> {out}")

# 3.6 Fact Table
print("\n[3.6] Generando fact_delitos ...")
fact_cols = [
    'DR_NO', 'DATE_OCC', 'anio', 'mes', 'semana_anio', 'dia_mes',
    'hora_time', 'rango_horario', 'es_finde', 'es_feriado',
    'AREA', 'AREA_NAME', 'Rpt_Dist_No',
    'Crm_Cd', 'Crm_Cd_Desc', col_gravedad, 'Weapon_Desc',
    'Vict_Age', 'rango_etario', 'Vict_Sex', 'Vict_Descent_Desc',
    'Premis_Desc', 'LAT', 'LON',
    'Status_Desc', 'Date_Rptd'
]
fact_delitos = df[fact_cols].copy()
fact_delitos = fact_delitos.rename(columns={'hora_time': 'hora'})
fact_delitos['dias_hasta_reporte'] = (df['Date_Rptd'] - df['DATE_OCC']).dt.days
out = os.path.join(PROC_DIR, 'fact_delitos.csv')
fact_delitos.to_csv(out, index=False)
print(f"{len(fact_delitos):,} filas ->{out}")

# 3.7 Data Mart Resumen Temporal
print("\n[3.7] Generando mart_resumen_temporal ...")
mart_temporal = fact_delitos.groupby(
    ['anio', 'mes', 'semana_anio', 'dia_mes', 'hora',
     'AREA', 'Crm_Cd', 'es_finde', 'es_feriado'],
    dropna=False
).agg(
    total_delitos=('DR_NO', 'count')
).reset_index()
out = os.path.join(PROC_DIR, 'mart_resumen_temporal.csv')
mart_temporal.to_csv(out, index=False)
print(f"{len(mart_temporal):,} filas ->{out}")

# ─────────────────────────────────────────────
# VALIDACIONES DE CALIDAD
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("VALIDACIONES DE CALIDAD")
print("=" * 60)

errores = []

if not df['DR_NO'].is_unique:
    errores.append("DR_NO tiene duplicados")
else:
    print("Sin duplicados en DR_NO")

if df['DATE_OCC'].max() > pd.Timestamp.today():
    errores.append("Hay fechas futuras en DATE_OCC")
else:
    print("Sin fechas futuras")

invalidos_hora = df['hora'].dropna()
if ((invalidos_hora < 0) | (invalidos_hora > 23)).any():
    errores.append("Hay horas fuera de rango 0-23")
else:
    print("TIME_OCC en rango 0-23")

invalidos_edad = df['Vict_Age'].dropna()
if ((invalidos_edad < 0) | (invalidos_edad > 120)).any():
    errores.append("Hay edades fuera de rango 0-120")
else:
    print("Vict_Age en rango 0-120")

if not df['Vict_Sex'].isin(['F', 'M', 'X']).all():
    errores.append("Vict_Sex tiene valores inválidos")
else:
    print("Vict_Sex solo F/M/X")

n_lat_lon_cero = ((df['LAT'] == 0) & (df['LON'] == 0)).sum()
if n_lat_lon_cero > 0:
    print(f"LAT/LON en (0,0): {n_lat_lon_cero:,} registros (datos sin geolocalizar)")
else:
    print("Sin LAT/LON en (0,0)")

anios_datos = set(df['anio'].unique())
anios_feriados = set(pd.to_datetime(df_holidays['DATE_OCC_date']).dt.year.unique())
faltantes = anios_datos - anios_feriados
if faltantes:
    print(f"Años sin feriados cargados: {faltantes}")
else:
    print("Feriados cargados para todos los años presentes")

if errores:
    print(f"\nErrores encontrados:")
    for e in errores:
        print(f"    - {e}")
else:
    print("\nTodas las validaciones de calidad pasadas")

# ─────────────────────────────────────────────
# RESUMEN FINAL
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("RESUMEN FINAL")
print("=" * 60)
print(f"  Registros iniciales:    {n_inicial:,}")
print(f"  Registros procesados:   {len(df):,}")
print(f"  Rango de fechas:        {df['DATE_OCC'].min().date()} -> {df['DATE_OCC'].max().date()}")
print(f"  Años cubiertos:         {sorted(df['anio'].unique())}")
print(f"\n  Archivos generados en data/processed/:")
for fname in sorted(os.listdir(PROC_DIR)):
    fpath = os.path.join(PROC_DIR, fname)
    size_kb = os.path.getsize(fpath) / 1024
    print(f"    {fname:<35} {size_kb:>8.0f} KB")
print("\n  Pipeline completado exitosamente.")
