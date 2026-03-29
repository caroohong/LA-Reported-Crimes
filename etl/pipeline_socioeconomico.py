"""
pipeline_socioeconomico.py
--------------------------
ETL que integra los datos de criminalidad de Los Angeles (2020-2025) con datos
socioeconomicos de poblacion y pobreza por censo 2024 (LA County split tracts).

Fuentes:
    data/raw/crime_data_la.csv              — 1,005,104 incidentes LAPD 2020-2025
    data/raw/2024_population_poverty.csv    — Poblacion y pobreza por tract censal 2024

Salidas en data/processed/:
    dim_socioeconomia.csv       — Dimension socioeconomica por HD con cobertura LAPD
    mart_tasa_crimen.csv        — Tasa anualizada de criminalidad por 100k hab. por HD
    mart_crimen_anual_hd.csv    — Evolucion anual por HD con contexto socioeconomico
    mart_perfil_victima_hd.csv  — Perfil de victima (sexo + etnia) por HD

Nota geografica:
    Los datos de poblacion usan Distritos de Salud (HD) del Condado de LA.
    Las 21 areas LAPD se agrupan en 9 HD segun ubicacion geografica (mapeo aproximado).
    Solo se incluyen HD con cobertura LAPD directa. HD de otras jurisdicciones
    (Glendale PD, Inglewood PD, LASD, etc.) se excluyen del analisis de criminalidad.

Criterio de anos completos:
    2020-2023: anos con datos completos, usados para tasas anualizadas.
    2024: ano con datos incompletos (~45% menos registros — posible lag de reporte).
    2025: ano parcial (solo enero-marzo). Excluido del calculo de tasas.
"""

import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR  = os.path.join(BASE_DIR, 'data', 'raw')
PROC_DIR = os.path.join(BASE_DIR, 'data', 'processed')

os.makedirs(PROC_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO GEOGRAFICO: Area LAPD → Health District (HD)
# ─────────────────────────────────────────────────────────────────────────────
# Las 21 areas LAPD se agrupan en 9 HD segun ubicacion geografica.
# Los limites no coinciden exactamente: este mapeo es la mejor aproximacion
# disponible sin un join espacial (GIS).

LAPD_HD_MAP = {
    'Central':     'Central',
    'Rampart':     'Central',           # MacArthur Park / Koreatown
    'Hollywood':   'Hollywood-Wilshire',
    'Wilshire':    'Hollywood-Wilshire',
    'West LA':     'West',
    'Pacific':     'West',              # Venice, Marina del Rey
    'West Valley': 'West Valley',
    'Topanga':     'West Valley',       # Chatsworth, Canoga Park
    'Van Nuys':    'East Valley',
    'N Hollywood': 'East Valley',
    'Mission':     'East Valley',       # Sylmar, Pacoima
    'Devonshire':  'East Valley',       # Northridge, Granada Hills
    'Foothill':    'East Valley',       # Sunland, Tujunga
    'Northeast':   'Northeast',         # Eagle Rock, Los Feliz
    'Hollenbeck':  'Northeast',         # Boyle Heights, East LA
    'Southwest':   'Southwest',         # Crenshaw, Leimert Park
    'Olympic':     'Southwest',         # Pico-Union, Koreatown Sur
    '77th Street': 'Southeast',         # South Central / Watts adyacente
    'Newton':      'Southeast',         # South Central
    'Southeast':   'Southeast',         # Watts, Jordan Downs
    'Harbor':      'Harbor',            # San Pedro, Wilmington
}

# HD validos con cobertura LAPD directa
HD_CON_LAPD = set(LAPD_HD_MAP.values())

# Inverso: HD → lista ordenada de areas LAPD
HD_LAPD_MAP = {}
for area, hd in LAPD_HD_MAP.items():
    HD_LAPD_MAP.setdefault(hd, []).append(area)

# Anos con datos completos (excluye 2025 parcial y 2024 incompleto para tasas)
ANOS_COMPLETOS = [2020, 2021, 2022, 2023]
ANO_SOSPECHOSO = 2024   # ~45% menos registros — posible lag de carga
ANO_PARCIAL    = 2025   # solo enero-marzo

# ─────────────────────────────────────────────────────────────────────────────
# PASO 1: EXTRACCION
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 65)
print("PASO 1: EXTRACCION")
print("=" * 65)

print("\n[1.1] Leyendo crime_data_la.csv ...")
crime_path = os.path.join(RAW_DIR, 'crime_data_la.csv')
df_crime = pd.read_csv(crime_path, low_memory=False, encoding='latin-1')
df_crime.columns = df_crime.columns.str.strip().str.replace(' ', '_')
print(f"  Registros cargados: {len(df_crime):,}")

print("\n[1.2] Leyendo 2024_population_poverty.csv ...")
pop_path = os.path.join(RAW_DIR, '2024_population_poverty.csv')
df_pop = pd.read_csv(pop_path, encoding='utf-8-sig')
df_pop.columns = df_pop.columns.str.strip()
print(f"  Tracts cargados: {len(df_pop):,}")

# ─────────────────────────────────────────────────────────────────────────────
# PASO 2: TRANSFORMACION
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 65)
print("PASO 2: TRANSFORMACION")
print("=" * 65)

# ── 2.1 Limpiar y preparar datos de crimen ──────────────────────────────────

print("\n[2.1] Preparando datos de crimen ...")

for fmt in ['%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y']:
    converted = pd.to_datetime(df_crime['DATE_OCC'], format=fmt, errors='coerce')
    if converted.notna().sum() > len(df_crime) * 0.9:
        df_crime['DATE_OCC'] = converted
        break
else:
    df_crime['DATE_OCC'] = pd.to_datetime(df_crime['DATE_OCC'], errors='coerce')

df_crime = df_crime.drop_duplicates(subset='DR_NO')
df_crime = df_crime[df_crime['DATE_OCC'].notna()].copy()
df_crime['anio'] = df_crime['DATE_OCC'].dt.year
df_crime['AREA_NAME'] = df_crime['AREA_NAME'].str.strip()
df_crime['hd_name'] = df_crime['AREA_NAME'].map(LAPD_HD_MAP)

# Marcar calidad del ano
def clasificar_ano(a):
    if a in ANOS_COMPLETOS:
        return 'completo'
    if a == ANO_SOSPECHOSO:
        return 'incompleto'
    return 'parcial'

df_crime['estado_anio'] = df_crime['anio'].map(clasificar_ano)

# Distribucion por ano para diagnostico
dist_anio = df_crime.groupby('anio').size().reset_index(name='registros')
print(f"\n  Distribucion de registros por ano:")
for _, row in dist_anio.iterrows():
    estado = clasificar_ano(int(row['anio']))
    tag = f"  [{estado.upper()}]" if estado != 'completo' else ''
    print(f"    {int(row['anio'])}: {int(row['registros']):>9,}{tag}")

n_sin_hd = df_crime['hd_name'].isna().sum()
print(f"\n  Areas LAPD mapeadas a HD: {len(df_crime) - n_sin_hd:,} registros")
print(f"  Registros sin mapeo HD (no deberia ser > 0): {n_sin_hd:,}")
if n_sin_hd > 0:
    print(f"  Areas sin mapear: {df_crime[df_crime['hd_name'].isna()]['AREA_NAME'].unique()}")

# ── 2.2 Limpiar y agregar datos de poblacion ─────────────────────────────────

print("\n[2.2] Preparando datos de poblacion y pobreza ...")

df_la = df_pop[df_pop['CITY'] == 'Los Angeles'].copy()
df_la['HD_name'] = df_la['HD_name'].str.strip()

# FIX: excluir HDs sin cobertura LAPD (Glendale, Inglewood, San Fernando,
# South, Torrance) — tienen tracts en LA pero su policia no es el LAPD.
hds_en_datos   = set(df_la['HD_name'].unique())
hds_sin_lapd   = hds_en_datos - HD_CON_LAPD
hds_con_lapd   = hds_en_datos & HD_CON_LAPD

print(f"  HD en datos de poblacion:          {sorted(hds_en_datos)}")
print(f"  HD con cobertura LAPD (incluidos): {sorted(hds_con_lapd)}")
print(f"  HD sin cobertura LAPD (excluidos): {sorted(hds_sin_lapd)}")
print(f"  Razon de exclusion: estos HD tienen policia propia "
      f"(Glendale PD, Inglewood PD, LASD, Torrance PD) — "
      f"no hay datos LAPD para ellos.")

df_la_lapd = df_la[df_la['HD_name'].isin(HD_CON_LAPD)].copy()
print(f"  Tracts incluidos: {len(df_la_lapd):,} de {len(df_la):,} en LA ciudad")

age_cols      = [c for c in df_la.columns if c.startswith('POP24_AGE_')]
race_pop_cols = ['POP24_WHITE', 'POP24_BLACK', 'POP24_AIAN',
                 'POP24_ASIAN', 'POP24_HNPI', 'POP24_HISPANIC']
race_pov_cols = ['POV24_WHITE', 'POV24_BLACK', 'POV24_AIAN',
                 'POV24_ASIAN', 'POV24_HNPI', 'POV24_HISPANIC']

# Grupos etarios consolidados
df_la_lapd = df_la_lapd.copy()
df_la_lapd['POP24_JOVENES']         = df_la_lapd[['POP24_AGE_0_4','POP24_AGE_5_9',
                                                    'POP24_AGE_10_14','POP24_AGE_15_17']].sum(axis=1)
df_la_lapd['POP24_ADULTOS_JOVENES'] = df_la_lapd[['POP24_AGE_18_19','POP24_AGE_20_24',
                                                    'POP24_AGE_25_29']].sum(axis=1)
df_la_lapd['POP24_ADULTOS']         = df_la_lapd[['POP24_AGE_30_34','POP24_AGE_35_44',
                                                    'POP24_AGE_45_54','POP24_AGE_55_64']].sum(axis=1)
df_la_lapd['POP24_ADULTOS_MAYORES'] = df_la_lapd[['POP24_AGE_65_74','POP24_AGE_75_84',
                                                    'POP24_AGE_85_100']].sum(axis=1)

# ── 2.3 Agregar poblacion por HD ─────────────────────────────────────────────

print("\n[2.3] Agregando datos socioeconomicos por HD ...")

sum_cols = (['POP24_TOTAL', 'POV24_TOTAL', 'POP24_MALE', 'POP24_FEMALE',
             'POP24_JOVENES', 'POP24_ADULTOS_JOVENES', 'POP24_ADULTOS',
             'POP24_ADULTOS_MAYORES', 'AREA_SQMIL']
            + race_pop_cols + race_pov_cols + age_cols)

df_hd = df_la_lapd.groupby('HD_name')[sum_cols].sum().reset_index()

# Validar que no haya HDs con poblacion 0 (causaria division por cero)
hds_pop_cero = df_hd[df_hd['POP24_TOTAL'] == 0]['HD_name'].tolist()
if hds_pop_cero:
    print(f"  ADVERTENCIA: HDs con POP24_TOTAL=0 encontrados y eliminados: {hds_pop_cero}")
    df_hd = df_hd[df_hd['POP24_TOTAL'] > 0].copy()
else:
    print(f"  OK: ningun HD tiene poblacion cero.")

# FIX: calcular porcentajes con denominador seguro (no divide por cero)
def pct(num, den):
    return (num / den.replace(0, np.nan) * 100).round(2)

df_hd['tasa_pobreza_pct']   = pct(df_hd['POV24_TOTAL'], df_hd['POP24_TOTAL'])
df_hd['densidad_hab_sqmil'] = (df_hd['POP24_TOTAL'] / df_hd['AREA_SQMIL']).round(1)
df_hd['pct_white']          = pct(df_hd['POP24_WHITE'],    df_hd['POP24_TOTAL'])
df_hd['pct_black']          = pct(df_hd['POP24_BLACK'],    df_hd['POP24_TOTAL'])
df_hd['pct_hispanic']       = pct(df_hd['POP24_HISPANIC'], df_hd['POP24_TOTAL'])
df_hd['pct_asian']          = pct(df_hd['POP24_ASIAN'],    df_hd['POP24_TOTAL'])
df_hd['pct_aian']           = pct(df_hd['POP24_AIAN'],     df_hd['POP24_TOTAL'])
df_hd['pct_hnpi']           = pct(df_hd['POP24_HNPI'],     df_hd['POP24_TOTAL'])
df_hd['pct_jovenes']         = pct(df_hd['POP24_JOVENES'],         df_hd['POP24_TOTAL'])
df_hd['pct_adultos_jovenes'] = pct(df_hd['POP24_ADULTOS_JOVENES'], df_hd['POP24_TOTAL'])
df_hd['pct_adultos']         = pct(df_hd['POP24_ADULTOS'],         df_hd['POP24_TOTAL'])
df_hd['pct_adultos_mayores'] = pct(df_hd['POP24_ADULTOS_MAYORES'], df_hd['POP24_TOTAL'])

df_hd['areas_lapd']  = df_hd['HD_name'].map(lambda h: ', '.join(sorted(HD_LAPD_MAP.get(h, []))))
df_hd['n_areas_lapd']= df_hd['HD_name'].map(lambda h: len(HD_LAPD_MAP.get(h, [])))

print(f"  HDs validos con cobertura LAPD: {len(df_hd)}")
for _, row in df_hd[['HD_name','POP24_TOTAL','tasa_pobreza_pct','n_areas_lapd']].iterrows():
    print(f"    {row['HD_name']:<22} | Pob: {int(row['POP24_TOTAL']):>9,} "
          f"| Pobreza: {row['tasa_pobreza_pct']:>5.1f}% "
          f"| Areas LAPD: {int(row['n_areas_lapd'])}")

# ── 2.4 Agregar crimenes por HD y año ────────────────────────────────────────

print("\n[2.4] Agregando crimenes por HD ...")

df_mapeado = df_crime[df_crime['hd_name'].notna()].copy()

# FIX: separar anos completos vs. incompletos para calculos de tasa
df_completos  = df_mapeado[df_mapeado['anio'].isin(ANOS_COMPLETOS)]
n_anos_completos = len(ANOS_COMPLETOS)

# Crimenes en anos completos por HD (para tasa anualizada)
crimen_completos_hd = (df_completos
    .groupby('hd_name')
    .agg(delitos_anos_completos=('DR_NO', 'count'),
         delitos_part1_completos=('Part_1-2', lambda x: (x == 1).sum()),
         delitos_part2_completos=('Part_1-2', lambda x: (x == 2).sum()))
    .reset_index()
    .rename(columns={'hd_name': 'HD_name'}))

# Crimenes totales historicos por HD (todos los anos, para contexto)
crimen_total_hd = (df_mapeado
    .groupby('hd_name')
    .agg(total_delitos_historico=('DR_NO', 'count'))
    .reset_index()
    .rename(columns={'hd_name': 'HD_name'}))

# Crimenes por HD y ano (todos los anos, con flag de estado)
crimen_anual_hd = (df_mapeado
    .groupby(['hd_name', 'anio', 'estado_anio'])
    .agg(total_delitos=('DR_NO', 'count'),
         delitos_part1=('Part_1-2', lambda x: (x == 1).sum()),
         delitos_part2=('Part_1-2', lambda x: (x == 2).sum()))
    .reset_index()
    .rename(columns={'hd_name': 'HD_name'}))

# ── 2.5 Perfil de victima por HD ─────────────────────────────────────────────

print("\n[2.5] Calculando perfil de victima por HD ...")

DESCENT_MAP = {
    'A': 'Other Asian', 'B': 'Black', 'C': 'Chinese', 'D': 'Cambodian',
    'F': 'Filipino', 'G': 'Guamanian', 'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native', 'J': 'Japanese', 'K': 'Korean',
    'L': 'Laotian', 'O': 'Other', 'P': 'Pacific Islander', 'S': 'Samoan',
    'U': 'Hawaiian', 'V': 'Vietnamese', 'W': 'White', 'X': 'Unknown',
    'Z': 'Asian Indian'
}

df_mapeado['Vict_Sex'] = df_mapeado['Vict_Sex'].str.strip()
df_mapeado.loc[~df_mapeado['Vict_Sex'].isin(['F', 'M', 'X']), 'Vict_Sex'] = 'X'
df_mapeado['Vict_Descent_Desc'] = (df_mapeado['Vict_Descent']
                                    .str.strip()
                                    .map(DESCENT_MAP)
                                    .fillna('Unknown'))

# Solo anos completos para el perfil de victima (evita sesgo de anos parciales)
perfil_victima_hd = (df_mapeado[df_mapeado['anio'].isin(ANOS_COMPLETOS)]
    .groupby(['hd_name', 'Vict_Sex', 'Vict_Descent_Desc'])
    .agg(total_delitos=('DR_NO', 'count'))
    .reset_index()
    .rename(columns={'hd_name': 'HD_name'}))

# ── 2.6 Construir marts consolidados ─────────────────────────────────────────

print("\n[2.6] Construyendo marts consolidados ...")

socio_base = df_hd[['HD_name', 'POP24_TOTAL', 'tasa_pobreza_pct', 'densidad_hab_sqmil',
                     'AREA_SQMIL', 'areas_lapd', 'n_areas_lapd',
                     'pct_hispanic', 'pct_black', 'pct_white', 'pct_asian',
                     'pct_jovenes', 'pct_adultos_mayores']]

# --- mart_tasa_crimen: KPI central, tasas anualizadas sobre anos completos ---
mart_tasa = crimen_completos_hd.merge(socio_base, on='HD_name', how='inner')

mart_tasa['anos_base_calculo'] = n_anos_completos
mart_tasa['anos_base_str']     = f"{ANOS_COMPLETOS[0]}-{ANOS_COMPLETOS[-1]}"

# FIX: tasa anualizada = promedio anual de delitos / poblacion * 100k
mart_tasa['delitos_por_anio']         = (mart_tasa['delitos_anos_completos'] / n_anos_completos).round(0).astype(int)
mart_tasa['tasa_crimen_anual_100k']   = (mart_tasa['delitos_por_anio'] / mart_tasa['POP24_TOTAL'] * 100_000).round(1)
mart_tasa['delitos_graves_por_anio']  = (mart_tasa['delitos_part1_completos'] / n_anos_completos).round(0).astype(int)
mart_tasa['tasa_graves_anual_100k']   = (mart_tasa['delitos_graves_por_anio'] / mart_tasa['POP24_TOTAL'] * 100_000).round(1)
mart_tasa['delitos_menores_por_anio'] = (mart_tasa['delitos_part2_completos'] / n_anos_completos).round(0).astype(int)

mart_tasa = mart_tasa.sort_values('tasa_crimen_anual_100k', ascending=False).reset_index(drop=True)

# Ordenar columnas logicamente
mart_tasa = mart_tasa[[
    'HD_name', 'areas_lapd', 'n_areas_lapd',
    'anos_base_str', 'anos_base_calculo',
    'delitos_anos_completos', 'delitos_por_anio',
    'delitos_graves_por_anio', 'delitos_menores_por_anio',
    'POP24_TOTAL', 'tasa_pobreza_pct', 'densidad_hab_sqmil', 'AREA_SQMIL',
    'tasa_crimen_anual_100k', 'tasa_graves_anual_100k',
    'pct_hispanic', 'pct_black', 'pct_white', 'pct_asian',
    'pct_jovenes', 'pct_adultos_mayores',
]]

# --- mart_crimen_anual_hd: evolucion anual con contexto socioeconomico ---
mart_anual = crimen_anual_hd.merge(socio_base, on='HD_name', how='left')

# FIX: tasa anual real = delitos de ese año / poblacion
mart_anual['tasa_crimen_anual_100k'] = (
    mart_anual['total_delitos'] / mart_anual['POP24_TOTAL'] * 100_000
).round(1)

# Reordenar columnas con estado_anio al frente para que sea obvio en Power BI
mart_anual = mart_anual[[
    'HD_name', 'anio', 'estado_anio', 'areas_lapd',
    'total_delitos', 'delitos_part1', 'delitos_part2',
    'tasa_crimen_anual_100k',
    'POP24_TOTAL', 'tasa_pobreza_pct', 'densidad_hab_sqmil',
    'pct_hispanic', 'pct_black', 'pct_white', 'pct_asian',
    'pct_jovenes', 'pct_adultos_mayores',
]].sort_values(['HD_name', 'anio']).reset_index(drop=True)

# ─────────────────────────────────────────────────────────────────────────────
# PASO 3: CARGA
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 65)
print("PASO 3: CARGA")
print("=" * 65)

dim_socio_cols = [
    'HD_name', 'areas_lapd', 'n_areas_lapd',
    'POP24_TOTAL', 'POP24_MALE', 'POP24_FEMALE',
    'POV24_TOTAL', 'tasa_pobreza_pct', 'densidad_hab_sqmil', 'AREA_SQMIL',
    'POP24_WHITE', 'POP24_BLACK', 'POP24_HISPANIC', 'POP24_ASIAN', 'POP24_AIAN', 'POP24_HNPI',
    'pct_white', 'pct_black', 'pct_hispanic', 'pct_asian', 'pct_aian', 'pct_hnpi',
    'POV24_WHITE', 'POV24_BLACK', 'POV24_HISPANIC', 'POV24_ASIAN',
    'POP24_JOVENES', 'POP24_ADULTOS_JOVENES', 'POP24_ADULTOS', 'POP24_ADULTOS_MAYORES',
    'pct_jovenes', 'pct_adultos_jovenes', 'pct_adultos', 'pct_adultos_mayores',
] + age_cols

dim_socio = df_hd[[c for c in dim_socio_cols if c in df_hd.columns]].copy()

outputs = {
    'dim_socioeconomia.csv':      dim_socio,
    'mart_tasa_crimen.csv':       mart_tasa,
    'mart_crimen_anual_hd.csv':   mart_anual,
    'mart_perfil_victima_hd.csv': perfil_victima_hd,
}

for filename, df_out in outputs.items():
    path = os.path.join(PROC_DIR, filename)
    df_out.to_csv(path, index=False)
    size_kb = os.path.getsize(path) / 1024
    print(f"  {filename:<35} {len(df_out):>6,} filas | {size_kb:>7.1f} KB")

# Eliminar mart_crimen_hd.csv obsoleto si existe (era redundante con mart_tasa_crimen)
obsoleto = os.path.join(PROC_DIR, 'mart_crimen_hd.csv')
if os.path.exists(obsoleto):
    os.remove(obsoleto)
    print(f"  mart_crimen_hd.csv eliminado (era redundante con mart_tasa_crimen.csv)")

# ─────────────────────────────────────────────────────────────────────────────
# VALIDACION FINAL
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 65)
print("VALIDACION FINAL")
print("=" * 65)

print(f"\n[1] Tasa anualizada de crimen por 100k hab. — base {ANOS_COMPLETOS[0]}-{ANOS_COMPLETOS[-1]}")
print(f"  {'HD':<22} {'Poblacion':>10} {'Delitos/ano':>12} {'Pobreza%':>9} {'Tasa/100k':>10}")
print("  " + "-" * 68)
for _, r in mart_tasa.iterrows():
    print(f"  {r['HD_name']:<22} {int(r['POP24_TOTAL']):>10,} "
          f"{int(r['delitos_por_anio']):>12,} "
          f"{r['tasa_pobreza_pct']:>8.1f}% "
          f"{r['tasa_crimen_anual_100k']:>9.0f}")

print(f"\n[2] Verificacion de cobertura del mapeo LAPD -> HD")
total_mapeados = df_mapeado['DR_NO'].nunique()
total_global   = df_crime['DR_NO'].nunique()
print(f"  Delitos totales en el dataset: {total_global:,}")
print(f"  Delitos mapeados a algun HD:   {total_mapeados:,} ({total_mapeados/total_global*100:.1f}%)")
if total_mapeados < total_global:
    print(f"  ADVERTENCIA: {total_global - total_mapeados:,} delitos sin HD asignado")

print(f"\n[3] Distribucion por estado_anio en mart_crimen_anual_hd")
resumen_estado = mart_anual.groupby(['anio','estado_anio'])['total_delitos'].sum().reset_index()
for _, r in resumen_estado.iterrows():
    print(f"  {int(r['anio'])}: {int(r['total_delitos']):>9,} delitos  [{r['estado_anio'].upper()}]")

print(f"\n[4] Chequeo de valores nulos o infinitos en mart_tasa_crimen")
numericas = ['tasa_crimen_anual_100k', 'tasa_graves_anual_100k', 'tasa_pobreza_pct',
             'densidad_hab_sqmil', 'pct_hispanic', 'pct_black', 'pct_white']
ok = True
for col in numericas:
    n_null = mart_tasa[col].isna().sum()
    n_inf  = np.isinf(mart_tasa[col]).sum() if mart_tasa[col].dtype != object else 0
    if n_null > 0 or n_inf > 0:
        print(f"  PROBLEMA en {col}: {n_null} nulos, {n_inf} infinitos")
        ok = False
if ok:
    print(f"  OK: sin nulos ni infinitos en columnas numericas clave")

print(f"\n[5] Correlacion pobreza vs. tasa de crimen anualizada")
corr = mart_tasa[['tasa_pobreza_pct','tasa_crimen_anual_100k']].corr().iloc[0,1]
nivel = ('alta (r > 0.7)' if abs(corr) > 0.7
         else 'moderada (0.4-0.7)' if abs(corr) > 0.4
         else 'debil (< 0.4)')
print(f"  Pearson r = {corr:.3f}  -> correlacion {nivel}")

print(f"\n[6] HDs con mayor y menor tasa de crimen anualizada")
print(f"  Mayor: {mart_tasa.iloc[0]['HD_name']}  — "
      f"{mart_tasa.iloc[0]['tasa_crimen_anual_100k']:.0f}/100k/ano  "
      f"(pobreza: {mart_tasa.iloc[0]['tasa_pobreza_pct']:.1f}%)")
print(f"  Menor: {mart_tasa.iloc[-1]['HD_name']}  — "
      f"{mart_tasa.iloc[-1]['tasa_crimen_anual_100k']:.0f}/100k/ano  "
      f"(pobreza: {mart_tasa.iloc[-1]['tasa_pobreza_pct']:.1f}%)")

print("\n" + "=" * 65)
print("ETL SOCIOECONOMICO COMPLETADO SIN ERRORES")
print("=" * 65)
