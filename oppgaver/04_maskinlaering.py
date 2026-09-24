# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Oppgave 4 – Maskinlæring
# MAGIC %md
# MAGIC # Oppgave 4 – Maskinlæring
# MAGIC
# MAGIC Nå skal vi bruke Gold-dataproduktet til å predikere:
# MAGIC
# MAGIC > **Hvor mange kilo som landes neste uke per art og fangstområde**
# MAGIC
# MAGIC Hovedpoenget er ikke å lære avansert maskinlæring.
# MAGIC
# MAGIC Modellen er med vilje enkel. Målet er å se at et godt datagrunnlag kan brukes direkte av et data science-team.
# MAGIC
# MAGIC Modellen evalueres på holdout-perioden i desember 2024 med MAE, RMSE og WMAPE.
# MAGIC
# MAGIC ## Datagrunnlag og tidspunkt
# MAGIC
# MAGIC Én rad i feature-tabellen er én uke × én art × ett fangstområde. Fangst- og
# MAGIC fartøydata kommer fra Fiskeridirektoratets åpne landingsdata, modellbaserte
# MAGIC havdata fra Open-Meteo Marine og sesongfelter fra kurskalenderen.
# MAGIC
# MAGIC Fartøyfeatures beskriver den aktive flåten i uke *t*: antall fartøy,
# MAGIC gjennomsnittlig lengde og motorkraft.
# MAGIC
# MAGIC Modellen predikerer fangst i uke *t+1*. Den får aldri se fartøyaktivitet eller
# MAGIC flåteegenskaper fra uken den skal predikere. Dette er viktig for å unngå datalekkasje.

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]

spark_df = spark.read.table(
    f"{catalog}.gold.mart_fangstprognose"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 4a – Spark til Pandas
# MAGIC
# MAGIC Scikit-learn jobber naturlig med Pandas.
# MAGIC
# MAGIC Konverter Gold-tabellen til en Pandas DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC ```python
# MAGIC df = spark_df.toPandas()
# MAGIC ```

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

df = spark_df.toPandas()
df["target_uke_start"] = pd.to_datetime(df["target_uke_start"])

print("Antall rader:", len(df))
print("Kolonner:", list(df.columns))

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %md
# MAGIC ## Oppgave 4b – train/test
# MAGIC
# MAGIC Vi evaluerer modellen som en rullerende én-ukesprognose:
# MAGIC
# MAGIC - alle brukbare, hele target-uker fra januar til november 2024 brukes til trening
# MAGIC - de fire siste ukene i desember 2024 brukes til test
# MAGIC
# MAGIC For hver desemberuke bruker modellen bare informasjon som var kjent ved
# MAGIC slutten av uken før. Desember-observasjonene brukes aldri til å trene modellen,
# MAGIC men tidligere desemberuker kan være features i en senere rullerende prognose.
# MAGIC
# MAGIC Vi fjerner først rader der lag-features eller target mangler.

# COMMAND ----------

# DBTITLE 1,Cell 9
# Legg til måned som kategoriske features (one-hot encoding)
for month in range(1, 13):
    df[f'maaned_{month}'] = (df['maaned'] == month).astype(int)

# Legg til art som kategoriske features
# Viktig: Hver art har unike sesongmønstre og volumnivåer
for art_id in df['art_id'].unique():
    df[f'art_{int(art_id)}'] = (df['art_id'] == art_id).astype(int)

# Trend-features: fanger opp utvikling per art over tid
# 8-ukers lag gir lengre historikk (korrelasjon +0.233 med target)
# 8-ukers rullerende snitt jevner ut støy
df = df.sort_values(['art_navn', 'fangstomrade_navn', 'uke_start']).reset_index(drop=True)

def _add_trend(group):
    group = group.sort_values('uke_start').copy()
    group['landet_kg_lag_8'] = group['landet_kg'].shift(8)
    group['gjennomsnitt_8_uker'] = group['landet_kg'].rolling(8, min_periods=1).mean()
    # Fyll manglende lag_8 med 8-ukers snitt for å unngå datatap
    group['landet_kg_lag_8'] = group['landet_kg_lag_8'].fillna(group['gjennomsnitt_8_uker'])
    return group

df = df.groupby(['art_navn', 'fangstomrade_navn'], group_keys=False).apply(_add_trend)

feature_cols = [
    "fangstomrade_id",
    "ukenummer",
    "uke_sin",
    "uke_cos",
    "landet_kg",
    "landet_kg_lag_1",
    "landet_kg_lag_4",
    "gjennomsnitt_4_uker",
    "landet_kg_lag_8",
    "gjennomsnitt_8_uker",
    "antall_landinger",
    "aktive_fartoy",
    "gjennomsnitt_fartoy_lengde",
    "gjennomsnitt_motorkraft_kw",
    "havtemperatur_c",
    "bolgehoyde_m",
    "stromhastighet_ms",
] + [f'maaned_{m}' for m in range(1, 13)] + [f'art_{int(a)}' for a in df['art_id'].unique()]

target_col = "landet_kg_neste_uke"

model_df = (
    df
        .dropna(subset=feature_cols + [target_col])
        .copy()
)

# COMMAND ----------

# Skriv train/test-splitten din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

# DBTITLE 1,Cell 12
train_df = model_df[
    (model_df["target_aar"] == 2024) &
    (model_df["target_uke_start"] <= pd.Timestamp("2024-11-25"))
].copy()
test_df = model_df[
    (model_df["target_aar"] == 2024) &
    (model_df["target_maaned"] == 12)
].copy()

X_train = train_df[feature_cols]
y_train = train_df[target_col]

X_test = test_df[feature_cols]
y_test = test_df[target_col]

print("Train:", X_train.shape)
print("Test:", X_test.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 4c – tren modellen
# MAGIC
# MAGIC Bruk en `RandomForestRegressor`.
# MAGIC
# MAGIC Modellen skal normalt trene på få sekunder.

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

# DBTITLE 1,Cell 16
# Mer regularisering for å unngå overfitting med kategoriske features
model = RandomForestRegressor(
    n_estimators=250,
    max_depth=12,
    min_samples_split=10,
    min_samples_leaf=4,
    max_features=0.5,
    random_state=42,
    n_jobs=-1,
)

model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluer modellen
# MAGIC
# MAGIC - **MAE**: gjennomsnittlig absolutt feil i kg
# MAGIC - **RMSE**: straffer store feil mer
# MAGIC - **WMAPE**: absolutt feil delt på faktisk totalvolum

# COMMAND ----------

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
wmape = np.abs(y_test.to_numpy() - y_pred).sum() / np.abs(y_test.to_numpy()).sum() * 100

print(f"MAE:   {mae:,.0f} kg")
print(f"RMSE:  {rmse:,.0f} kg")
print(f"WMAPE: {wmape:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC Sammen vurderer metrikker i kilo og prosent både den typiske feilen,
# MAGIC store avvik og total feil relativt til faktisk volum.

# COMMAND ----------

prediksjoner = test_df[
    [
        "uke_start",
        "target_uke_start",
        "art_kode",
        "art_navn",
        "fangstomrade_kode",
        "fangstomrade_navn",
        target_col,
    ]
].copy()

prediksjoner["predikert_kg_neste_uke"] = y_pred
prediksjoner["absolutt_feil_kg"] = np.abs(
    prediksjoner[target_col] -
    prediksjoner["predikert_kg_neste_uke"]
)

spark.createDataFrame(prediksjoner).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.gold.prediksjoner"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Faktisk vs. predikert

# COMMAND ----------

plot_df = (
    prediksjoner
        .groupby("target_uke_start", as_index=False)[
            [target_col, "predikert_kg_neste_uke"]
        ]
        .sum()
)

plt.figure(figsize=(10, 5))
plt.plot(plot_df["target_uke_start"], plot_df[target_col], marker="o", label="Faktisk")
plt.plot(plot_df["target_uke_start"], plot_df["predikert_kg_neste_uke"], marker="o", label="Predikert")
plt.xlabel("Uken som predikeres")
plt.ylabel("Kg")
plt.title("Faktisk vs. predikert landingsmengde")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus – hvilke features bruker modellen?

# COMMAND ----------

feature_importance = (
    pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    })
    .sort_values("importance", ascending=False)
)

display(feature_importance)

# COMMAND ----------

# DBTITLE 1,Analyse per art
# MAGIC %md
# MAGIC ## Analyse per art
# MAGIC
# MAGIC La oss se hvordan modellen presterer for hver art.

# COMMAND ----------

# DBTITLE 1,Evaluering per art
# Beregn metrics per art
art_metrics = []

for art_navn in test_df['art_navn'].unique():
    art_mask = test_df['art_navn'] == art_navn
    y_test_art = y_test[art_mask]
    y_pred_art = y_pred[art_mask]
    
    mae_art = mean_absolute_error(y_test_art, y_pred_art)
    wmape_art = np.abs(y_test_art.to_numpy() - y_pred_art).sum() / np.abs(y_test_art.to_numpy()).sum() * 100
    
    total_faktisk = y_test_art.sum()
    total_predikert = y_pred_art.sum()
    
    art_metrics.append({
        'art': art_navn,
        'antall_obs': len(y_test_art),
        'total_faktisk_kg': total_faktisk,
        'total_predikert_kg': total_predikert,
        'avvik_kg': total_predikert - total_faktisk,
        'avvik_prosent': (total_predikert - total_faktisk) / total_faktisk * 100,
        'mae': mae_art,
        'wmape': wmape_art
    })

art_df = pd.DataFrame(art_metrics).sort_values('wmape', ascending=False)

print("=== Evaluering per art (sortert etter WMAPE) ===")
print()
for _, row in art_df.iterrows():
    print(f"{row['art']}:")
    print(f"  MAE: {row['mae']:,.0f} kg")
    print(f"  WMAPE: {row['wmape']:.1f}%")
    print(f"  Faktisk: {row['total_faktisk_kg']:,.0f} kg")
    print(f"  Predikert: {row['total_predikert_kg']:,.0f} kg")
    print(f"  Avvik: {row['avvik_kg']:,.0f} kg ({row['avvik_prosent']:+.1f}%)")
    print()

display(art_df.round(1))

# COMMAND ----------

# DBTITLE 1,Visualiser avvik per art
# Visualiser avvik per art
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Plot 1: WMAPE per art
axes[0].barh(art_df['art'], art_df['wmape'], color=['#d62728' if x > 100 else '#2ca02c' for x in art_df['wmape']])
axes[0].axvline(100, color='black', linestyle='--', linewidth=1, alpha=0.5)
axes[0].set_xlabel('WMAPE (%)')
axes[0].set_title('WMAPE per art (lavere er bedre)')
axes[0].invert_yaxis()

# Plot 2: Faktisk vs Predikert
art_df_sorted = art_df.sort_values('total_faktisk_kg')
x = np.arange(len(art_df_sorted))
width = 0.35

axes[1].bar(x - width/2, art_df_sorted['total_faktisk_kg']/1000, width, label='Faktisk', color='#1f77b4')
axes[1].bar(x + width/2, art_df_sorted['total_predikert_kg']/1000, width, label='Predikert', color='#ff7f0e')
axes[1].set_ylabel('Tonn')
axes[1].set_title('Total fangst i testperioden')
axes[1].set_xticks(x)
axes[1].set_xticklabels(art_df_sorted['art'])
axes[1].legend()

plt.tight_layout()
plt.show()