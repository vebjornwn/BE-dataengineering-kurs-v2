# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
# MAGIC ## Konkurranse
# MAGIC
# MAGIC Vi bruker samme modell for alle.
# MAGIC
# MAGIC **Lavest WMAPE på testperioden vinner.**
# MAGIC
# MAGIC ## Datagrunnlag og tidspunkt
# MAGIC
# MAGIC Én rad i feature-tabellen er én uke × én art × ett fangstområde. Fangst- og
# MAGIC fartøydata kommer fra Fiskeridirektoratets åpne landingsdata, havdata fra
# MAGIC Open-Meteo Marine og sesongfelter fra kurskalenderen.
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

# MAGIC %md
# MAGIC ## Oppgave 4b – train/test
# MAGIC
# MAGIC Vi evaluerer modellen som en rullerende én-ukesprognose:
# MAGIC
# MAGIC - alle brukbare, hele target-uker i 2024 brukes til trening
# MAGIC - ukene med start 6., 13., 20. og 27. januar 2025 brukes til test
# MAGIC
# MAGIC For hver januaruke bruker modellen bare informasjon som var kjent ved
# MAGIC slutten av uken før. Januar-observasjonene brukes aldri til å trene modellen,
# MAGIC men tidligere januaruker kan være features i en senere rullerende prognose.
# MAGIC
# MAGIC Vi fjerner først rader der lag-features eller target mangler.

# COMMAND ----------

feature_cols = [
    "art_id",
    "fangstomrade_id",
    "ukenummer",
    "uke_sin",
    "uke_cos",
    "landet_kg",
    "landet_kg_lag_1",
    "landet_kg_lag_4",
    "gjennomsnitt_4_uker",
    "antall_landinger",
    "aktive_fartoy",
    "gjennomsnitt_fartoy_lengde",
    "gjennomsnitt_motorkraft_kw",
    "havtemperatur_c",
    "bolgehoyde_m",
    "stromhastighet_ms",
]

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

train_df = model_df[
    (model_df["target_aar"] == 2024) &
    (model_df["target_uke_start"] <= pd.Timestamp("2024-12-23"))
].copy()
test_df = model_df[
    (model_df["target_aar"] == 2025) &
    (model_df["target_maaned"] == 1)
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

model = RandomForestRegressor(
    n_estimators=200,
    min_samples_leaf=2,
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
# MAGIC
# MAGIC For konkurransen bruker vi WMAPE.

# COMMAND ----------

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
wmape = np.abs(y_test.to_numpy() - y_pred).sum() / np.abs(y_test.to_numpy()).sum() * 100

print(f"MAE:   {mae:,.0f} kg")
print(f"RMSE:  {rmse:,.0f} kg")
print(f"WMAPE: {wmape:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Konkurranseresultat:** noter WMAPE-en din.
# MAGIC
# MAGIC Lavest prosent i rommet vinner.

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

spark.createDataFrame(prediksjoner).write.mode("overwrite").saveAsTable(
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
