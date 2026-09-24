# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Oppgave 5 – Dashboard
# MAGIC
# MAGIC Dashboardet er allerede definert som kode og deployes med **Declarative Automation Bundles (DAB)**.
# MAGIC
# MAGIC Det betyr at vi ikke skal bruke tiden på å bygge et dashboard fra bunnen av.
# MAGIC
# MAGIC I stedet skal du:
# MAGIC
# MAGIC 1. åpne det ferdige dashboardet
# MAGIC 2. forstå hvilke Gold-tabeller det bruker
# MAGIC 3. endre eller legge til én visualisering
# MAGIC 4. deploye endringen
# MAGIC
# MAGIC Dette er også et eksempel på hvordan BI-artefakter kan versjoneres og deployes på samme måte som annen kode.

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]

print(f"Gold-katalog: {catalog}")
print(f"- {catalog}.gold.mart_fangst_uke")
print(f"- {catalog}.gold.prediksjoner")

# COMMAND ----------

# DBTITLE 1,Hva finnes allerede i dashboardet?
# MAGIC %md
# MAGIC ## Hva finnes allerede i dashboardet?
# MAGIC
# MAGIC Det ferdige dashboardet har:
# MAGIC
# MAGIC - KPI: total landingsmengde
# MAGIC - KPI: maks aktive fartøy per uke
# MAGIC - landingsmengde per uke, splittet på art
# MAGIC - landingsmengde per art
# MAGIC - landingsmengde per fangstområde
# MAGIC - faktisk vs. predikert landingsmengde for desember 2024
# MAGIC - filter på art
# MAGIC - filter på fangstområde
# MAGIC
# MAGIC Historikkgrafene dekker 2024. Prognosegrafen bruker
# MAGIC `target_uke_start`, altså datoen for uken som faktisk predikeres.
# MAGIC
# MAGIC Dashboardet bruker:
# MAGIC
# MAGIC ```text
# MAGIC gold.mart_fangst_uke
# MAGIC gold.prediksjoner
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Åpne dashboardet
# MAGIC
# MAGIC Gå tilbake til bundle-visningen:
# MAGIC
# MAGIC 1. Åpne **Deployments**
# MAGIC 2. Under **Bundle resources**, åpne **Fiskeri – fangst og prognose**
# MAGIC 3. Velg **Edit draft**
# MAGIC
# MAGIC Siden vi bruker `dev`-targetet kan dashboardet redigeres direkte i Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Din oppgave
# MAGIC
# MAGIC Gjør **én** forbedring.
# MAGIC
# MAGIC Eksempler:
# MAGIC
# MAGIC - legg til gjennomsnittlig bølgehøyde
# MAGIC - lag en graf for havtemperatur gjennom året
# MAGIC - endre landingsgrafen til et annet diagram
# MAGIC - lag en KPI for antall landinger
# MAGIC - endre sortering, titler eller layout
# MAGIC
# MAGIC Når du er fornøyd:
# MAGIC
# MAGIC 1. velg **Deploy**
# MAGIC 2. åpne **View deployed**
# MAGIC 3. kontroller at endringen vises
# MAGIC
# MAGIC Dashboardet er en del av bundle-deploymenten, så endringen deployes på samme måte som andre Databricks-resurser.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ferdig!
# MAGIC
# MAGIC Du har nå bygget hele flyten:
# MAGIC
# MAGIC ```text
# MAGIC fire råkilder
# MAGIC      ↓
# MAGIC Bronze
# MAGIC VARIANT + metadata
# MAGIC      ↓
# MAGIC Silver
# MAGIC typing + vask + standardisering
# MAGIC      ↓
# MAGIC Gold
# MAGIC fact + dimensions + marts
# MAGIC      ↓
# MAGIC ┌──────────────────┬──────────────────┐
# MAGIC │ ML-prognose      │ AI/BI-dashboard  │
# MAGIC └──────────────────┴──────────────────┘
# MAGIC ```