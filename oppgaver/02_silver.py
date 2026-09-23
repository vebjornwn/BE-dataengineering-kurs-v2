# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Oppgave 2 – Silver
# MAGIC
# MAGIC I Bronze beholdt vi dataene kildenært som `VARIANT` for å sikre at de opprinnelige dataene ble lagret uendret.
# MAGIC
# MAGIC #### Silver-laget
# MAGIC
# MAGIC I Silver-laget transformerer vi rådataene til et strukturert og konsistent format som er enklere å analysere og bygge videre på. Målet er å forbedre datakvaliteten samtidig som vi bevarer den forretningsmessige betydningen i dataene.
# MAGIC
# MAGIC Typiske oppgaver i Silver-laget er:
# MAGIC
# MAGIC - hente felter ut av `payload`
# MAGIC - bruke snake_case på kolonnenavn
# MAGIC - sette riktige datatyper
# MAGIC - standardisere koder og verdier
# MAGIC - fjerne duplikater og ugyldige rader
# MAGIC
# MAGIC Resultatet er datasett med kjent struktur og kvalitet som kan brukes som grunnlag for videre analyser, rapportering og maskinlæring.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 2a – parse landinger
# MAGIC
# MAGIC Lag en flat DataFrame fra `bronze.landinger_raw` med disse kolonnene:
# MAGIC
# MAGIC ```text
# MAGIC seddelnummer          STRING
# MAGIC landingsdato         DATE
# MAGIC fartoy_id            STRING
# MAGIC art_kode             STRING
# MAGIC art_navn             STRING
# MAGIC fangstomrade_kode    STRING
# MAGIC fangstomrade_navn    STRING
# MAGIC redskap_kode         STRING
# MAGIC rundvekt_kg          DOUBLE
# MAGIC ```
# MAGIC
# MAGIC Bruk `try_variant_get()`. Den returnerer NULL dersom verdien ikke finnes eller ikke kan castes.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC For å se hvilke felt som finnes i `landinger_raw`-strukturen, kan du bruke funksjonen `schema_of_variant_agg()` som samler skjemaet for alle rader:
# MAGIC
# MAGIC ```python
# MAGIC spark.sql(f"""
# MAGIC   SELECT schema_of_variant_agg(payload) AS skjema
# MAGIC   FROM {catalog}.bronze.landinger_raw
# MAGIC """).display()
# MAGIC ```
# MAGIC
# MAGIC Du kan også bare inspisere én rad for å se feltene direkte:
# MAGIC
# MAGIC ```python
# MAGIC display(spark.read.table(f"{catalog}.bronze.landinger_raw").limit(1))
# MAGIC ```
# MAGIC
# MAGIC I Bronze-laget er den opprinnelige raden lagret som et `VARIANT`-objekt i kolonnen `payload`. For å hente ut verdier fra et `VARIANT`-objekt kan du bruke:
# MAGIC
# MAGIC ```python
# MAGIC F.try_variant_get("payload", '$["Fartøy ID"]', "string")
# MAGIC ```
# MAGIC
# MAGIC Her betyr:
# MAGIC
# MAGIC - `payload` = kolonnen som inneholder VARIANT-data
# MAGIC - `'$["Fartøy ID"]'` = feltet i JSON-strukturen (bruk `$.felt` for enkle navn, `'$["Felt Navn"]'` for navn med mellomrom)
# MAGIC - `"string"` = ønsket datatype på resultatet
# MAGIC
# MAGIC Funksjonen returnerer `NULL` dersom feltet ikke finnes eller ikke kan konverteres til oppgitt datatype.
# MAGIC
# MAGIC **Ekstra tips:**
# MAGIC
# MAGIC - Bruk `.alias("navn")` for å gi kolonnene snake_case-navn
# MAGIC - For `landingsdato` må du konvertere fra tekst til dato med `F.to_date(..., "dd.MM.yyyy")` – kildedatoene er på formatet `15.03.2024`

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

landinger_raw = spark.read.table(f"{catalog}.bronze.landinger_raw")

landinger_parsed = landinger_raw.select(
    F.try_variant_get("payload", '$["Seddelnummer"]', "string").alias("seddelnummer"),
    F.to_date(
        F.try_variant_get("payload", '$["Landingsdato"]', "string"),
        "dd.MM.yyyy",
    ).alias("landingsdato"),
    F.try_variant_get("payload", '$["Fartøy ID"]', "string").alias("fartoy_id"),
    F.try_variant_get("payload", '$["Art FAO (kode)"]', "string").alias("art_kode"),
    F.try_variant_get("payload", '$["Art FAO (navn)"]', "string").alias("art_navn"),
    F.try_variant_get("payload", '$["Fangstområde (kode)"]', "string").alias("fangstomrade_kode"),
    F.try_variant_get("payload", '$["Fangstområde (navn)"]', "string").alias("fangstomrade_navn"),
    F.try_variant_get("payload", '$["Redskap (kode)"]', "string").alias("redskap_kode"),
    F.try_variant_get("payload", '$["Rundvekt (kg)"]', "double").alias("rundvekt_kg"),
    "_ingest_tidspunkt",
)

display(landinger_parsed.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 2b – datakvalitet
# MAGIC
# MAGIC I kursdataene finnes noen bevisste problemer:
# MAGIC
# MAGIC - enkelte sedler er duplisert
# MAGIC - noen vekter mangler
# MAGIC - noen fangstområdekoder mangler ledende null
# MAGIC
# MAGIC Rydd dataene slik at:
# MAGIC
# MAGIC 1. `seddelnummer` er unikt
# MAGIC 2. `rundvekt_kg > 0`
# MAGIC 3. `fangstomrade_kode` alltid har to tegn
# MAGIC 4. rader uten dato, fartøy, art eller fangstområde fjernes

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC 1. unike sedler → `dropDuplicates(["seddelnummer"])`
# MAGIC 2. positive vekter → `filter(F.col("rundvekt_kg") > 0)`
# MAGIC 3. to tegn → `F.lpad("fangstomrade_kode", 2, "0")` med `.withColumn`
# MAGIC 4. fjern ufullstendige → `dropna(subset=[...])`
# MAGIC
# MAGIC Kjør `lpad` før `dropna`.

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

landinger = (
    landinger_parsed
        .dropDuplicates(["seddelnummer"])
        .withColumn("fangstomrade_kode", F.lpad("fangstomrade_kode", 2, "0"))
        .filter(F.col("rundvekt_kg") > 0)
        .dropna(subset=[
            "landingsdato",
            "fartoy_id",
            "art_kode",
            "fangstomrade_kode",
        ])
)

landinger.write.mode("overwrite").saveAsTable(
    f"{catalog}.silver.landinger"
)

print("Bronze-rader:", landinger_raw.count())
print("Silver-rader:", landinger.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 2c – de andre kildene
# MAGIC
# MAGIC Vi trenger også rene tabeller for fartøy, havforhold og kalender.
# MAGIC
# MAGIC Under får du den ferdige koden. Studer hvordan samme `VARIANT`-mønster brukes på tvers av forskjellige kilder.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fartøy

# COMMAND ----------

fartoy_raw = spark.read.table(f"{catalog}.bronze.fartoy_raw")

fartoy_parsed = fartoy_raw.select(
    F.try_variant_get("payload", '$["Fartøy ID"]', "string").alias("fartoy_id"),
    F.try_variant_get("payload", '$["Fartøygruppe"]', "string").alias("fartoygruppe"),
    F.try_variant_get("payload", '$["Lengde (meter)"]', "double").alias("lengde_meter"),
    F.try_variant_get("payload", '$["Bredde (meter)"]', "double").alias("bredde_meter"),
    F.try_variant_get("payload", '$["Byggeår"]', "int").alias("byggear"),
    F.try_variant_get("payload", '$["Motorkraft (kW)"]', "double").alias("motorkraft_kw"),
    F.try_variant_get("payload", '$["Hjemkommune"]', "string").alias("hjemkommune"),
)

# Behold én rad per fartøy.
# Dersom et fartøy finnes flere ganger velger vi raden med høyest byggeår.
w = Window.partitionBy("fartoy_id").orderBy(F.col("byggear").desc_nulls_last())

fartoy = (
    fartoy_parsed
        .withColumn("_radnummer", F.row_number().over(w))
        .filter(F.col("_radnummer") == 1)
        .drop("_radnummer")
        .dropna(subset=["fartoy_id"])
)

fartoy.write.mode("overwrite").saveAsTable(
    f"{catalog}.silver.fartoy"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Havforhold

# COMMAND ----------

havforhold_raw = spark.read.table(f"{catalog}.bronze.havforhold_raw")

havforhold = (
    havforhold_raw
        .select(
            F.to_date(F.try_variant_get("payload", "$.dato", "string")).alias("dato"),
            F.lpad(
                F.try_variant_get("payload", "$.fangstomrade_kode", "string"),
                2,
                "0",
            ).alias("fangstomrade_kode"),
            F.try_variant_get("payload", "$.fangstomrade_navn", "string").alias("fangstomrade_navn"),
            F.try_variant_get("payload", "$.havtemperatur_c", "double").alias("havtemperatur_c"),
            F.try_variant_get("payload", "$.bolgehoyde_m", "double").alias("bolgehoyde_m"),
            F.try_variant_get("payload", "$.stromhastighet_ms", "double").alias("stromhastighet_ms"),
        )
        .dropDuplicates(["dato", "fangstomrade_kode"])
        .dropna(subset=["dato", "fangstomrade_kode"])
)

havforhold.write.mode("overwrite").saveAsTable(
    f"{catalog}.silver.havforhold"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kalender

# COMMAND ----------

kalender_raw = spark.read.table(f"{catalog}.bronze.kalender_raw")

kalender = (
    kalender_raw
        .select(
            F.to_date(F.try_variant_get("payload", "$.dato", "string")).alias("dato"),
            F.to_date(F.try_variant_get("payload", "$.uke_start", "string")).alias("uke_start"),
            F.try_variant_get("payload", "$.aar", "int").alias("aar"),
            F.try_variant_get("payload", "$.maaned", "int").alias("maaned"),
            F.try_variant_get("payload", "$.ukenummer", "int").alias("ukenummer"),
            F.try_variant_get("payload", "$.ukedag", "int").alias("ukedag"),
            F.try_variant_get("payload", "$.er_helg", "boolean").alias("er_helg"),
            F.try_variant_get("payload", "$.er_helligdag", "boolean").alias("er_helligdag"),
            F.try_variant_get("payload", "$.sesong", "string").alias("sesong"),
        )
        .dropDuplicates(["dato"])
)

kalender.write.mode("overwrite").saveAsTable(
    f"{catalog}.silver.kalender"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kontroller Silver
# MAGIC
# MAGIC Silver-tabellene skal nå ha tydelige navn og datatyper og kunne brukes sammen.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN `{catalog}`.`silver`"))
display(spark.read.table(f"{catalog}.silver.landinger").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Godt jobbet! Neste steg er **Gold**, hvor vi lager fakta-/dimensjonstabeller og et ukentlig dataprodukt for prognosen.