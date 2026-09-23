# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Oppgave 1 – Bronze
# MAGIC
# MAGIC ## Case
# MAGIC
# MAGIC I dette kurset skal vi etter hvert predikere hvor mye **torsk, sei og hyse** som landes neste uke i ulike norske fangstområder.
# MAGIC
# MAGIC Før vi kan analysere eller predikere noe, må vi bygge et pålitelig datagrunnlag.
# MAGIC
# MAGIC ## Dataene i kurset
# MAGIC
# MAGIC Kurset bruker et lite, frosset uttrekk av faktiske åpne data. Filene ligger i
# MAGIC repoet slik at alle deltakere får identisk input og kurset ikke er avhengig av
# MAGIC eksterne tjenester mens det pågår.
# MAGIC
# MAGIC **Fangst og fartøy – Fiskeridirektoratet**
# MAGIC [Åpne fangstdata (seddel) koblet med fartøydata](https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata)
# MAGIC
# MAGIC - Én kilderad er en varelinje på et landingsdokument.
# MAGIC - Uttrekket dekker torsk, hyse og sei i seks fangstområder.
# MAGIC - 2024 brukes til trening. Fire hele uker i januar 2025 brukes som holdout.
# MAGIC - Dataene inneholder faktisk kobling mellom landing og fartøy via `Fartøy ID`.
# MAGIC - Fartøydata omfatter blant annet type, lengde, bredde, bruttotonnasje,
# MAGIC   byggeår, motorkraft, hjemkommune og nasjonalitet.
# MAGIC
# MAGIC For å holde repoet lite tar byggejobben inntil åtte gyldige varelinjer per
# MAGIC uke × art × område. Den beholder også enkelte ekte kvalitetsproblemrader som
# MAGIC deltakerne skal håndtere i Silver.
# MAGIC
# MAGIC **Havforhold – Open-Meteo Marine**
# MAGIC [Marine Weather API](https://open-meteo.com/en/docs/marine-weather-api)
# MAGIC
# MAGIC Daglige modellverdier for havtemperatur, bølgehøyde og strømhastighet hentes
# MAGIC for ett representativt punkt per fangstområde. De brukes som analysefeatures,
# MAGIC ikke til navigasjon. Kalenderdata genereres lokalt.
# MAGIC
# MAGIC #### Medallion-arkitektur
# MAGIC Gjennom kurset følger vi medallion-arkitekturen:
# MAGIC
# MAGIC ```text
# MAGIC Landing -> Bronze -> Silver -> Gold -> ML / Dashboard
# MAGIC ```
# MAGIC
# MAGIC I en medallion-arkitektur flyter data gjennom flere lag der de gradvis forbedres, struktureres og kvalitetssikres. Dette gjør det enklere å skille mellom rådata, bearbeidede data og data som er klare for analyse.
# MAGIC
# MAGIC Dersom du ønsker å lese mer om arkitekturen, kan du se Databricks sin forklaring: https://docs.databricks.com/aws/en/lakehouse/medallion
# MAGIC
# MAGIC #### Bronze-laget
# MAGIC Bronze-laget inneholder rådata som ligger så nært kilden som mulig. Hensikten er å bevare en kopi av de opprinnelige dataene slik at vi alltid kan spore hva som ble mottatt fra kildesystemet og eventuelt bygge senere steg på nytt.
# MAGIC
# MAGIC I Bronze lagrer vi selve kilderaden som `VARIANT`. `VARIANT` er en datatype for semi-strukturerte data, for eksempel `JSON`, som gjør at vi kan lagre data med fleksibel struktur uten å definere alle kolonner på forhånd. Vi lagrer også teknisk metadata som tidspunkt for innlasting og informasjon om hvor dataene kom fra.
# MAGIC
# MAGIC Vi gjør **ikke** snake_case, datatypekonvertering, deduplisering eller businesslogikk før Silver.

# COMMAND ----------

## Kjør denne cellen for å importere biblioteker og initialisere riktige variabler

from pyspark.sql import functions as F

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
volume_path = f"/Volumes/{catalog}/bronze/kildedata"

print(f"Katalog: {catalog}")
print(f"Landing: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hjelpefunksjon: CSV-rad til VARIANT
# MAGIC
# MAGIC CSV-leseren må først forstå skilletegn og header.
# MAGIC Deretter pakkes alle kildefeltene tilbake inn i ett JSON-objekt og lagres som `VARIANT`.
# MAGIC På den måten kan Bronze bevare navn som:
# MAGIC
# MAGIC - `Fartøy ID`
# MAGIC - `Art FAO (kode)`
# MAGIC - `Rundvekt (kg)`
# MAGIC
# MAGIC uten at Delta-tabellen trenger disse navnene som egne kolonner.

# COMMAND ----------

def csv_til_bronze_variant(path: str, delimiter: str = ","):
    kilde = (
        spark.read
            .option("header", True)
            .option("delimiter", delimiter)
            .csv(path)
    )

    kildekolonner = [
        F.col(f"`{kolonne.replace('`', '``')}`")
        for kolonne in kilde.columns
    ]

    return (
        kilde
            .select(
                F.to_json(F.struct(*kildekolonner)).alias("_payload_json"),
                F.current_timestamp().alias("_ingest_tidspunkt"),
                F.col("_metadata.file_path").alias("_kildefil"),
            )
            .select(
                F.expr("parse_json(_payload_json)").alias("payload"),
                "_ingest_tidspunkt",
                "_kildefil",
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 1a – landingsdata
# MAGIC
# MAGIC Les `landinger_kurs.csv` og skriv dataene til:
# MAGIC
# MAGIC ```text
# MAGIC <catalog>.bronze.landinger_raw
# MAGIC ```
# MAGIC
# MAGIC Tabellen skal kun ha:
# MAGIC
# MAGIC - `payload VARIANT`
# MAGIC - `_ingest_tidspunkt`
# MAGIC - `_kildefil`
# MAGIC
# MAGIC Filen bruker semikolon som skilletegn.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC Bruk parameterverdien; `delimiter=";"`, i hjelpefunksjonen over.

# COMMAND ----------

# Skriv løsningen din her.


# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

landinger_raw = csv_til_bronze_variant(
    f"{volume_path}/landinger_kurs.csv",
    delimiter=";",
)

landinger_raw.write.mode("overwrite").saveAsTable(
    f"{catalog}.bronze.landinger_raw"
)

display(landinger_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 1b – fartøy og kalender
# MAGIC
# MAGIC Gjør det samme for:
# MAGIC
# MAGIC - `fartoy_kurs.csv -> bronze.fartoy_raw`
# MAGIC - `kalender_kurs.csv -> bronze.kalender_raw`
# MAGIC
# MAGIC Begge er kommaseparerte CSV-filer.

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

for filename, table in [
    ("fartoy_kurs.csv", "fartoy_raw"),
    ("kalender_kurs.csv", "kalender_raw"),
]:
    df = csv_til_bronze_variant(f"{volume_path}/{filename}")

    df.write.mode("overwrite").saveAsTable(
        f"{catalog}.bronze.{table}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 1c – havforhold
# MAGIC
# MAGIC Havforhold kommer allerede som JSON Lines.
# MAGIC Her trenger vi derfor ikke først å konvertere CSV til JSON.
# MAGIC
# MAGIC Les hver linje og bruk `parse_json()` slik at også denne Bronze-tabellen får:
# MAGIC
# MAGIC - `payload VARIANT`
# MAGIC - `_ingest_tidspunkt`
# MAGIC - `_kildefil`

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC ```python
# MAGIC spark.read.text(...)
# MAGIC ```
# MAGIC
# MAGIC og deretter `parse_json(value)`.

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

havforhold_raw = (
    spark.read.text(f"{volume_path}/havforhold_kurs.jsonl")
        .select(
            F.expr("parse_json(value)").alias("payload"),
            F.current_timestamp().alias("_ingest_tidspunkt"),
            F.col("_metadata.file_path").alias("_kildefil"),
        )
)

havforhold_raw.write.mode("overwrite").saveAsTable(
    f"{catalog}.bronze.havforhold_raw"
)

display(havforhold_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kontroller Bronze
# MAGIC
# MAGIC Alle fire Bronze-tabellene skal nå ha samme tekniske mønster:
# MAGIC
# MAGIC ```text
# MAGIC payload
# MAGIC _ingest_tidspunkt
# MAGIC _kildefil
# MAGIC ```
# MAGIC
# MAGIC Originale feltnavn og verdier ligger urørt inni `payload`.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN `{catalog}`.`bronze`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Neste steg: Silver
# MAGIC
# MAGIC I Silver skal vi blant annet gjøre:
# MAGIC
# MAGIC ```text
# MAGIC "Fartøy ID"          -> fartoy_id
# MAGIC "Landingsdato"       -> landingsdato (DATE)
# MAGIC "Rundvekt (kg)"      -> rundvekt_kg (DOUBLE)
# MAGIC "Fangstområde (kode)"-> fangstomrade_kode
# MAGIC ```
# MAGIC
# MAGIC I tillegg skal vi håndtere duplikater, manglende verdier og inkonsistente koder.
