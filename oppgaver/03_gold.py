# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Oppgave 3 - Gold
# MAGIC I Silver gjorde vi de enkelte kildene rene og konsistente.
# MAGIC
# MAGIC #### Gold-laget
# MAGIC
# MAGIC I Gold modellerer vi data for analyse, rapportering og maskinlæring. Her kombinerer vi data fra flere kilder og organiserer dem slik at de blir enkle å forstå og bruke for analytikere, dashboards og ML-modeller.
# MAGIC
# MAGIC Vi skal lage:
# MAGIC - dimensjoner for fartøy, art, fangstområde og dato
# MAGIC - en faktatabell for landinger
# MAGIC - en ukentlig mart med aggregater og eksterne havdata
# MAGIC - en feature-mart for neste ukes fangstprognose
# MAGIC
# MAGIC ## Hva betyr grain?
# MAGIC Før du lager en tabell bør du kunne fullføre setningen:
# MAGIC
# MAGIC "Én rad i denne tabellen representerer ..."
# MAGIC
# MAGIC Dette kalles tabellens **grain** og beskriver detaljnivået i dataene. En rad kan for eksempel representere én landing, én fisketype per landing eller én uke i ett fangstområde.
# MAGIC
# MAGIC Grain er noe av det viktigste å forstå før man joiner data. Dersom to tabeller har forskjellig grain, kan man lett få dupliserte rader eller feil aggregater.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]

landinger = spark.read.table(f"{catalog}.silver.landinger")
fartoy = spark.read.table(f"{catalog}.silver.fartoy")
havforhold = spark.read.table(f"{catalog}.silver.havforhold")
kalender = spark.read.table(f"{catalog}.silver.kalender")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 3a – dimensjoner og faktatabell
# MAGIC
# MAGIC Vi bruker følgende grain:
# MAGIC
# MAGIC ```text
# MAGIC dim_fartoy        én rad per fartøy
# MAGIC dim_art           én rad per art
# MAGIC dim_fangstomrade  én rad per fangstområde
# MAGIC dim_dato          én rad per dato
# MAGIC fact_landinger    én rad per unik landingsseddel
# MAGIC ```
# MAGIC
# MAGIC Lag først `dim_art` og `dim_fangstomrade`.
# MAGIC
# MAGIC Bruk gjerne `xxhash64()` for å lage en enkel surrogate key.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC ```python
# MAGIC landinger.select(...).distinct().withColumn(
# MAGIC     "art_key",
# MAGIC     F.xxhash64(...)
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

dim_art = (
    landinger
        .select("art_kode", "art_navn")
        .distinct()
        .withColumn("art_key", F.xxhash64("art_kode"))
        .select("art_key", "art_kode", "art_navn")
)

dim_fangstomrade = (
    landinger
        .select("fangstomrade_kode", "fangstomrade_navn")
        .distinct()
        .withColumn("fangstomrade_key", F.xxhash64("fangstomrade_kode"))
        .select(
            "fangstomrade_key",
            "fangstomrade_kode",
            "fangstomrade_navn",
        )
)

dim_fartoy = (
    fartoy
        .withColumn("fartoy_key", F.xxhash64("fartoy_id"))
        .select("fartoy_key", *fartoy.columns)
)

dim_dato = (
    kalender
        .withColumn("dato_key", F.date_format("dato", "yyyyMMdd").cast("int"))
        .select("dato_key", *kalender.columns)
)

for navn, df in [
    ("dim_art", dim_art),
    ("dim_fangstomrade", dim_fangstomrade),
    ("dim_fartoy", dim_fartoy),
    ("dim_dato", dim_dato),
]:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.{navn}")

# COMMAND ----------

# MAGIC %md
# MAGIC Nå lager vi faktatabellen ved å koble nøklene fra dimensjonene på landingsdataene.

# COMMAND ----------

fact_landinger = (
    landinger
        .join(dim_art, ["art_kode", "art_navn"], "left")
        .join(
            dim_fangstomrade,
            ["fangstomrade_kode", "fangstomrade_navn"],
            "left",
        )
        .join(dim_fartoy.select("fartoy_id", "fartoy_key"), "fartoy_id", "left")
        .join(
            dim_dato
                .select(
                    F.col("dato").alias("landingsdato"),
                    "dato_key",
                ),
            "landingsdato",
            "left",
        )
        .select(
            "seddelnummer",
            "dato_key",
            "fartoy_key",
            "art_key",
            "fangstomrade_key",
            "landingsdato",
            "fartoy_id",
            "art_kode",
            "fangstomrade_kode",
            "redskap_kode",
            "rundvekt_kg",
        )
)

fact_landinger.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.gold.fact_landinger"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 3b – ukentlig mart
# MAGIC
# MAGIC Nå endrer vi grain.
# MAGIC
# MAGIC Én rad i `mart_fangst_uke` skal representere:
# MAGIC
# MAGIC > **én uke × én art × ett fangstområde**
# MAGIC
# MAGIC Beregn minst:
# MAGIC
# MAGIC - totalt landet kg
# MAGIC - antall landinger
# MAGIC - antall aktive fartøy
# MAGIC - gjennomsnittlig fartøylengde
# MAGIC - gjennomsnittlig motorkraft
# MAGIC
# MAGIC Deretter skal havdata joines inn på samme ukentlige grain.
# MAGIC
# MAGIC Merk at ett fartøy kan ha flere varelinjer samme uke. Beregn derfor
# MAGIC flåteegenskapene på **unike fartøy per uke × art × område**, slik at en båt
# MAGIC ikke får større vekt bare fordi landingen har flere varelinjer.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC Først:
# MAGIC
# MAGIC ```python
# MAGIC F.date_trunc("week", "landingsdato").cast("date")
# MAGIC ```
# MAGIC
# MAGIC Aggreger havforhold separat per uke og område **før** du joiner.

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

landinger_med_fartoy = (
    landinger
        .join(
            fartoy.select(
                "fartoy_id",
                "lengde_meter",
                "motorkraft_kw",
            ),
            "fartoy_id",
            "left",
        )
        .withColumn(
            "uke_start",
            F.date_trunc("week", "landingsdato").cast("date"),
        )
)

uke_grain = [
    "uke_start",
    "art_kode",
    "art_navn",
    "fangstomrade_kode",
    "fangstomrade_navn",
]

fangst_volum_uke = (
    landinger_med_fartoy
        .groupBy(*uke_grain)
        .agg(
            F.sum("rundvekt_kg").alias("landet_kg"),
            F.countDistinct("seddelnummer").alias("antall_landinger"),
        )
)

fartoy_uke = (
    landinger_med_fartoy
        .select(
            *uke_grain,
            "fartoy_id",
            "lengde_meter",
            "motorkraft_kw",
        )
        .dropDuplicates(uke_grain + ["fartoy_id"])
        .groupBy(*uke_grain)
        .agg(
            F.countDistinct("fartoy_id").alias("aktive_fartoy"),
            F.avg("lengde_meter").alias("gjennomsnitt_fartoy_lengde"),
            F.avg("motorkraft_kw").alias("gjennomsnitt_motorkraft_kw"),
        )
)

fangst_uke = fangst_volum_uke.join(fartoy_uke, uke_grain, "left")

hav_uke = (
    havforhold
        .withColumn(
            "uke_start",
            F.date_trunc("week", "dato").cast("date"),
        )
        .groupBy("uke_start", "fangstomrade_kode")
        .agg(
            F.avg("havtemperatur_c").alias("havtemperatur_c"),
            F.avg("bolgehoyde_m").alias("bolgehoyde_m"),
            F.avg("stromhastighet_ms").alias("stromhastighet_ms"),
        )
)

uke_kalender = (
    kalender
        .filter(F.col("ukedag") == 1)
        .select(
            "uke_start",
            "ukenummer",
            "maaned",
            "sesong",
        )
        .dropDuplicates(["uke_start"])
)

mart_fangst_uke = (
    fangst_uke
        .join(
            hav_uke,
            ["uke_start", "fangstomrade_kode"],
            "left",
        )
        .join(uke_kalender, "uke_start", "left")
        .withColumn(
            "art_id",
            F.when(F.col("art_kode") == "COD", 1)
             .when(F.col("art_kode") == "HAD", 2)
             .when(F.col("art_kode") == "POK", 3)
             .otherwise(99),
        )
        .withColumn(
            "fangstomrade_id",
            F.col("fangstomrade_kode").cast("int"),
        )
)

mart_fangst_uke.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.gold.mart_fangst_uke"
)

display(mart_fangst_uke.orderBy("uke_start", "art_kode").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oppgave 3c – feature-mart for prognosen
# MAGIC
# MAGIC Nå skal vi lage features som en maskinlæringsmodell kan bruke.
# MAGIC
# MAGIC For hver kombinasjon av art og fangstområde lager vi:
# MAGIC
# MAGIC - fangst forrige uke
# MAGIC - fangst for 4 uker siden
# MAGIC - gjennomsnittlig fangst siste 4 uker
# MAGIC - sykliske sesongfeatures (`uke_sin`, `uke_cos`)
# MAGIC - target = landet kg **neste uke**
# MAGIC
# MAGIC Hver rad bruker bare informasjon som er kjent ved slutten av den aktuelle
# MAGIC uken. `aktive_fartoy` og flåteegenskapene beskriver altså uke *t*, mens
# MAGIC targeten fra `lead()` er fangsten i uke *t+1*. Vi bruker ikke fartøyene som
# MAGIC faktisk blir aktive i uken vi prøver å predikere.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint**
# MAGIC
# MAGIC Bruk et Window:
# MAGIC
# MAGIC ```python
# MAGIC Window.partitionBy(
# MAGIC     "art_kode",
# MAGIC     "fangstomrade_kode"
# MAGIC ).orderBy("uke_start")
# MAGIC ```
# MAGIC
# MAGIC og funksjonene `lag()`, `lead()` og `avg()`.

# COMMAND ----------

# Skriv løsningen din her.

# COMMAND ----------

# MAGIC %md
# MAGIC **Løsningsforslag**

# COMMAND ----------

serie_vindu = (
    Window
        .partitionBy("art_kode", "fangstomrade_kode")
        .orderBy("uke_start")
)

rullerende_vindu = serie_vindu.rowsBetween(-3, 0)

mart_fangstprognose = (
    mart_fangst_uke
        .withColumn(
            "landet_kg_lag_1",
            F.lag("landet_kg", 1).over(serie_vindu),
        )
        .withColumn(
            "landet_kg_lag_4",
            F.lag("landet_kg", 4).over(serie_vindu),
        )
        .withColumn(
            "gjennomsnitt_4_uker",
            F.avg("landet_kg").over(rullerende_vindu),
        )
        .withColumn(
            "uke_sin",
            F.sin(F.lit(2 * math.pi) * F.col("ukenummer") / F.lit(52.0)),
        )
        .withColumn(
            "uke_cos",
            F.cos(F.lit(2 * math.pi) * F.col("ukenummer") / F.lit(52.0)),
        )
        .withColumn(
            "target_uke_start",
            F.date_add("uke_start", 7),
        )
        .withColumn("target_aar", F.year("target_uke_start"))
        .withColumn("target_maaned", F.month("target_uke_start"))
        .withColumn(
            "landet_kg_neste_uke",
            F.lead("landet_kg", 1).over(serie_vindu),
        )
)

mart_fangstprognose.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.gold.mart_fangstprognose"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kontroller Gold

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN `{catalog}`.`gold`"))

display(
    spark.read.table(f"{catalog}.gold.mart_fangstprognose")
        .orderBy("art_kode", "fangstomrade_kode", "uke_start")
        .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Nå har vi et dataprodukt med riktig grain og historiske features. Det er dette datasettet data science-teamet kan bygge modellen sin på.