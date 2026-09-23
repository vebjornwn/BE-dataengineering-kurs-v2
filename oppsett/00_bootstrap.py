# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Oppsett – fiskeriworkshop
# MAGIC
# MAGIC Denne notebooken gjør kun det tekniske oppsettet før kurset starter.
# MAGIC
# MAGIC Den:
# MAGIC - bruker katalogen du allerede står i
# MAGIC - oppretter schemaene `bronze`, `silver` og `gold`
# MAGIC - oppretter Volume `bronze.kildedata`
# MAGIC - kopierer de ferdige råfilene fra Git-repoet til landing-sonen
# MAGIC
# MAGIC Den oppretter **ikke Bronze-tabellene**. Det er Oppgave 1.

# COMMAND ----------

import os
import shutil
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Opprett medallion-strukturen

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
print(f"Bruker katalog: {catalog}")

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`bronze`.`kildedata`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Kopier råfilene til landing-sonen

# COMMAND ----------

source_dir = (Path(os.getcwd()).parent / "data" / "raw").resolve()
volume_dir = Path(f"/Volumes/{catalog}/bronze/kildedata")

if not source_dir.exists():
    raise FileNotFoundError(
        f"Fant ikke data/raw. Forventet: {source_dir}. "
        "Kontroller at Git Folder/bundle inneholder råfilene."
    )

files = sorted(p for p in source_dir.iterdir() if p.is_file())
if not files:
    raise RuntimeError(f"Ingen råfiler funnet i {source_dir}")

volume_dir.mkdir(parents=True, exist_ok=True)

for src in files:
    dst = volume_dir / src.name
    shutil.copyfile(src, dst)
    print(f"Kopiert {src.name} -> {dst}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Smoke test

# COMMAND ----------

expected = {
    "landinger_kurs.csv",
    "fartoy_kurs.csv",
    "havforhold_kurs.jsonl",
    "kalender_kurs.csv",
}
actual = {p.name for p in volume_dir.iterdir() if p.is_file()}
missing = expected - actual

print("\nFiler i landing-sonen:")
for path in sorted(volume_dir.iterdir()):
    print(f" - {path.name} ({path.stat().st_size:,} bytes)")

if missing:
    raise RuntimeError(f"Mangler filer etter bootstrap: {sorted(missing)}")

print("\n✅ Oppsett ferdig. Åpne oppgaver/01_bronze.py.")
