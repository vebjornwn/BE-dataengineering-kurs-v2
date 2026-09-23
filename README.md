# BE Data Engineering-kurs H26 – Fiskeri

90-minutters workshop i data engineering med Databricks.

## Case

Dere jobber med et dataprodukt for norsk fiskeri.

Målet er å kombinere landinger, fartøydata, havforhold og kalenderdata og til slutt predikere:

> **landet kg neste uke per art og fangstområde**

Kurset bruker torsk, hyse og sei gjennom hele 2024, med januar 2025 som holdout-periode for prognosen.

## Dataflyt

```text
GitHub
   ↓
Landing / Unity Catalog Volume
   ↓
Bronze
   payload VARIANT + teknisk metadata
   ↓
Silver
   typing + snake_case + datakvalitet
   ↓
Gold
   fact + dimensions + ukentlig mart
   ↓
┌──────────────┬───────────────┐
│ ML-prognose  │ AI/BI dashboard│
└──────────────┴───────────────┘
```

## Oppgaver

1. **Bronze** – land fire kilder kildenært som VARIANT
2. **Silver** – parse, type, standardiser og vask data
3. **Gold** – bygg fakta/dimensjoner og ukentlig forecast-mart
4. **Maskinlæring** – prediker neste ukes landingsmengde
5. **Dashboard** – presenter historikk og prognose

Alle oppgavene følger samme mønster som tidligere BearingPoint-kurs:

- kort forklaring
- konkret oppgave
- hint
- løsningsforslag rett etter oppgaven

## Før kurset

Deltakeren bør ha:

1. GitHub-bruker
2. Databricks Free Edition-konto
3. kontrollert at Databricks-workspace åpner

Ingen lokal Python, Spark, Git eller Databricks CLI er nødvendig.

## Start i Databricks

1. Åpne **Workspace**
2. Velg **Create → Git folder**
3. Klon dette repoet
4. Deploy `dev`-targetet fra bundle-visningen
5. Kjør bootstrap-jobben
6. Åpne `oppgaver/01_bronze.py`

### Fallback

Hvis DAB-deploy skaper problemer:

```text
oppsett/00_bootstrap.py → Run all
```

og fortsett deretter med Oppgave 1.

## DAB

`databricks.yml` og `ressurser/bootstrap_job.yml` brukes til å opprette en liten serverless bootstrap-jobb.

DAB er distribusjonsmekanismen, men er bevisst **ikke** en kritisk avhengighet for selve kurset.

## Kursdata

Repoet inneholder et lite, frosset uttrekk av faktiske data fra 2024 og januar 2025 slik at workshopen er reproducerbar og ikke er avhengig av eksterne API-er under gjennomføring.

Landingene og fartøyopplysningene kommer fra Fiskeridirektoratets åpne fangstdata koblet med fartøydata. Fartøybredde berikes fra merkeregisteret, og havforhold kommer fra Open-Meteo Marine. Se `forberedelser/KILDER.md` for kilder, avgrensninger og regenerering.

Uttrekket kan regenereres fra åpne kilder før gjennomføring med `forberedelser/bygg_kursdata.py`.

## Modellevaluering

Modellen trenes på 2024 og resultatet evalueres på fire rullerende ukeprognoser i januar 2025 med:

- MAE
- RMSE
- **WMAPE**
