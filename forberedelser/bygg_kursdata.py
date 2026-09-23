"""
Bygg frosne kursdata fra åpne kilder.

Dette scriptet er for kursholder / vedlikehold av kurset.
Det skal IKKE kjøres av deltakerne i Databricks.

Avhengigheter:
    pip install pandas requests openpyxl

Merk:
- Fiskeridirektoratets 2024-fil er stor. Scriptet leser den i chunks.
- Havdata hentes fra Open-Meteo Marine API til representative koordinater
  for seks hovedområder. Dataene brukes kun som analysefeatures, ikke navigasjon.
- Kjør scriptet lokalt eller i en egen byggejobb, og commit deretter de små
  resultatfilene til data/raw/.
"""

from __future__ import annotations

import io
import json
import math
import zipfile
from pathlib import Path

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "raw"
CACHE = ROOT / "forberedelser" / "cache"

OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

FANGST_URL = "https://register.fiskeridir.no/uttrekk/fangstdata_2024.csv.zip"

FARTOY_URL = (
    "https://www.fiskeridir.no/statistikk-tall-og-analyse/"
    "data-og-statistikk-om-yrkesfiske/"
    "apne-data-fiskere-fartoy-og-fisketillatelser/"
    "_/attachment/download/"
    "579dd5d6-e9fa-4928-9473-7b9fd0bc2767%3A"
    "7833854935cd44fe0f7ad877a1051d49da8b1634/"
    "fart%C3%B8y-eier-f%C3%B8rste-ledd.xlsx"
)

ARTER = {"COD": "Torsk", "HAD": "Hyse", "POK": "Sei"}
HOVEDOMRADER = {
    "03": ("Øst-Finnmark", 71.0, 30.0),
    "04": ("Vest-Finnmark", 71.0, 24.5),
    "05": ("Røstbanken til Malangsgrunnen", 68.5, 13.0),
    "06": ("Helgelandsbanken", 66.5, 11.0),
    "07": ("Storegga-Frøyabanken", 63.5, 5.5),
    "08": ("Eigersundbanken", 58.5, 4.5),
}


def download(url: str, path: Path) -> Path:
    if path.exists():
        print(f"Bruker cache: {path.name}")
        return path

    print(f"Laster ned: {url}")
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    path.write_bytes(response.content)
    return path


def first_existing(columns, candidates):
    lookup = {str(c).strip().lower(): c for c in columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in lookup:
            return lookup[key]
    return None


def bygg_landinger() -> set[str]:
    """Lag et lite, sesongdekkende uttrekk av 2024-fangstdata."""

    zip_path = download(FANGST_URL, CACHE / "fangstdata_2024.csv.zip")

    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise RuntimeError(f"Forventet én CSV i zip, fant: {csv_names}")

        with zf.open(csv_names[0]) as f:
            chunks = pd.read_csv(
                f,
                sep=";",
                dtype=str,
                chunksize=200_000,
                low_memory=False,
            )

            selected = []

            for chunk in chunks:
                required = [
                    "Dokumentnummer",
                    "Salgslag (kode)",
                    "Linjenummer",
                    "Landingsdato",
                    "Fartøy ID",
                    "Art FAO (kode)",
                    "Art FAO",
                    "Hovedområde (kode)",
                    "Hovedområde",
                    "Redskap (kode)",
                    "Rundvekt",
                ]

                missing = [c for c in required if c not in chunk.columns]
                if missing:
                    raise RuntimeError(
                        "Fiskeridirektoratet har endret schema. "
                        f"Mangler kolonner: {missing}"
                    )

                x = chunk[required].copy()
                x["Art FAO (kode)"] = x["Art FAO (kode)"].astype(str).str.strip()
                x["Hovedområde (kode)"] = (
                    x["Hovedområde (kode)"]
                    .astype(str)
                    .str.replace(".0", "", regex=False)
                    .str.zfill(2)
                )

                x = x[
                    x["Art FAO (kode)"].isin(ARTER)
                    & x["Hovedområde (kode)"].isin(HOVEDOMRADER)
                ]

                if not x.empty:
                    selected.append(x)

    if not selected:
        raise RuntimeError("Ingen rader matchet art/område-filteret.")

    df = pd.concat(selected, ignore_index=True)

    df["Landingsdato_dt"] = pd.to_datetime(
        df["Landingsdato"],
        dayfirst=True,
        errors="coerce",
    )
    df = df[df["Landingsdato_dt"].dt.year == 2024].copy()

    df["uke"] = df["Landingsdato_dt"].dt.isocalendar().week.astype(int)

    # Maks 8 varelinjer per uke x art x hovedområde.
    # Dette holder repoet lite, men bevarer hele sesongen.
    df = (
        df.sort_values(
            [
                "uke",
                "Art FAO (kode)",
                "Hovedområde (kode)",
                "Landingsdato_dt",
            ]
        )
        .groupby(
            ["uke", "Art FAO (kode)", "Hovedområde (kode)"],
            group_keys=False,
        )
        .head(8)
        .copy()
    )

    df["Seddelnummer"] = (
        df["Salgslag (kode)"].fillna("X").astype(str)
        + "-"
        + df["Dokumentnummer"].fillna("X").astype(str)
        + "-"
        + df["Linjenummer"].fillna("X").astype(str)
    )

    out = pd.DataFrame(
        {
            "Seddelnummer": df["Seddelnummer"],
            "Landingsdato": df["Landingsdato_dt"].dt.strftime("%d.%m.%Y"),
            "Fartøy ID": df["Fartøy ID"],
            "Art FAO (kode)": df["Art FAO (kode)"],
            "Art FAO (navn)": df["Art FAO"].fillna(
                df["Art FAO (kode)"].map(ARTER)
            ),
            "Fangstområde (kode)": df["Hovedområde (kode)"],
            "Fangstområde (navn)": df["Hovedområde"],
            "Redskap (kode)": df["Redskap (kode)"],
            "Rundvekt (kg)": pd.to_numeric(
                df["Rundvekt"],
                errors="coerce",
            ),
        }
    )

    path = OUT / "landinger_2024.csv"
    out.to_csv(path, sep=";", index=False, encoding="utf-8")
    print(f"Skrev {len(out):,} rader -> {path}")

    return set(out["Fartøy ID"].dropna().astype(str))


def bygg_fartoy(selected_ids: set[str]) -> None:
    """Bygg masterdata for fartøy som faktisk finnes i kursuttrekket."""

    xlsx_path = download(FARTOY_URL, CACHE / "fartoy_eier.xlsx")
    excel = pd.ExcelFile(xlsx_path)

    sheet = "2024" if "2024" in excel.sheet_names else next(
        (s for s in excel.sheet_names if "2024" in str(s)),
        None,
    )
    if sheet is None:
        raise RuntimeError(
            f"Fant ikke årsark 2024. Ark: {excel.sheet_names}"
        )

    df = pd.read_excel(xlsx_path, sheet_name=sheet)

    id_col = first_existing(df.columns, ["Fartøy ID"])
    length_col = first_existing(df.columns, ["Største lengde", "Lengde"])
    width_col = first_existing(df.columns, ["Bredde"])
    build_col = first_existing(df.columns, ["Byggeår - fartøy", "Byggeår"])
    power_col = first_existing(df.columns, ["Motorkraft"])
    municipality_col = first_existing(
        df.columns,
        ["Fartøykommune", "Kommune", "Kommunenavn"],
    )

    required = {
        "Fartøy ID": id_col,
        "Største lengde": length_col,
        "Bredde": width_col,
        "Byggeår": build_col,
        "Motorkraft": power_col,
    }
    missing = [name for name, col in required.items() if col is None]
    if missing:
        raise RuntimeError(
            "Fant ikke forventede fartøykolonner: "
            + ", ".join(missing)
            + f"\nTilgjengelige kolonner: {list(df.columns)}"
        )

    x = df[df[id_col].astype(str).isin(selected_ids)].copy()

    length = pd.to_numeric(x[length_col], errors="coerce")
    hp = pd.to_numeric(x[power_col], errors="coerce")

    # Enkel forretningsvennlig gruppe til workshop.
    group = pd.cut(
        length,
        bins=[-math.inf, 20, 30, math.inf],
        labels=["Kyst", "Kyst større", "Hav"],
    )

    out = pd.DataFrame(
        {
            "Fartøy ID": x[id_col].astype(str),
            "Fartøygruppe": group.astype(str),
            "Lengde (meter)": length,
            "Bredde (meter)": pd.to_numeric(x[width_col], errors="coerce"),
            "Byggeår": pd.to_numeric(x[build_col], errors="coerce"),
            # Merkeregisteret dokumenterer Motorkraft i HK.
            "Motorkraft (kW)": hp / 1.36,
            "Hjemkommune": (
                x[municipality_col].astype(str)
                if municipality_col is not None
                else ""
            ),
        }
    )

    path = OUT / "fartoy_2024.csv"
    out.to_csv(path, index=False, encoding="utf-8")
    print(f"Skrev {len(out):,} rader -> {path}")


def bygg_havforhold() -> None:
    """
    Hent daglige havfeatures for representative punkter.

    Open-Meteo Marine bruker numeriske modeller. Dette er analysefeatures,
    ikke data for navigasjon.
    """

    rows = []

    for code, (name, lat, lon) in HOVEDOMRADER.items():
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "hourly": (
                "sea_surface_temperature,"
                "wave_height,"
                "ocean_current_velocity"
            ),
            "timezone": "GMT",
            "cell_selection": "sea",
        }

        response = requests.get(
            "https://marine-api.open-meteo.com/v1/marine",
            params=params,
            timeout=180,
        )
        response.raise_for_status()
        payload = response.json()

        hourly = pd.DataFrame(payload["hourly"])
        hourly["time"] = pd.to_datetime(hourly["time"])
        hourly["dato"] = hourly["time"].dt.date

        daily = (
            hourly.groupby("dato", as_index=False)
            .agg(
                havtemperatur_c=("sea_surface_temperature", "mean"),
                bolgehoyde_m=("wave_height", "mean"),
                stromhastighet_kmh=("ocean_current_velocity", "mean"),
            )
        )

        # API-et returnerer km/h som standard for ocean_current_velocity.
        daily["stromhastighet_ms"] = daily["stromhastighet_kmh"] / 3.6

        for row in daily.itertuples(index=False):
            rows.append(
                {
                    "dato": str(row.dato),
                    "fangstomrade_kode": code,
                    "fangstomrade_navn": name,
                    "havtemperatur_c": (
                        None
                        if pd.isna(row.havtemperatur_c)
                        else round(float(row.havtemperatur_c), 2)
                    ),
                    "bolgehoyde_m": (
                        None
                        if pd.isna(row.bolgehoyde_m)
                        else round(float(row.bolgehoyde_m), 2)
                    ),
                    "stromhastighet_ms": (
                        None
                        if pd.isna(row.stromhastighet_ms)
                        else round(float(row.stromhastighet_ms), 3)
                    ),
                }
            )

    path = OUT / "havforhold_2024.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Skrev {len(rows):,} rader -> {path}")


def bygg_kalender() -> None:
    dates = pd.date_range("2024-01-01", "2024-12-31", freq="D")

    holidays = {
        "2024-01-01",
        "2024-03-28",
        "2024-03-29",
        "2024-04-01",
        "2024-05-01",
        "2024-05-09",
        "2024-05-17",
        "2024-05-20",
        "2024-12-25",
        "2024-12-26",
    }

    df = pd.DataFrame({"dato": dates})
    iso = df["dato"].dt.isocalendar()

    df["uke_start"] = (
        df["dato"] - pd.to_timedelta(df["dato"].dt.weekday, unit="D")
    )
    df["aar"] = df["dato"].dt.year
    df["maaned"] = df["dato"].dt.month
    df["ukenummer"] = iso.week.astype(int)
    df["ukedag"] = df["dato"].dt.weekday + 1
    df["er_helg"] = df["ukedag"] >= 6
    df["er_helligdag"] = df["dato"].dt.strftime("%Y-%m-%d").isin(holidays)

    month = df["maaned"]
    df["sesong"] = "hoest"
    df.loc[month.isin([12, 1, 2]), "sesong"] = "vinter"
    df.loc[month.isin([3, 4, 5]), "sesong"] = "vaar"
    df.loc[month.isin([6, 7, 8]), "sesong"] = "sommer"

    df["dato"] = df["dato"].dt.strftime("%Y-%m-%d")
    df["uke_start"] = df["uke_start"].dt.strftime("%Y-%m-%d")

    path = OUT / "kalender_2024.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Skrev {len(df):,} rader -> {path}")


def main() -> None:
    selected_ids = bygg_landinger()
    bygg_fartoy(selected_ids)
    bygg_havforhold()
    bygg_kalender()

    print("\nFerdig. Kontroller data/raw/ og commit filene til repoet.")


if __name__ == "__main__":
    main()
