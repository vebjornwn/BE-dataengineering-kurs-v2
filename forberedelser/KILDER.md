# Datakilder og regenerering av kursdata

Repoet inneholder et frosset 2024-uttrekk for å gjøre kurset raskt og reproducerbart.

Under selve kurset skal deltakerne **ikke** laste ned store eksterne datasett.

## 1. Fangst- og landingsdata – Fiskeridirektoratet

Offisiell side:

https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata

Direkte 2024-fil:

https://register.fiskeridir.no/uttrekk/fangstdata_2024.csv.zip

Kilden er UTF-8 CSV med semikolon og én fil per fangstår.

For kursversjonen brukes et lite, sesongdekkende uttrekk:

- 2024
- torsk, hyse og sei
- utvalgte fangstområder
- inntil åtte gyldige varelinjer per uke, art og fangstområde
- én ekstra ekte kvalitetsproblemrad per gruppe der kilden har manglende fartøy-ID eller ugyldig rundvekt
- felter som dokument/seddel, dato, fartøy-ID, art, fangstområde, redskap og rundvekt

Dokumentasjonen beskriver blant annet at enheten er dokument/varelinje, at Fartøy ID er intern fartøynøkkel og at Rundvekt er kg levende vekt.

## 2. Fartøydata – Fiskeridirektoratet

Offisiell side:

https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fiskere-fartoy-og-fisketillatelser

Fangstfilen er allerede koblet med fartøydata på fangstdatoen. Det frosne
`fartoy_2024.csv` bygges derfor primært fra de samme radene som kurslandingene.
Dette sikrer at alle ikke-tomme fartøy-ID-er i kursuttrekket kan kobles til en
faktisk båt. Bredde, som ikke finnes i fangstfilen, berikes fra merkeregisterets
XLSX-fil med årsark.

Til kurset beholdes:

- Fartøy ID
- registreringsmerke og fartøynavn
- fartøytype og nasjonalitet
- fartøygruppe
- lengde og lengdegruppe
- bredde
- bruttotonnasje
- byggeår
- motorkraft
- hjemkommune

Fartøymasteren har én rad per fartøy. Ved flere observasjoner brukes de nyeste
registrerte verdiene i 2024-uttrekket, med eldre ikke-tomme verdier som fallback
for enkeltfelter.

## 3. Havforhold – Open-Meteo Marine

Dokumentasjon:

https://open-meteo.com/en/docs/marine-weather-api

Relevante variabler:

- sea_water_temperature
- sea_surface_wave_height
- sea_water_speed

Historiske data skal preprosesseres **før** kurset til:

```text
dato
fangstomrade_kode
fangstomrade_navn
havtemperatur_c
bolgehoyde_m
stromhastighet_ms
```

Kursfilen lagres som JSON Lines slik at deltakerne får en semi-strukturert kilde.

## 4. Kalender

Genereres lokalt for hele 2024.

Inneholder blant annet:

- dato
- uke_start
- år
- måned
- ISO-uke
- ukedag
- helg
- helligdag
- sesong

Kalenderen gir et naturlig utgangspunkt for `dim_dato` og sesongfeatures.

## Hvorfor vi fryser data i repoet

Det gjør at:

- 40 deltakere bruker identisk input
- kurset fungerer selv om et eksternt API er nede
- vi unngår flere hundre MB/GByte med nedlasting
- serverless-kjøringene blir korte
- resultatene og løsningsforslagene er reproducerbare

Kildehenting og preprocessing er en instruktør-/byggejobb, ikke en deltakeroppgave.
