#!/usr/bin/env python3
"""
SUNDAY SWIMS — Weerdataverzameling (GitHub versie)
=====================================================
Dit script draait automatisch elke dag via GitHub Actions.
- Haalt weerdata op via open-meteo.com
- Haalt kanaaldata op via MOW-HIC (indien token beschikbaar)
- Combineert met handmatige metingen uit data/metingen.csv
- Exporteert data/sunday_swims_data.json voor de website

HIC_TOKEN wordt ingelezen als omgevingsvariabele (GitHub Secret).
"""

import requests
import pandas as pd
import json
import os
from datetime import date, timedelta
from pathlib import Path

# ─── INSTELLINGEN ────────────────────────────────────────────────────────────

LATITUDE  = 50.8403
LONGITUDE = 4.3372

# Token wordt meegegeven via GitHub Secrets (omgevingsvariabele)
HIC_TOKEN = os.environ.get("HIC_TOKEN", "")

HIC_BASE_URL = "https://hicws.vlaanderen.be/KiWIS/KiWIS"

SCRIPT_DIR   = Path(__file__).parent
DATA_DIR     = SCRIPT_DIR / "data"
JSON_FILE    = DATA_DIR / "sunday_swims_data.json"
METINGEN_CSV = DATA_DIR / "metingen.csv"

DATA_DIR.mkdir(exist_ok=True)

# ─── HULPFUNCTIES ────────────────────────────────────────────────────────────

def windrichting_naar_naam(graden) -> str:
    try:
        if graden is None:
            return ""
        g = float(graden)
        if g != g:
            return ""
        richtingen = ["N", "NO", "O", "ZO", "Z", "ZW", "W", "NW"]
        return richtingen[round(g / 45) % 8]
    except Exception:
        return ""

# ─── OPEN-METEO ──────────────────────────────────────────────────────────────

def haal_weerdata_op(start_datum: str, eind_datum: str) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":  LATITUDE,
        "longitude": LONGITUDE,
        "daily": [
            "precipitation_sum", "precipitation_hours",
            "rain_sum", "snowfall_sum", "showers_sum",
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "windspeed_10m_max", "winddirection_10m_dominant",
            "uv_index_max", "sunshine_duration", "cloudcover_mean",
        ],
        "hourly": ["precipitation"],
        "start_date": start_datum,
        "end_date":   eind_datum,
        "timezone":   "Europe/Brussels",
    }

    print(f"  → Weerdata ophalen: {start_datum} → {eind_datum}")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    d = r.json()

    df = pd.DataFrame({
        "datum":                d["daily"]["time"],
        "neerslag_mm":          d["daily"]["precipitation_sum"],
        "neerslag_uren":        d["daily"]["precipitation_hours"],
        "regen_mm":             d["daily"]["rain_sum"],
        "sneeuw_cm":            d["daily"]["snowfall_sum"],
        "buien_mm":             d["daily"]["showers_sum"],
        "temp_max_c":           d["daily"]["temperature_2m_max"],
        "temp_min_c":           d["daily"]["temperature_2m_min"],
        "temp_gemiddeld_c":     d["daily"]["temperature_2m_mean"],
        "windsnelheid_max_kmh": d["daily"]["windspeed_10m_max"],
        "windrichting_graden":  d["daily"]["winddirection_10m_dominant"],
        "uv_index_max":         d["daily"]["uv_index_max"],
        "zonneschijn_uur":      [
            round(s / 3600, 2) if s is not None else None
            for s in d["daily"]["sunshine_duration"]
        ],
        "bewolking_pct":        d["daily"]["cloudcover_mean"],
    })
    df["datum"] = pd.to_datetime(df["datum"]).dt.date

    # Max neerslag per uur
    uurlijks = pd.DataFrame({
        "datum_uur":       d["hourly"]["time"],
        "neerslag_mm_uur": d["hourly"]["precipitation"],
    })
    uurlijks["datum"] = pd.to_datetime(uurlijks["datum_uur"]).dt.date
    max_uur = (
        uurlijks.groupby("datum")["neerslag_mm_uur"]
        .max().reset_index()
        .rename(columns={"neerslag_mm_uur": "neerslag_max_mm_per_uur"})
    )
    return df.merge(max_uur, on="datum", how="left")


def bereken_cumulatieve_neerslag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("datum").reset_index(drop=True)
    df["neerslag_24u_mm"] = df["neerslag_mm"].rolling(1, min_periods=1).sum().round(1)
    df["neerslag_48u_mm"] = df["neerslag_mm"].rolling(2, min_periods=1).sum().round(1)
    df["neerslag_72u_mm"] = df["neerslag_mm"].rolling(3, min_periods=1).sum().round(1)
    return df

# ─── MOW-HIC ─────────────────────────────────────────────────────────────────

def hic_zoek_ts_id(station: str, parameter: str):
    params = {
        "service":            "kisters",
        "type":               "queryServices",
        "request":            "getTimeseriesList",
        "station_name":       station,
        "parametertype_name": parameter,
        "format":             "json",
        "Authorization":      f"Bearer {HIC_TOKEN}",
    }
    try:
        r = requests.get(HIC_BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if len(data) > 1:
            return str(data[1][0])
    except Exception as e:
        print(f"    ! ts_id niet gevonden voor {station}/{parameter}: {e}")
    return None


def haal_hic_data_op(ts_id: str, start_datum: str, eind_datum: str) -> pd.DataFrame:
    params = {
        "service":       "kisters",
        "type":          "queryServices",
        "request":       "getTimeseriesValues",
        "ts_id":         ts_id,
        "from":          f"{start_datum}T00:00:00",
        "to":            f"{eind_datum}T23:59:59",
        "format":        "json",
        "Authorization": f"Bearer {HIC_TOKEN}",
    }
    r = requests.get(HIC_BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if not data or len(data) < 1:
        return pd.DataFrame(columns=["datum_uur", "waarde"])

    rijen = data[0].get("data", [])
    if not rijen:
        return pd.DataFrame(columns=["datum_uur", "waarde"])

    df = pd.DataFrame(rijen, columns=["datum_uur", "waarde"])
    df["datum_uur"] = pd.to_datetime(df["datum_uur"])
    df["waarde"]    = pd.to_numeric(df["waarde"], errors="coerce")
    return df


def aggregeer_naar_dag(df_uur: pd.DataFrame, prefix: str,
                       heeft_min: bool = False) -> pd.DataFrame:
    df_uur["datum"] = df_uur["datum_uur"].dt.date
    agg_dict = {"mean": "mean", "max": "max"}
    if heeft_min:
        agg_dict["min"] = "min"

    agg = df_uur.groupby("datum")["waarde"].agg(**{
        f"{prefix}_gem": "mean",
        f"{prefix}_max": "max",
        **({f"{prefix}_min": "min"} if heeft_min else {})
    }).round(3).reset_index()
    return agg


def haal_alle_hic_data_op(start_datum: str, eind_datum: str):
    if not HIC_TOKEN:
        print("  → Geen HIC-token, kanaaldata overgeslagen.")
        return None

    print("  → Kanaaldata ophalen via MOW-HIC...")
    resultaat = None

    station_params = [
        ("kbc02g-1066",     "afvoer",    "kanaal_afvoer",      True),
        ("kbc02g-1066",     "waterpeil", "kanaal_peil",         False),
        ("KC-RUI-OPW-1095", "waterpeil", "ruisbroek_opw_peil",  False),
        ("KC-RUI-AFW-1095", "waterpeil", "ruisbroek_afw_peil",  False),
    ]

    for station, parameter, prefix, heeft_min in station_params:
        print(f"    → {station} / {parameter}...")
        ts_id = hic_zoek_ts_id(station, parameter)
        if ts_id is None:
            continue
        try:
            df_uur = haal_hic_data_op(ts_id, start_datum, eind_datum)
            if df_uur.empty:
                continue
            df_dag = aggregeer_naar_dag(df_uur, prefix, heeft_min)
            resultaat = df_dag if resultaat is None else resultaat.merge(df_dag, on="datum", how="outer")
        except Exception as e:
            print(f"    ! Fout: {e}")

    return resultaat

# ─── HANDMATIGE METINGEN ─────────────────────────────────────────────────────

def laad_metingen() -> pd.DataFrame:
    """Laadt handmatige metingen uit data/metingen.csv."""
    if not METINGEN_CSV.exists():
        print("  → Geen metingen.csv gevonden.")
        return pd.DataFrame()

    df = pd.read_csv(METINGEN_CSV)
    if df.empty or "datum" not in df.columns:
        return pd.DataFrame()

    df["datum"] = pd.to_datetime(df["datum"]).dt.date
    print(f"  → {len(df)} handmatige meting(en) geladen uit metingen.csv")
    return df


# ─── WEERSVOORSPELLING ───────────────────────────────────────────────────────

def haal_voorspelling_op() -> list:
    """Haalt de weersvoorspelling voor de komende 7 dagen op via Open-Meteo."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":  LATITUDE,
        "longitude": LONGITUDE,
        "daily": [
            "precipitation_sum",
            "precipitation_probability_max",
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "windspeed_10m_max",
            "winddirection_10m_dominant",
            "uv_index_max",
            "sunshine_duration",
            "cloudcover_mean",
            "weathercode",
        ],
        "forecast_days": 7,
        "timezone": "Europe/Brussels",
    }

    print("  → Weersvoorspelling ophalen (7 dagen)...")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    d = r.json()

    records = []
    for i, datum in enumerate(d["daily"]["time"]):
        records.append({
            "datum":                  datum,
            "temp_max_c":             d["daily"]["temperature_2m_max"][i],
            "temp_min_c":             d["daily"]["temperature_2m_min"][i],
            "temp_gemiddeld_c":       d["daily"]["temperature_2m_mean"][i],
            "neerslag_mm":            d["daily"]["precipitation_sum"][i],
            "neerslag_kans_pct":      d["daily"]["precipitation_probability_max"][i],
            "windsnelheid_max_kmh":   d["daily"]["windspeed_10m_max"][i],
            "windrichting_naam":      windrichting_naar_naam(d["daily"]["winddirection_10m_dominant"][i]),
            "uv_index_max":           d["daily"]["uv_index_max"][i],
            "zonneschijn_uur":        round(d["daily"]["sunshine_duration"][i] / 3600, 1) if d["daily"]["sunshine_duration"][i] else None,
            "bewolking_pct":          d["daily"]["cloudcover_mean"][i],
            "weathercode":            d["daily"]["weathercode"][i],
        })
    return records

# ─── JSON ────────────────────────────────────────────────────────────────────

def laad_bestaande_json() -> pd.DataFrame:
    """Laadt bestaande data uit JSON als die al bestaat."""
    if not JSON_FILE.exists():
        print("  → Nog geen JSON-bestand, wordt nieuw aangemaakt.")
        return pd.DataFrame()

    with open(JSON_FILE, encoding="utf-8") as f:
        bestaand = json.load(f)

    data = bestaand.get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["datum"] = pd.to_datetime(df["datum"]).dt.date
    print(f"  → Bestaande JSON geladen: {len(df)} rijen")
    return df


def bepaal_ontbrekende_datums(bestaande_df: pd.DataFrame):
    vandaag   = date.today()
    max_terug = vandaag - timedelta(days=89)

    if bestaande_df.empty:
        return str(max_terug), str(vandaag)

    laatste  = bestaande_df["datum"].max()
    volgende = laatste + timedelta(days=1)

    if volgende > vandaag:
        return None, None

    return str(volgende), str(vandaag)


def exporteer_json(df: pd.DataFrame):
    """Exporteert de volledige dataset naar data/sunday_swims_data.json."""
    df = df.sort_values("datum").reset_index(drop=True)

    # Windrichting als naam toevoegen
    if "windrichting_graden" in df.columns:
        df["windrichting_naam"] = df["windrichting_graden"].apply(windrichting_naar_naam)

    # Datum als string
    df["datum"] = df["datum"].astype(str)

    # NaN → null (NaN is invalid JSON)
    json_str = df.to_json(orient="records", force_ascii=False, date_format="iso")
    json_str = json_str.replace(": NaN", ": null").replace(":NaN", ":null")
    records = json.loads(json_str)

    voorspelling = haal_voorspelling_op()

    output = {
        "gegenereerd_op": str(date.today()),
        "locatie": {
            "naam": "Kanaal Brussel-Charleroi, Anderlecht",
            "lat":  LATITUDE,
            "lon":  LONGITUDE,
        },
        "data": records,
        "voorspelling": voorspelling,
    }

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  → JSON opgeslagen: {JSON_FILE}  ({len(df)} rijen)")

# ─── HOOFDPROGRAMMA ──────────────────────────────────────────────────────────

def main():
    print("\n╔══════════════════════════════════════════╗")
    print("║  SUNDAY SWIMS — Weerdataverzameling      ║")
    print("╚══════════════════════════════════════════╝\n")

    bestaande_df = laad_bestaande_json()
    start, eind  = bepaal_ontbrekende_datums(bestaande_df)

    if start is None:
        print("  ✓ Weerdata up-to-date.")
        nieuwe_df = pd.DataFrame()
    else:
        nieuwe_df = haal_weerdata_op(start, eind)

        hic_df = haal_alle_hic_data_op(start, eind)
        if hic_df is not None:
            hic_df["datum"] = pd.to_datetime(hic_df["datum"]).dt.date
            nieuwe_df = nieuwe_df.merge(hic_df, on="datum", how="left")

    # Samenvoegen
    if not bestaande_df.empty and not nieuwe_df.empty:
        gecombineerd = (
            pd.concat([bestaande_df, nieuwe_df], ignore_index=True)
            .drop_duplicates(subset=["datum"])
            .sort_values("datum")
            .reset_index(drop=True)
        )
    elif not nieuwe_df.empty:
        gecombineerd = nieuwe_df
    else:
        gecombineerd = bestaande_df

    # Cumulatieve neerslag herberekenen over volledige dataset
    gecombineerd = bereken_cumulatieve_neerslag(gecombineerd)

    # Handmatige metingen samenvoegen
    metingen = laad_metingen()
    if not metingen.empty:
        # Verwijder handmatige kolommen uit gecombineerd zodat metingen.csv altijd wint
        handmatige_kolommen = [c for c in metingen.columns if c != "datum"]
        for k in handmatige_kolommen:
            if k in gecombineerd.columns:
                gecombineerd = gecombineerd.drop(columns=[k])
        gecombineerd = gecombineerd.merge(metingen, on="datum", how="left")

    exporteer_json(gecombineerd)

    print(f"\n  ✓ Klaar! Totaal: {len(gecombineerd)} dagen in dataset.\n")


if __name__ == "__main__":
    main()
