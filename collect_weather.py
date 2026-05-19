#!/usr/bin/env python3
"""
SUNDAY SWIMS — Weerdataverzameling (GitHub versie)
=====================================================
Dit script draait automatisch elke dag via GitHub Actions.
- Haalt gemeten weerdata op via KMI open data (station Ukkel, code 6447)
- Haalt weersvoorspelling op via Open-Meteo (7 dagen)
- Haalt kanaaldata op via MOW-HIC groep 3323277 (indien token beschikbaar)
- Combineert met handmatige metingen uit data/metingen.csv
- Exporteert data/sunday_swims_data.json voor de website

HIC_TOKEN wordt ingelezen als omgevingsvariabele (GitHub Secret).
Het is de Base64-gecodeerde 'clientId:clientSecret' string van HIC.
"""

import requests
import pandas as pd
import json
import os
from datetime import date, timedelta, timezone, datetime
from pathlib import Path
from urllib.parse import quote_plus

# ─── INSTELLINGEN ────────────────────────────────────────────────────────────

LATITUDE  = 50.8403
LONGITUDE = 4.3372

# KMI open data — geen token vereist
KMI_WFS_URL    = "https://opendata.meteo.be/service/ows"
KMI_STATION    = 6447   # Ukkel (Uccle) — dichtstbijzijnde KMI-station

# HIC_TOKEN = Base64 'clientId:clientSecret' string (GitHub Secret)
HIC_TOKEN      = os.environ.get("HIC_TOKEN", "")
HIC_BASE_URL   = "https://hicws.vlaanderen.be/KiWIS/KiWIS"
HIC_AUTH_URL   = "https://hicwsauth.vlaanderen.be/auth"
HIC_GROUP_ID   = "3323277"   # Dedicated groep aangemaakt door Leen Boeckx (MOW-HIC)

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

# ─── HIC AUTHENTICATIE ───────────────────────────────────────────────────────

def haal_hic_access_token() -> str:
    """
    Vraagt een access token op bij de HIC auth service.
    HIC_TOKEN is de Base64-gecodeerde 'clientId:clientSecret' string.
    Het token is 24 uur geldig — per dagelijkse run één keer opvragen.
    """
    if not HIC_TOKEN:
        print("  → Geen HIC_TOKEN gevonden, kanaaldata wordt overgeslagen.")
        return ""

    print("  → HIC access token opvragen...")
    try:
        r = requests.post(
            HIC_AUTH_URL,
            headers={
                "Authorization": f"Basic {HIC_TOKEN}",
                "Content-Type":  "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=30,
        )
        r.raise_for_status()
        token = r.json().get("access_token", "")
        if token:
            print("  ✓ HIC access token ontvangen.")
        else:
            print("  ! HIC auth response bevat geen access_token.")
        return token
    except Exception as e:
        print(f"  ! HIC authenticatie mislukt: {e}")
        return ""

# ─── KMI WEERDATA ────────────────────────────────────────────────────────────

def haal_kmi_data_op(start_datum: str, eind_datum: str) -> pd.DataFrame:
    """
    Haalt dagelijkse weerdata op uit KMI synoptische observaties (station Ukkel).

    Observatietijden per dag (UTC):
      00:00  precip_range=1  (neerslag 18:00–00:00),  sun_duration_24hours, temp_min (nacht)
      06:00  precip_range=2  (neerslag 18:00–06:00),  temp_min (ochtend)
      12:00  precip_range=1  (neerslag 06:00–12:00)
      18:00  precip_range=2  (neerslag 06:00–18:00),  temp_max

    Dagelijkse neerslag = som van precip_range=2 om 06:00 en 18:00 UTC.
    Alle tijden zijn UTC; datum = lokale datum (UTC+1/+2).
    """
    print(f"  → KMI weerdata ophalen: {start_datum} → {eind_datum} (station Ukkel)...")

    # Ruim iets extra opvragen aan beide kanten om randgevallen te dekken
    dt_start = datetime.fromisoformat(start_datum) - timedelta(days=1)
    dt_eind  = datetime.fromisoformat(eind_datum)  + timedelta(days=1)

    cql = (
        f"code IN ({KMI_STATION}) AND "
        f"timestamp DURING "
        f"{dt_start.strftime('%Y-%m-%dT00:00:00Z')}/"
        f"{dt_eind.strftime('%Y-%m-%dT00:00:00Z')}"
    )

    # CQL_FILTER mag niet dubbel ge-URL-encoded worden door requests.
    # We bouwen de URL handmatig met quote_plus op enkel de CQL-waarde.
    base = (
        f"{KMI_WFS_URL}?service=wfs&version=2.0.0&request=getFeature"
        f"&typeNames=synop:synop_data&outputformat=json&sortBy=timestamp+A"
        f"&CQL_FILTER={quote_plus(cql)}"
    )

    try:
        r = requests.get(base, timeout=60)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ! KMI ophalen mislukt: {e}")
        return pd.DataFrame()

    features = data.get("features", [])
    if not features:
        print("  ! Geen KMI-observaties gevonden.")
        return pd.DataFrame()

    rows = []
    for f in features:
        p = f["properties"]
        rows.append({
            "timestamp":            pd.to_datetime(p["timestamp"], utc=True),
            "precip_quantity":      p.get("precip_quantity"),
            "precip_range":         p.get("precip_range"),
            "temp":                 p.get("temp"),
            "temp_min":             p.get("temp_min"),
            "temp_max":             p.get("temp_max"),
            "wind_speed":           p.get("wind_speed"),
            "wind_direction":       p.get("wind_direction"),
            "wind_peak_speed":      p.get("wind_peak_speed"),
            "humidity_relative":    p.get("humidity_relative"),
            "pressure":             p.get("pressure"),
            "cloudiness":           p.get("cloudiness"),
            "sun_duration_24hours": p.get("sun_duration_24hours"),
        })

    df = pd.DataFrame(rows)

    # Lokale datum (Brussels = UTC+1 standaard, UTC+2 zomer)
    # We gebruiken UTC+1 als veilige benadering; het verschil is verwaarloosbaar
    # voor dagaggregatie van synoptische obs die op vaste UTC-tijden vallen.
    df["datum"] = (df["timestamp"] + pd.Timedelta(hours=1)).dt.date

    # ── Neerslag: som van precip_range=2 om 06:00 en 18:00 UTC ──────────────
    # range=2 om 06:00 = afgelopen 12u (18:00 gisteren – 06:00 vandaag)
    # range=2 om 18:00 = afgelopen 12u (06:00 – 18:00 vandaag)
    # Samen = volledige dag 06:00–06:00, wat overeenkomt met de KMI-dagdefinitie.
    df_r2 = df[
        (df["precip_range"] == 2) &
        (df["timestamp"].dt.hour.isin([6, 18]))
    ].copy()
    df_r2["precip_datum"] = df_r2.apply(
        lambda row: (
            # 18:00 UTC obs hoort bij de lokale datum van die dag
            row["datum"] if row["timestamp"].hour == 18
            # 06:00 UTC obs hoort bij de lokale datum van dezelfde dag
            else row["datum"]
        ),
        axis=1
    )
    neerslag = (
        df_r2.groupby("precip_datum")["precip_quantity"]
        .sum().reset_index()
        .rename(columns={"precip_datum": "datum", "precip_quantity": "neerslag_mm"})
    )
    neerslag["neerslag_mm"] = neerslag["neerslag_mm"].round(1)

    # ── Temperatuur ──────────────────────────────────────────────────────────
    # temp_min: gerapporteerd om 06:00 UTC
    # temp_max: gerapporteerd om 18:00 UTC
    # temp_gemiddeld: gemiddelde van alle uurlijkse observaties op die datum
    temp_min = (
        df[df["timestamp"].dt.hour == 6][["datum", "temp_min"]]
        .dropna(subset=["temp_min"])
        .groupby("datum")["temp_min"].min().reset_index()
        .rename(columns={"temp_min": "temp_min_c"})
    )
    temp_max = (
        df[df["timestamp"].dt.hour == 18][["datum", "temp_max"]]
        .dropna(subset=["temp_max"])
        .groupby("datum")["temp_max"].max().reset_index()
        .rename(columns={"temp_max": "temp_max_c"})
    )
    temp_gem = (
        df.dropna(subset=["temp"])
        .groupby("datum")["temp"].mean().round(1).reset_index()
        .rename(columns={"temp": "temp_gemiddeld_c"})
    )

    # ── Wind ─────────────────────────────────────────────────────────────────
    wind_max = (
        df.dropna(subset=["wind_peak_speed"])
        .groupby("datum")["wind_peak_speed"].max().round(1).reset_index()
        .rename(columns={"wind_peak_speed": "windsnelheid_max_kmh"})
    )
    # Windrichting: van de 18:00 UTC observatie (meest representatief voor de dag)
    wind_richting = (
        df[df["timestamp"].dt.hour == 18][["datum", "wind_direction"]]
        .dropna(subset=["wind_direction"])
        .groupby("datum")["wind_direction"].first().reset_index()
        .rename(columns={"wind_direction": "windrichting_graden"})
    )

    # ── Overige dagwaarden ───────────────────────────────────────────────────
    vochtigheid = (
        df.dropna(subset=["humidity_relative"])
        .groupby("datum")["humidity_relative"].mean().round(1).reset_index()
        .rename(columns={"humidity_relative": "vochtigheid_pct"})
    )
    luchtdruk = (
        df.dropna(subset=["pressure"])
        .groupby("datum")["pressure"].mean().round(1).reset_index()
        .rename(columns={"pressure": "luchtdruk_hpa"})
    )
    bewolking = (
        df.dropna(subset=["cloudiness"])
        .groupby("datum")["cloudiness"]
        # cloudiness schaal 0–8 (oktas) → omzetten naar % (×12.5)
        .mean().apply(lambda x: round(x * 12.5, 1)).reset_index()
        .rename(columns={"cloudiness": "bewolking_pct"})
    )
    # Zonneschijn: gerapporteerd om 00:00 UTC als som over afgelopen 24u (in seconden)
    # Behoort bij de vorige dag lokaal → toewijzen aan datum - 1 dag
    zon_raw = df[
        (df["timestamp"].dt.hour == 0) &
        (df["sun_duration_24hours"].notna())
    ][["datum", "sun_duration_24hours"]].copy()
    zon_raw["datum"] = zon_raw["datum"].apply(lambda d: d - timedelta(days=1))
    zonneschijn = (
        zon_raw.groupby("datum")["sun_duration_24hours"]
        .first().apply(lambda s: round(s / 3600, 2)).reset_index()
        .rename(columns={"sun_duration_24hours": "zonneschijn_uur"})
    )

    # ── Samenvoegen ──────────────────────────────────────────────────────────
    dagframes = [
        neerslag, temp_min, temp_max, temp_gem,
        wind_max, wind_richting, vochtigheid,
        luchtdruk, bewolking, zonneschijn,
    ]
    result = dagframes[0]
    for frame in dagframes[1:]:
        result = result.merge(frame, on="datum", how="outer")

    # Windrichting als naam
    if "windrichting_graden" in result.columns:
        result["windrichting_naam"] = result["windrichting_graden"].apply(
            windrichting_naar_naam)

    # Filteren op gewenste datumrange
    start_d = date.fromisoformat(start_datum)
    eind_d  = date.fromisoformat(eind_datum)
    result = result[
        (result["datum"] >= start_d) & (result["datum"] <= eind_d)
    ].reset_index(drop=True)

    print(f"  ✓ KMI: {len(result)} dag(en) verwerkt.")
    return result


def bereken_cumulatieve_neerslag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("datum").reset_index(drop=True)
    df["neerslag_24u_mm"] = df["neerslag_mm"].rolling(1, min_periods=1).sum().round(1)
    df["neerslag_48u_mm"] = df["neerslag_mm"].rolling(2, min_periods=1).sum().round(1)
    df["neerslag_72u_mm"] = df["neerslag_mm"].rolling(3, min_periods=1).sum().round(1)
    return df

# ─── MOW-HIC ─────────────────────────────────────────────────────────────────

def haal_hic_groepslijst(access_token: str) -> list:
    """
    Haalt de lijst van tijdreeksen op uit groep 3323277.
    Logt de volledige lijst zodat we de exacte ts_name-waarden kunnen aflezen.
    """
    params = {
        "service":            "kisters",
        "type":               "queryServices",
        "request":            "getTimeseriesList",
        "datasource":         "4",
        "timeseriesgroup_id": HIC_GROUP_ID,
        "format":             "json",
    }
    try:
        r = requests.get(
            HIC_BASE_URL,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        if not data or len(data) < 2:
            print("  ! Groepslijst is leeg of onverwacht formaat.")
            print(f"    Ruwe respons: {data}")
            return []

        kolommen = data[0]
        rijen    = data[1:]
        reeksen  = [dict(zip(kolommen, rij)) for rij in rijen]

        print(f"  ✓ {len(reeksen)} tijdreeks(en) gevonden in groep {HIC_GROUP_ID}:")
        for ts in reeksen:
            print(f"    ts_id={ts.get('ts_id')}  station={ts.get('station_name')}  "
                  f"ts_name={ts.get('ts_name')}  parameter={ts.get('parametertype_name')}")
        return reeksen

    except Exception as e:
        print(f"  ! Ophalen groepslijst mislukt: {e}")
        return []


def haal_hic_tijdreeks_op(ts_id: str, start_datum: str, eind_datum: str,
                           access_token: str) -> pd.DataFrame:
    """Haalt ruwe uurdata op voor één ts_id."""
    params = {
        "service":    "kisters",
        "type":       "queryServices",
        "request":    "getTimeseriesValues",
        "datasource": "4",
        "ts_id":      ts_id,
        "from":       f"{start_datum}T00:00:00",
        "to":         f"{eind_datum}T23:59:59",
        "format":     "json",
    }
    r = requests.get(
        HIC_BASE_URL,
        params=params,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
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
    """Aggregeert uurdata naar daggemiddelden (+ max, optioneel min)."""
    df_uur = df_uur.copy()
    df_uur["datum"] = df_uur["datum_uur"].dt.date
    agg = df_uur.groupby("datum")["waarde"].agg(**{
        f"{prefix}_gem": "mean",
        f"{prefix}_max": "max",
        **({f"{prefix}_min": "min"} if heeft_min else {})
    }).round(3).reset_index()
    return agg


def bepaal_prefix_en_min(ts_name: str, station_name: str) -> tuple:
    """Bepaalt kolomprefix en heeft_min op basis van ts_name en station_name."""
    naam    = (ts_name or "").lower()
    station = (station_name or "").lower()

    if "afvoer" in naam or "discharge" in naam or "debiet" in naam:
        return "kanaal_afvoer", True
    if "ruisbroek" in station and ("opw" in station or "upstream" in station):
        return "ruisbroek_opw_peil", False
    if "ruisbroek" in station and ("afw" in station or "downstream" in station):
        return "ruisbroek_afw_peil", False
    if "waterpeil" in naam or "level" in naam or "peil" in naam:
        return "kanaal_peil", False

    return None, False


def haal_alle_hic_data_op(start_datum: str, eind_datum: str,
                           access_token: str):
    """Haalt kanaaldata op via groep 3323277."""
    print(f"  → Kanaaldata ophalen via MOW-HIC groep {HIC_GROUP_ID}...")
    reeksen = haal_hic_groepslijst(access_token)

    if not reeksen:
        print("  ! Geen tijdreeksen gevonden, kanaaldata overgeslagen.")
        return None

    resultaat = None

    for ts in reeksen:
        ts_id    = ts.get("ts_id")
        ts_name  = ts.get("ts_name", "")
        station  = ts.get("station_name", "")
        prefix, heeft_min = bepaal_prefix_en_min(ts_name, station)

        if not ts_id:
            continue
        if prefix is None:
            print(f"    → Onbekende tijdreeks overgeslagen: {station} / {ts_name}")
            continue

        print(f"    → {station} / {ts_name} → {prefix}...")
        try:
            df_uur = haal_hic_tijdreeks_op(ts_id, start_datum, eind_datum,
                                            access_token)
            if df_uur.empty:
                print(f"    ! Geen data voor {station} / {ts_name}")
                continue
            df_dag = aggregeer_naar_dag(df_uur, prefix, heeft_min)
            resultaat = (df_dag if resultaat is None
                         else resultaat.merge(df_dag, on="datum", how="outer"))
        except Exception as e:
            print(f"    ! Fout bij {station} / {ts_name}: {e}")

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

# ─── WEERSVOORSPELLING (Open-Meteo) ──────────────────────────────────────────

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

    print("  → Weersvoorspelling ophalen via Open-Meteo (7 dagen)...")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    d = r.json()

    records = []
    for i, datum in enumerate(d["daily"]["time"]):
        records.append({
            "datum":                datum,
            "temp_max_c":           d["daily"]["temperature_2m_max"][i],
            "temp_min_c":           d["daily"]["temperature_2m_min"][i],
            "temp_gemiddeld_c":     d["daily"]["temperature_2m_mean"][i],
            "neerslag_mm":          d["daily"]["precipitation_sum"][i],
            "neerslag_kans_pct":    d["daily"]["precipitation_probability_max"][i],
            "windsnelheid_max_kmh": d["daily"]["windspeed_10m_max"][i],
            "windrichting_naam":    windrichting_naar_naam(
                                        d["daily"]["winddirection_10m_dominant"][i]),
            "uv_index_max":         d["daily"]["uv_index_max"][i],
            "zonneschijn_uur":      round(d["daily"]["sunshine_duration"][i] / 3600, 1)
                                    if d["daily"]["sunshine_duration"][i] else None,
            "bewolking_pct":        d["daily"]["cloudcover_mean"][i],
            "weathercode":          d["daily"]["weathercode"][i],
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


def exporteer_json(df: pd.DataFrame, alle_metingen: pd.DataFrame = None):
    """Exporteert de dataset naar data/sunday_swims_data.json."""
    df = df.sort_values("datum").reset_index(drop=True)
    df["datum"] = df["datum"].astype(str)

    json_str = df.to_json(orient="records", force_ascii=False, date_format="iso")
    json_str = json_str.replace(": NaN", ": null").replace(":NaN", ":null")
    records  = json.loads(json_str)

    metingen_records = []
    if alle_metingen is not None and not alle_metingen.empty:
        m = alle_metingen.copy()
        m["datum"] = m["datum"].astype(str)
        m_json = m.to_json(orient="records", force_ascii=False)
        m_json = m_json.replace(": NaN", ": null").replace(":NaN", ":null")
        metingen_records = json.loads(m_json)

    voorspelling = haal_voorspelling_op()

    output = {
        "gegenereerd_op": str(date.today()),
        "locatie": {
            "naam": "Kanaal Brussel-Charleroi, Anderlecht",
            "lat":  LATITUDE,
            "lon":  LONGITUDE,
        },
        "data":         records,
        "metingen":     metingen_records,
        "voorspelling": voorspelling,
    }

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  → JSON opgeslagen: {JSON_FILE}  "
          f"({len(df)} rijen, {len(metingen_records)} metingen)")

# ─── HOOFDPROGRAMMA ──────────────────────────────────────────────────────────

def main():
    print("\n╔══════════════════════════════════════════╗")
    print("║  SUNDAY SWIMS — Weerdataverzameling      ║")
    print("╚══════════════════════════════════════════╝\n")

    # Stap 1: HIC access token ophalen (eenmalig per run)
    access_token = haal_hic_access_token()

    # Stap 2: Bepaal welke datums nog ontbreken
    bestaande_df = laad_bestaande_json()
    start, eind  = bepaal_ontbrekende_datums(bestaande_df)

    if start is None:
        print("  ✓ Data up-to-date.")
        nieuwe_df = pd.DataFrame()
    else:
        # Stap 3: KMI weerdata ophalen (gemeten waarden)
        nieuwe_df = haal_kmi_data_op(start, eind)

        if nieuwe_df.empty:
            print("  ! Geen KMI-data beschikbaar — run overgeslagen.")
            return

        # Stap 4: Kanaaldata ophalen via groep 3323277
        if access_token:
            hic_df = haal_alle_hic_data_op(start, eind, access_token)
            if hic_df is not None:
                hic_df["datum"] = pd.to_datetime(hic_df["datum"]).dt.date
                nieuwe_df = nieuwe_df.merge(hic_df, on="datum", how="left")
        else:
            print("  → Kanaaldata overgeslagen (geen geldig access token).")

    # Stap 5: Samenvoegen met bestaande data
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

    # Stap 6: Cumulatieve neerslag herberekenen over volledige dataset
    gecombineerd = bereken_cumulatieve_neerslag(gecombineerd)

    # Stap 7: Handmatige metingen samenvoegen
    metingen = laad_metingen()
    if not metingen.empty:
        handmatige_kolommen = [c for c in metingen.columns if c != "datum"]
        for k in handmatige_kolommen:
            if k in gecombineerd.columns:
                gecombineerd = gecombineerd.drop(columns=[k])
        gecombineerd = gecombineerd.merge(metingen, on="datum", how="left")

    # Stap 8: Exporteren
    exporteer_json(gecombineerd, metingen)

    print(f"\n  ✓ Klaar! Totaal: {len(gecombineerd)} dagen in dataset.\n")


if __name__ == "__main__":
    main()
