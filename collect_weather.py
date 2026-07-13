#!/usr/bin/env python3
"""
SUNDAY SWIMS — Weerdataverzameling (GitHub versie)
=====================================================
Dit script draait automatisch elke dag via GitHub Actions.
- Haalt gemeten weerdata op via KMI open data (station Ukkel, code 6447)
- Haalt weersvoorspelling op via Open-Meteo (7 dagen)
- Haalt kanaaldata op via MOW-HIC groep 3323277 (indien token beschikbaar)
- Haalt sluisdata en neerslag op via Flowbru/Hydria API (indien credentials beschikbaar)
- Combineert met handmatige metingen uit data/metingen.csv
- Exporteert data/sunday_swims_data.json voor de website

HIC_TOKEN wordt ingelezen als omgevingsvariabele (GitHub Secret).
FLOWBRU_USER en FLOWBRU_PASS worden ingelezen als GitHub Secrets.
"""

import requests
import pandas as pd
import json
json_module = json
import json as json_module
import os
import base64
from datetime import date, timedelta, timezone, datetime
from pathlib import Path
from urllib.parse import quote_plus
from aws_request_signer import AwsRequestSigner

# ─── INSTELLINGEN ────────────────────────────────────────────────────────────

LATITUDE  = 50.8403
LONGITUDE = 4.3372

# KMI open data — geen token vereist
KMI_WFS_URL    = "https://opendata.meteo.be/service/ows"
KMI_STATION    = 6447   # Ukkel (Uccle) — dichtstbijzijnde KMI-station

# HIC — Base64 'clientId:clientSecret' string (GitHub Secret)
HIC_TOKEN      = os.environ.get("HIC_TOKEN", "")
HIC_BASE_URL   = "https://hicws.vlaanderen.be/KiWIS/KiWIS"
HIC_AUTH_URL   = "https://hicwsauth.vlaanderen.be/auth"
HIC_GROUP_ID   = "3323277"

# Flowbru/Hydria API (GitHub Secrets)
FLOWBRU_USER   = os.environ.get("FLOWBRU_USER", "")
FLOWBRU_PASS   = os.environ.get("FLOWBRU_PASS", "")
FLOWBRU_BASE   = "https://www.flowbru.eu/api/1"
FLOWBRU_CID    = "9D9E9DE1F0E437A6"

# Flowbru station/channel IDs (uit gebruikersovereenkomst)
FLOWBRU_STATIONS = [
    # Pluvio Ecluse Anderlecht: raw 5-min data wordt apart opgehaald
    # via haal_flowbru_neerslag_op() en gesommeerd per dag.
    {
        "name":    "Canal Ecluse Anderlecht AMONT",
        "sid":     "9EF0952181A3B8AF",
        "channel": "ch0",
        "aggr":    "med",      # mediaan robuust tegen sluis-operaties
        "prefix":  "flowbru_amont",
        "heeft_min": False,
        "factor":  0.001,      # mmTAW → mTAW
    },
    {
        "name":    "Canal Ecluse Anderlecht AVAL",
        "sid":     "9EF0952181A3B8AF",
        "channel": "ch1",
        "aggr":    "med",
        "prefix":  "flowbru_aval",
        "heeft_min": False,
        "factor":  0.001,
    },
    {
        "name":    "Canal Ecluse Anderlecht SAS",
        "sid":     "9EF0952181A3B8AF",
        "channel": "ch2",
        "aggr":    "med",
        "prefix":  "flowbru_sas",
        "heeft_min": False,
        "factor":  0.001,
    },
]

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
    """
    print(f"  → KMI weerdata ophalen: {start_datum} → {eind_datum} (station Ukkel)...")

    dt_start = datetime.fromisoformat(start_datum) - timedelta(days=1)
    dt_eind  = datetime.fromisoformat(eind_datum)  + timedelta(days=1)

    cql = (
        f"code IN ({KMI_STATION}) AND "
        f"timestamp DURING "
        f"{dt_start.strftime('%Y-%m-%dT00:00:00Z')}/"
        f"{dt_eind.strftime('%Y-%m-%dT00:00:00Z')}"
    )

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
    df["datum"] = (df["timestamp"] + pd.Timedelta(hours=1)).dt.date

    # Neerslag: som van precip_range=2 om 06:00 en 18:00 UTC
    df_r2 = df[
        (df["precip_range"] == 2) &
        (df["timestamp"].dt.hour.isin([6, 18]))
    ].copy()
    df_r2["precip_datum"] = df_r2.apply(
        lambda row: row["datum"] if row["timestamp"].hour == 18 else row["datum"],
        axis=1
    )
    neerslag = (
        df_r2.groupby("precip_datum")["precip_quantity"]
        .sum().reset_index()
        .rename(columns={"precip_datum": "datum", "precip_quantity": "neerslag_mm"})
    )
    neerslag["neerslag_mm"] = neerslag["neerslag_mm"].round(1)

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
    wind_max = (
        df.dropna(subset=["wind_peak_speed"])
        .groupby("datum")["wind_peak_speed"].max().round(1).reset_index()
        .rename(columns={"wind_peak_speed": "windsnelheid_max_kmh"})
    )
    wind_richting = (
        df[df["timestamp"].dt.hour == 18][["datum", "wind_direction"]]
        .dropna(subset=["wind_direction"])
        .groupby("datum")["wind_direction"].first().reset_index()
        .rename(columns={"wind_direction": "windrichting_graden"})
    )
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
        .mean().apply(lambda x: round(x * 12.5, 1)).reset_index()
        .rename(columns={"cloudiness": "bewolking_pct"})
    )
    zon_raw = df[
        (df["timestamp"].dt.hour == 0) &
        (df["sun_duration_24hours"].notna())
    ][["datum", "sun_duration_24hours"]].copy()
    zon_raw["datum"] = zon_raw["datum"].apply(lambda d: d - timedelta(days=1))
    zonneschijn = (
        zon_raw.groupby("datum")["sun_duration_24hours"]
        .first().apply(lambda s: round(s / 60, 2)).reset_index()
        .rename(columns={"sun_duration_24hours": "zonneschijn_uur"})
    )

    dagframes = [
        neerslag, temp_min, temp_max, temp_gem,
        wind_max, wind_richting, vochtigheid,
        luchtdruk, bewolking, zonneschijn,
    ]
    result = dagframes[0]
    for frame in dagframes[1:]:
        result = result.merge(frame, on="datum", how="outer")

    if "windrichting_graden" in result.columns:
        result["windrichting_naam"] = result["windrichting_graden"].apply(
            windrichting_naar_naam)

    start_d = date.fromisoformat(start_datum)
    eind_d  = date.fromisoformat(eind_datum)
    result = result[
        (result["datum"] >= start_d) & (result["datum"] <= eind_d)
    ].reset_index(drop=True)

    print(f"  ✓ KMI: {len(result)} dag(en) verwerkt.")
    return result


def haal_uv_index_op(start_datum: str, eind_datum: str) -> pd.DataFrame:
    """Haalt UV-index op via Open-Meteo (niet beschikbaar bij KMI)."""
    print(f"  → UV-index ophalen via Open-Meteo: {start_datum} → {eind_datum}...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":   LATITUDE,
        "longitude":  LONGITUDE,
        "daily":      ["uv_index_max"],
        "start_date": start_datum,
        "end_date":   eind_datum,
        "timezone":   "Europe/Brussels",
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        d = r.json()
        df = pd.DataFrame({
            "datum":        [pd.Timestamp(t).date() for t in d["daily"]["time"]],
            "uv_index_max": d["daily"]["uv_index_max"],
        })
        print(f"  ✓ UV-index: {len(df)} dag(en) verwerkt.")
        return df
    except Exception as e:
        print(f"  ! UV-index ophalen mislukt: {e}")
        return pd.DataFrame()


def bereken_cumulatieve_neerslag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("datum").reset_index(drop=True)
    # KMI-based cumulative (kept as reference)
    df["neerslag_24u_mm"] = df["neerslag_mm"].rolling(1, min_periods=1).sum().round(1)
    df["neerslag_48u_mm"] = df["neerslag_mm"].rolling(2, min_periods=1).sum().round(1)
    df["neerslag_72u_mm"] = df["neerslag_mm"].rolling(3, min_periods=1).sum().round(1)
    # Flowbru-based cumulative (local rain gauge at Anderlecht lock)
    if "flowbru_neerslag_dag" in df.columns:
        df["flowbru_neerslag_48u_mm"] = df["flowbru_neerslag_dag"].rolling(2, min_periods=1).sum().round(1)
        df["flowbru_neerslag_72u_mm"] = df["flowbru_neerslag_dag"].rolling(3, min_periods=1).sum().round(1)
    return df

# ─── MOW-HIC ─────────────────────────────────────────────────────────────────

def haal_hic_groepslijst(access_token: str) -> list:
    """Haalt de lijst van tijdreeksen op uit groep 3323277."""
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
    df["datum_uur"] = pd.to_datetime(df["datum_uur"], utc=True)
    df["waarde"]    = pd.to_numeric(df["waarde"], errors="coerce")
    return df


def aggregeer_naar_dag(df_uur: pd.DataFrame, prefix: str,
                       heeft_min: bool = False) -> pd.DataFrame:
    """Aggregeert uurdata naar dagmediaan (+ max, optioneel min).
    Mediaan is robuuster voor kanaaldata met lock-operaties."""
    df_uur = df_uur.copy()
    df_uur["datum"] = df_uur["datum_uur"].dt.date
    agg = df_uur.groupby("datum")["waarde"].agg(**{
        f"{prefix}_gem": "median",
        f"{prefix}_max": "max",
        **({f"{prefix}_min": "min"} if heeft_min else {})
    }).round(3).reset_index()
    return agg


def bepaal_prefix_en_min(ts_name: str, station_name: str,
                          parameter: str = "") -> tuple:
    """Bepaalt kolomprefix en heeft_min op basis van station_name en parameter.

    HIC groep 3323277 bevat 4 reeksen, alle met ts_name='Pv':
      - Ruisbroek/Kl Brussel-Charleroi + parameter=H  → kanaal_peil
      - Ruisbroek/Kl Brussel-Charleroi + parameter=Q  → kanaal_afvoer
      - Ruisbroek Sluis Opwaarts DVW/...  + parameter=H  → ruisbroek_opw_peil
      - Ruisbroek Sluis Afwaarts DVW/...  + parameter=H  → ruisbroek_afw_peil
    """
    station = (station_name or "").lower()
    param   = (parameter or "").upper()

    # Ruisbroek upstream/downstream sluis → altijd waterpeil
    if "opw" in station or "opwaarts" in station:
        return "ruisbroek_opw_peil", False
    if "afw" in station or "afwaarts" in station:
        return "ruisbroek_afw_peil", False

    # Hoofdkanaalstation: onderscheid H (level) vs Q (debiet/flow)
    if "ruisbroek" in station:
        if param == "Q":
            return "kanaal_afvoer", True   # heeft_min=True: negatief = omgekeerde stroming
        if param == "H":
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
        ts_id     = ts.get("ts_id")
        ts_name   = ts.get("ts_name", "")
        station   = ts.get("station_name", "")
        parameter = ts.get("parametertype_name", "")
        prefix, heeft_min = bepaal_prefix_en_min(ts_name, station, parameter)

        if not ts_id:
            continue
        if prefix is None:
            print(f"    → Onbekende tijdreeks overgeslagen: {station} / {ts_name}")
            continue

        print(f"    → {station} / {ts_name} ({parameter}) → {prefix}...")
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

# ─── FLOWBRU / HYDRIA ────────────────────────────────────────────────────────

def flowbru_auth_header() -> dict:
    """Geeft de Basic Auth header terug voor de Flowbru API."""
    token = base64.b64encode(
        f"{FLOWBRU_USER}:{FLOWBRU_PASS}".encode()
    ).decode()
    return {"Authorization": f"Basic {token}"}


def flowbru_timestamp(d: date) -> str:
    """Converteert een date naar Flowbru timestamp-formaat YYYYMMDDHHmm (UTC)."""
    return d.strftime("%Y%m%d0000")


def haal_flowbru_data_op(start_datum: str, eind_datum: str) -> pd.DataFrame:
    """
    Haalt dagelijks geaggregeerde data op van de Flowbru API voor alle
    geconfigureerde stations (sluis Anderlecht: waterstanden + neerslag).

    Aggregatie: 1day interval, methode per kanaal (med of sum_hr).
    Waterstanden worden van mmTAW naar mTAW omgezet (factor 0.001).
    Timestamps zijn UTC.
    """
    if not FLOWBRU_USER or not FLOWBRU_PASS:
        print("  → Geen Flowbru credentials gevonden, overgeslagen.")
        return pd.DataFrame()

    print(f"  → Flowbru data ophalen: {start_datum} → {eind_datum}...")
    headers = flowbru_auth_header()
    start_d = date.fromisoformat(start_datum)
    eind_d  = date.fromisoformat(eind_datum)

    resultaat = None

    for station in FLOWBRU_STATIONS:
        url = (f"{FLOWBRU_BASE}/customers/{FLOWBRU_CID}"
               f"/sites/{station['sid']}/histdata0/1day")
        body = {
            "select": [f"{station['channel']} {station['aggr']}"],
            "from":   flowbru_timestamp(start_d - timedelta(days=1)),
            "until":  flowbru_timestamp(eind_d  + timedelta(days=1)),
        }
        try:
            r = requests.get(
                url,
                params={"json": json_module.dumps(body)},
                headers=headers,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"    ! Flowbru ophalen mislukt voor {station['name']}: {e}")
            continue

        if not data or not isinstance(data, list):
            print(f"    ! Geen data voor {station['name']}")
            continue

        rows = []
        for entry in data:
            # Flowbru retourneert [timestamp_str, waarde]
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            ts_str = str(entry[0])
            val    = entry[1]
            if val is None:
                continue
            try:
                # Timestamp formaat: YYYYMMDDHHmmssSSS (trailing zeros weggelaten)
                # Pad to at least 12 chars (YYYYMMDDHHmm)
                ts_padded = ts_str.ljust(12, "0")
                ts = datetime.strptime(ts_padded[:12], "%Y%m%d%H%M")
                datum = ts.date()
                waarde = float(val) * station["factor"]
                rows.append({"datum": datum, "waarde": waarde})
            except Exception:
                continue

        if not rows:
            print(f"    ! Geen bruikbare rijen voor {station['name']}")
            continue

        df = pd.DataFrame(rows)
        df = df[(df["datum"] >= start_d) & (df["datum"] <= eind_d)]

        if df.empty:
            continue

        col = f"{station['prefix']}_dag"
        df = df.rename(columns={"waarde": col})
        df[col] = df[col].round(3)

        print(f"    ✓ {station['name']}: {len(df)} dag(en)")
        resultaat = (df if resultaat is None
                     else resultaat.merge(df, on="datum", how="outer"))

    return resultaat if resultaat is not None else pd.DataFrame()


# Drempelwaarden voor sluisactiviteit-detectie
SLUIS_NABIJHEID_M   = 0.30   # max afstand (m) tot amont/aval-niveau om als "vol"/"leeg" te tellen
SLUIS_MAX_DUUR_MIN  = 30     # max duur (minuten) van een geldige vul/leeg-beweging
SLUIS_MIN_DUUR_MIN  = 3      # min duur (minuten) — sluit ruis/spikes uit


def bereken_sluis_activiteit(start_datum: str, eind_datum: str,
                              amont_niveaus: dict = None,
                              aval_niveaus: dict = None) -> pd.DataFrame:
    """
    Haalt 5-minuten sas-data op van de sluis Anderlecht (Flowbru) en telt
    het aantal echte sluisbewegingen per dag.

    Een geldige sluisbeweging (fill of empty) moet:
      1. Starten dicht bij het ene niveau (aval of amont) en eindigen dicht
         bij het andere niveau (binnen SLUIS_NABIJHEID_M), dus een VOLLEDIGE
         doorgang — geen halfweg stoppende bewegingen.
      2. Plaatsvinden binnen een realistische tijdspanne
         (SLUIS_MIN_DUUR_MIN tot SLUIS_MAX_DUUR_MIN minuten) — sluit trage,
         uren durende vul-bewegingen (lege sluis die zich vult zonder schip)
         en korte ruis-spikes uit.

    amont_niveaus/aval_niveaus: optionele dicts {datum: niveau_m} met de
    dagelijkse amont/aval-niveaus. Indien niet gegeven, wordt de min/max
    van de sas-data zelf als referentie gebruikt (minder nauwkeurig).

    Retourneert DataFrame met kolommen 'datum' en 'sluis_activiteit_dag'.
    """
    if not FLOWBRU_USER or not FLOWBRU_PASS:
        return pd.DataFrame()

    print(f"  → Sluis activiteit ophalen (5-min sas data): {start_datum} → {eind_datum}...")
    headers = flowbru_auth_header()
    start_d = date.fromisoformat(start_datum)
    eind_d  = date.fromisoformat(eind_datum)

    # Sas = SID 9EF0952181A3B8AF, channel ch2 (raw, geen aggregatie)
    url = (f"{FLOWBRU_BASE}/customers/{FLOWBRU_CID}"
           f"/sites/9EF0952181A3B8AF/histdata0")
    body = {
        "select": ["ch2"],
        "from":   flowbru_timestamp(start_d),
        "until":  flowbru_timestamp(eind_d + timedelta(days=1)),
    }

    try:
        r = requests.get(
            url,
            params={"json": json_module.dumps(body)},
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ! Flowbru sas 5-min ophalen mislukt: {e}")
        return pd.DataFrame()

    if not data or not isinstance(data, list):
        print("  ! Geen sas 5-min data ontvangen.")
        return pd.DataFrame()

    rows = []
    for entry in data:
        if not isinstance(entry, list) or len(entry) < 2 or entry[1] is None:
            continue
        try:
            ts_str    = str(entry[0]).ljust(12, "0")
            ts        = datetime.strptime(ts_str[:12], "%Y%m%d%H%M")
            waarde    = float(entry[1]) * 0.001  # mmTAW → mTAW
            rows.append({"ts": ts, "datum": ts.date(), "sas": waarde})
        except Exception:
            continue

    if not rows:
        print("  ! Geen bruikbare sas 5-min rijen.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df[(df["datum"] >= start_d) & (df["datum"] <= eind_d)]

    if df.empty:
        return pd.DataFrame()

    amont_niveaus = amont_niveaus or {}
    aval_niveaus  = aval_niveaus or {}

    resultaten = []
    for dag, groep in df.groupby("datum"):
        groep = groep.sort_values("ts").reset_index(drop=True)
        vals  = groep["sas"].values
        tijden = groep["ts"].values

        if len(vals) < 4:
            resultaten.append({"datum": dag, "sluis_activiteit_dag": None})
            continue

        # Referentieniveaus: amont (hoog) en aval (laag) van die dag,
        # of bij ontbreken: min/max van de sas-reeks zelf als benadering.
        hoog_ref = amont_niveaus.get(dag, vals.max())
        laag_ref = aval_niveaus.get(dag, vals.min())
        if hoog_ref < laag_ref:
            hoog_ref, laag_ref = laag_ref, hoog_ref

        # Binair signaal: 1 = dicht bij amont (vol), 0 = dicht bij aval (leeg),
        # NaN/onbeslist = ergens in het midden (telt niet als extreem bereikt)
        signaal = []
        for v in vals:
            if abs(v - hoog_ref) <= SLUIS_NABIJHEID_M:
                signaal.append(1)
            elif abs(v - laag_ref) <= SLUIS_NABIJHEID_M:
                signaal.append(0)
            else:
                signaal.append(None)

        # Loop door de reeks: zoek opeenvolgende periodes met bevestigde staat
        # (0 of 1), en tel een overgang als er tussen twee bevestigde periodes
        # met verschillende staat een wissel plaatsvindt binnen de toegestane tijd.
        bevestigde_idx = [i for i, s in enumerate(signaal) if s is not None]
        overgangen = 0
        for j in range(1, len(bevestigde_idx)):
            i_prev = bevestigde_idx[j - 1]
            i_curr = bevestigde_idx[j]
            if signaal[i_prev] == signaal[i_curr]:
                continue  # zelfde staat, geen overgang

            # Bereken duur van de overgang in minuten
            t_prev = pd.Timestamp(tijden[i_prev])
            t_curr = pd.Timestamp(tijden[i_curr])
            duur_min = (t_curr - t_prev).total_seconds() / 60.0

            if SLUIS_MIN_DUUR_MIN <= duur_min <= SLUIS_MAX_DUUR_MIN:
                overgangen += 1
            # Buiten dit bereik: te traag (vullen zonder schip) of te snel
            # (ruis) — telt niet mee.

        resultaten.append({"datum": dag, "sluis_activiteit_dag": overgangen})

    result = pd.DataFrame(resultaten)
    totaal = result["sluis_activiteit_dag"].sum()
    print(f"  ✓ Sluis activiteit: {len(result)} dag(en), {totaal} bewegingen totaal "
          f"(nabijheid ≤{SLUIS_NABIJHEID_M}m, duur {SLUIS_MIN_DUUR_MIN}-{SLUIS_MAX_DUUR_MIN}min).")
    return result


def haal_flowbru_neerslag_op(start_datum: str, eind_datum: str) -> pd.DataFrame:
    """
    Haalt ruwe 5-minuten neerslagdata op van de pluviometer aan de sluis
    Anderlecht (P07, SID 9DB48CE145EB1F26, ch2) en sommeert per dag.
    
    De ruwe waarden zijn in mm/5min — directe som geeft dagelijkse mm.
    """
    if not FLOWBRU_USER or not FLOWBRU_PASS:
        return pd.DataFrame()

    print(f"  → Flowbru neerslag ophalen (5-min P07): {start_datum} → {eind_datum}...")
    headers = flowbru_auth_header()
    start_d = date.fromisoformat(start_datum)
    eind_d  = date.fromisoformat(eind_datum)

    url = (f"{FLOWBRU_BASE}/customers/{FLOWBRU_CID}"
           f"/sites/9DB48CE145EB1F26/histdata0")
    body = {
        "select": ["ch2"],
        "from":   flowbru_timestamp(start_d),
        "until":  flowbru_timestamp(eind_d + timedelta(days=1)),
    }

    try:
        r = requests.get(
            url,
            params={"json": json_module.dumps(body)},
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ! Flowbru neerslag ophalen mislukt: {e}")
        return pd.DataFrame()

    if not data or not isinstance(data, list):
        print("  ! Geen P07 neerslagdata ontvangen.")
        return pd.DataFrame()

    rows = []
    for entry in data:
        if not isinstance(entry, list) or len(entry) < 2 or entry[1] is None:
            continue
        try:
            ts_str = str(entry[0]).ljust(12, "0")
            ts     = datetime.strptime(ts_str[:12], "%Y%m%d%H%M")
            val    = float(entry[1])
            rows.append({"datum": ts.date(), "mm": val})
        except Exception:
            continue

    if not rows:
        print("  ! Geen bruikbare P07 rijen.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df[(df["datum"] >= start_d) & (df["datum"] <= eind_d)]

    if df.empty:
        return pd.DataFrame()

    # Som per dag — directe optelling van mm/5min waarden
    result = (
        df.groupby("datum")["mm"]
        .sum().round(1).reset_index()
        .rename(columns={"mm": "flowbru_neerslag_dag"})
    )
    print(f"  ✓ Flowbru neerslag: {len(result)} dag(en) verwerkt.")
    return result

# ─── BLUERIIOT (waterthermometer sluis Anderlecht) ──────────────────────────
#
# Onofficiële API, gereverse-engineered door de community (geen officiële
# publieke documentatie van RiiotLabs/Blueriiot beschikbaar).
# Gebaseerd op: https://github.com/marcelveldt/python-blueconnect
#           en: https://github.com/DannyRuijters/domoticz_blueconnect
#
# Login (POST /prod/user/login) geeft een JWT-token EN tijdelijke AWS-
# credentials (access_key, secret_key, session_token) terug. Vervolgens
# moeten alle requests naar /prod/* cryptografisch ondertekend worden met
# AWS Signature Version 4 (SigV4) — een simpele Bearer-token in de header
# volstaat niet, dat geeft telkens een "Authorization header requires..."
# fout terug van AWS API Gateway.

BLUERIIOT_USER = os.environ.get("BLUERIIOT_USER", "")
BLUERIIOT_PASS = os.environ.get("BLUERIIOT_PASS", "")
BLUERIIOT_BASE = "https://api.riiotlabs.com/prod"
BLUERIIOT_REGION = "eu-west-1"
# Vroege-ochtendvenster (lokale tijd) waarin de watertemperatuurmeting
# gebruikt wordt — koudste moment van de dag, voordat de scheepsromp
# (waaraan de sensor bevestigd is) opwarmt in de zon.
BLUERIIOT_OCHTEND_START = 4   # uur (inclusief)
BLUERIIOT_OCHTEND_EIND  = 8   # uur (exclusief)


def haal_blueriiot_credentials():
    """
    Logt in bij de Blueriiot/RiiotLabs API en geeft een dict terug met
    de tijdelijke AWS-credentials nodig voor het ondertekenen van
    volgende requests (SigV4), of None bij falen.
    """
    if not BLUERIIOT_USER or not BLUERIIOT_PASS:
        print("  → Geen Blueriiot credentials gevonden, overgeslagen.")
        return None

    try:
        r = requests.post(
            f"{BLUERIIOT_BASE}/user/login",
            json={"email": BLUERIIOT_USER, "password": BLUERIIOT_PASS},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        creds = data.get("credentials")
        if not creds:
            print("  ! Blueriiot login: geen credentials in response.")
            return None
        return creds
    except Exception as e:
        print(f"  ! Blueriiot login mislukt: {e}")
        return None


def blueriiot_signed_get(path: str, creds: dict, params: dict = None):
    """Voert een GET-request uit op de Blueriiot API, correct
    ondertekend met AWS SigV4 aan de hand van de tijdelijke credentials.

    Gebaseerd op de werkwijze in het officiële python-blueconnect pakket:
    naast de gesigneerde headers moet ook 'X-Amz-Security-Token' apart
    meegegeven worden. Bovendien moet de query string al in de URL zitten
    VOORDAT deze ondertekend wordt — anders komt de handtekening niet
    overeen met de effectief verstuurde request (geeft 403 Forbidden)."""
    signer = AwsRequestSigner(
        region=BLUERIIOT_REGION,
        access_key_id=creds["access_key"],
        secret_access_key=creds["secret_key"],
        service="execute-api",
    )
    url = f"{BLUERIIOT_BASE}{path}"
    if params:
        query_string = "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
        url = f"{url}?{query_string}"

    base_headers = {
        "User-Agent": "BlueConnect/3.2.1",
        "Accept": "*/*",
    }
    headers = base_headers.copy()
    headers.update(signer.sign_with_headers("GET", url, base_headers))
    headers["X-Amz-Security-Token"] = creds["session_token"]
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def haal_blueriiot_watertemp_op() -> dict:
    """
    Haalt de laatste watertemperatuurmeting op van de Blueriiot-sensor
    in de sluis Anderlecht. Geeft een dict terug met 'datum' en
    'ss_watertemp_c', of een lege dict bij falen/geen data.

    Endpoint bevestigd via het officiële python-blueconnect pakket:
    swimming_pool/{pool_id}/blue/{blue_device_serial}/lastMeasurements
    ?mode=blue_and_strip

    De sensor meet ongeveer om de 72 minuten, dus dit haalt telkens
    enkel de meest recente meting op (geen historiek/aggregatie nodig
    zoals bij Flowbru/HIC).
    """
    creds = haal_blueriiot_credentials()
    if not creds:
        return {}

    print("  → Blueriiot watertemperatuur ophalen...")
    try:
        pools_response = blueriiot_signed_get("/swimming_pool", creds)
        pools = pools_response.get("data") if isinstance(pools_response, dict) else pools_response
        if not pools or not isinstance(pools, list):
            print("  ! Geen zwembaden/pools gevonden op Blueriiot-account.")
            return {}

        # Neem de eerste (en voor ons enige) pool — "Canal"
        eerste = pools[0]
        pool_id = (
            eerste.get("swimming_pool", {}).get("swimming_pool_id")
            or eerste.get("swimming_pool_id")
        )
        if not pool_id:
            print(f"  ! Kon geen pool_id vinden in: {eerste}")
            return {}

        blue_response = blueriiot_signed_get(
            f"/swimming_pool/{pool_id}/blue", creds)
        blue_devices = blue_response.get("data") if isinstance(blue_response, dict) else blue_response
        if not blue_devices or not isinstance(blue_devices, list):
            print("  ! Geen Blue-sensor gevonden op deze pool.")
            return {}

        blue_key = blue_devices[0].get("blue_device_serial")  # bv. "01FC3E02"
        if not blue_key:
            print(f"  ! Kon geen blue_device_serial vinden in: {blue_devices[0]}")
            return {}

        meas_response = blueriiot_signed_get(
            f"/swimming_pool/{pool_id}/blue/{blue_key}/lastMeasurements",
            creds,
            params={"mode": "blue_and_strip"},
        )
        # (debug print verwijderd — integratie bevestigd werkend)

        metingen = meas_response.get("data") or []
        if not metingen:
            print("  ! Geen metingen gevonden in lastMeasurements response.")
            return {}

        def vind_meting(naam_bevat):
            return next(
                (m for m in metingen if naam_bevat in (m.get("name") or "").lower()),
                None
            )

        temp_meting = vind_meting("temp")
        ph_meting   = vind_meting("ph")
        cond_meting = vind_meting("conductiv")

        if not temp_meting:
            print(f"  ! Geen temperatuurmeting gevonden. Beschikbare metingen: "
                  f"{[m.get('name') for m in metingen]}")
            return {}

        temp = temp_meting.get("value")
        gemeten_op = (temp_meting.get("timestamp")
                      or meas_response.get("last_blue_measure_timestamp"))

        if temp is None:
            print("  ! Geen temperatuurwaarde in meting.")
            return {}

        if not gemeten_op:
            print("  ! Geen tijdstip bekend voor meting, overgeslagen.")
            return {}

        gemeten_dt = pd.to_datetime(gemeten_op)
        # Omzetten naar lokale tijd (Brussel, UTC+1/+2) voor het uurfilter
        gemeten_lokaal = gemeten_dt.tz_convert("Europe/Brussels") if gemeten_dt.tzinfo else gemeten_dt

        # Enkel vroege-ochtendmetingen gebruiken (koudste water, voor zonopwarming
        # van de scheepsromp waar de sensor aan bevestigd is). Venster: 4u-8u lokaal.
        if not (BLUERIIOT_OCHTEND_START <= gemeten_lokaal.hour < BLUERIIOT_OCHTEND_EIND):
            print(f"  → Blueriiot: laatste meting om {gemeten_lokaal.strftime('%H:%M')} "
                  f"lokale tijd valt buiten het vroege-ochtendvenster "
                  f"({BLUERIIOT_OCHTEND_START}u-{BLUERIIOT_OCHTEND_EIND}u) — overgeslagen.")
            return {}

        datum = gemeten_lokaal.date()

        resultaat = {
            "datum": datum,
            "tijd":  gemeten_lokaal.strftime("%H:%M"),
            "ss_watertemp_c": round(float(temp), 1),
        }
        if ph_meting and ph_meting.get("value") is not None:
            resultaat["ss_ph"] = round(float(ph_meting["value"]), 2)
        if cond_meting and cond_meting.get("value") is not None:
            resultaat["ss_conductivity"] = round(float(cond_meting["value"]), 0)

        print(f"  ✓ Blueriiot: {temp}°C (vroege ochtend, gemeten: "
              f"{gemeten_lokaal.strftime('%Y-%m-%d %H:%M')} lokale tijd)")
        return resultaat

    except Exception as e:
        print(f"  ! Blueriiot ophalen mislukt: {e}")
        return {}


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
    """
    Bepaalt vanaf welke datum nieuwe weerdata opgehaald moet worden.

    LET OP: kijkt niet enkel naar de laatst aanwezige datum-rij, maar naar
    de laatste datum met daadwerkelijk ingevulde KMI-weerdata
    (temp_gemiddeld_c). Reden: de Blueriiot-stap kan een datum toevoegen
    die verder volledig leeg is (enkel ss_watertemp_c ingevuld). Als we dan
    alleen naar 'aanwezige datum' zouden kijken, denkt het script dat die
    dag al 'klaar' is en slaat de echte weer-/kanaaldata voor die dag
    permanent over.
    """
    vandaag   = date.today()
    gisteren  = vandaag - timedelta(days=1)
    max_terug = vandaag - timedelta(days=89)

    if bestaande_df.empty:
        return str(max_terug), str(gisteren)

    if "temp_gemiddeld_c" in bestaande_df.columns:
        volledige_rijen = bestaande_df[bestaande_df["temp_gemiddeld_c"].notna()]
    else:
        volledige_rijen = bestaande_df

    if volledige_rijen.empty:
        # Nog geen enkele dag met echte weerdata — begin van vooraf
        return str(max_terug), str(gisteren)

    laatste  = volledige_rijen["datum"].max()
    volgende = laatste + timedelta(days=1)

    if volgende > gisteren:
        return None, None

    return str(volgende), str(gisteren)


def exporteer_json(df: pd.DataFrame, alle_metingen: pd.DataFrame = None,
                    watertemperatuur: dict = None):
    """Exporteert de dataset naar data/sunday_swims_data.json.

    'watertemperatuur' is een LOSSTAANDE, actuele meting (net als
    'voorspelling') — onafhankelijk van de datumlogica van 'data'. Dit
    voorkomt dat de weergave van de actuele watertemperatuur afhangt van
    of de rest van de dagrij (weer/kanaal) al compleet is."""
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

    try:
        voorspelling = haal_voorspelling_op()
    except Exception as e:
        print(f"  ! Weersvoorspelling ophalen mislukt: {e} — lege voorspelling gebruikt.")
        voorspelling = []

    output = {
        "gegenereerd_op": str(date.today()),
        "locatie": {
            "naam": "Kanaal Brussel-Charleroi, Anderlecht",
            "lat":  LATITUDE,
            "lon":  LONGITUDE,
        },
        "data":             records,
        "metingen":         metingen_records,
        "voorspelling":     voorspelling,
        "watertemperatuur": watertemperatuur,
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

        # Stap 4: UV-index ophalen via Open-Meteo
        uv_df = haal_uv_index_op(start, eind)
        if not uv_df.empty:
            uv_df["datum"] = pd.to_datetime(uv_df["datum"]).apply(
                lambda x: x.date() if hasattr(x, 'date') else x)
            nieuwe_df = nieuwe_df.merge(uv_df, on="datum", how="left")

        # Stap 5: HIC kanaaldata ophalen via groep 3323277
        if access_token:
            hic_df = haal_alle_hic_data_op(start, eind, access_token)
            if hic_df is not None:
                hic_df["datum"] = pd.to_datetime(hic_df["datum"]).dt.date
                nieuwe_df = nieuwe_df.merge(hic_df, on="datum", how="left")
        else:
            print("  → HIC kanaaldata overgeslagen (geen geldig access token).")

        # Stap 6: Flowbru sluisdata en neerslag ophalen
        flowbru_df = haal_flowbru_data_op(start, eind)
        if not flowbru_df.empty:
            flowbru_df["datum"] = pd.to_datetime(flowbru_df["datum"]).apply(
                lambda x: x.date() if hasattr(x, 'date') else x)
            nieuwe_df = nieuwe_df.merge(flowbru_df, on="datum", how="left")

        # Stap 6b: Flowbru neerslag ophalen (5-min P07, som per dag)
        neerslag_df = haal_flowbru_neerslag_op(start, eind)
        if not neerslag_df.empty:
            neerslag_df["datum"] = pd.to_datetime(neerslag_df["datum"]).apply(
                lambda x: x.date() if hasattr(x, 'date') else x)
            # Overschrijf de flowbru_neerslag_dag kolom die eerder via haal_flowbru_data_op
            # werd aangemaakt (was leeg na verwijdering uit FLOWBRU_STATIONS)
            if "flowbru_neerslag_dag" in nieuwe_df.columns:
                nieuwe_df = nieuwe_df.drop(columns=["flowbru_neerslag_dag"])
            nieuwe_df = nieuwe_df.merge(neerslag_df, on="datum", how="left")

        # Stap 6c: Sluis activiteit ophalen (5-min sas bewegingen tellen)
        # Gebruik de amont/aval-dagniveaus als referentie voor "vol"/"leeg"
        amont_lookup = {}
        aval_lookup  = {}
        if "flowbru_amont_dag" in nieuwe_df.columns:
            amont_lookup = (
                nieuwe_df.dropna(subset=["flowbru_amont_dag"])
                .set_index("datum")["flowbru_amont_dag"].to_dict()
            )
        if "flowbru_aval_dag" in nieuwe_df.columns:
            aval_lookup = (
                nieuwe_df.dropna(subset=["flowbru_aval_dag"])
                .set_index("datum")["flowbru_aval_dag"].to_dict()
            )
        activiteit_df = bereken_sluis_activiteit(start, eind, amont_lookup, aval_lookup)
        if not activiteit_df.empty:
            activiteit_df["datum"] = pd.to_datetime(activiteit_df["datum"]).apply(
                lambda x: x.date() if hasattr(x, 'date') else x)
            nieuwe_df = nieuwe_df.merge(activiteit_df, on="datum", how="left")

    # Stap 7: Samenvoegen met bestaande data
    # keep="last": bij een datum die in beide zit, wint nieuwe_df (opnieuw
    # opgehaalde, volledige data) van bestaande_df (kan een onvolledige
    # Blueriiot-only placeholderrij zijn van een eerdere run).
    if not bestaande_df.empty and not nieuwe_df.empty:
        gecombineerd = (
            pd.concat([bestaande_df, nieuwe_df], ignore_index=True)
            .drop_duplicates(subset=["datum"], keep="last")
            .sort_values("datum")
            .reset_index(drop=True)
        )
    elif not nieuwe_df.empty:
        gecombineerd = nieuwe_df
    else:
        gecombineerd = bestaande_df

    # Stap 8: Cumulatieve neerslag herberekenen over volledige dataset
    gecombineerd = bereken_cumulatieve_neerslag(gecombineerd)

    # Stap 9: Handmatige metingen samenvoegen
    metingen = laad_metingen()
    if not metingen.empty:
        handmatige_kolommen = [c for c in metingen.columns if c != "datum"]
        for k in handmatige_kolommen:
            if k in gecombineerd.columns:
                gecombineerd = gecombineerd.drop(columns=[k])
        gecombineerd = gecombineerd.merge(metingen, on="datum", how="left")

    # Stap 9b: Blueriiot watertemperatuur ophalen.
    # Twee dingen gebeuren met deze meting:
    #   1. Ze wordt (zoals voorheen) in 'gecombineerd' gemerged op datum — maar
    #      in een APARTE kolom 'ss_watertemp_blueriiot_c', naast de bestaande
    #      'ss_watertemp_c' (die gevuld wordt door handmatige metingen uit
    #      metingen.csv). Zo bestaan beide bronnen naast elkaar op dezelfde
    #      dag zonder dat de ene de andere overschrijft — nodig omdat de
    #      Blueriiot-lezing (elke ochtend automatisch) en een eventuele
    #      handmatige meting (samen met waterkwaliteitsstalen) onafhankelijk
    #      van elkaar plaatsvinden en beide zichtbaar moeten blijven.
    #   2. Ze wordt DAARNAAST ook als losstaand object opgeslagen (net als
    #      'voorspelling'), zodat de website voor de ACTUELE temperatuurweergave
    #      in de 'swimming conditions'-tegel altijd en enkel de Blueriiot-
    #      meting toont, nooit afhankelijk van of de rest van de dagrij
    #      (weer/kanaal) al compleet is.
    watertemperatuur_actueel = None
    blue_meting = haal_blueriiot_watertemp_op()
    if blue_meting:
        blue_datum = blue_meting["datum"]
        blue_temp  = blue_meting["ss_watertemp_c"]

        watertemperatuur_actueel = {
            "datum":    str(blue_datum),
            "tijd":     blue_meting.get("tijd"),
            "waarde_c": blue_temp,
        }
        if "ss_ph" in blue_meting:
            watertemperatuur_actueel["ph"] = blue_meting["ss_ph"]
        if "ss_conductivity" in blue_meting:
            watertemperatuur_actueel["conductivity"] = blue_meting["ss_conductivity"]

        # Normaliseer beide kanten naar datetime.date vóór vergelijking —
        # gecombineerd["datum"] kan hier nog string, Timestamp of date zijn
        # afhankelijk van eerdere verwerkingsstappen; een directe 'in'-check
        # op gemengde types faalt stil en creëert dan een lege duplicaatrij.
        if "ss_watertemp_blueriiot_c" not in gecombineerd.columns:
            gecombineerd["ss_watertemp_blueriiot_c"] = None

        datum_genormaliseerd = pd.to_datetime(gecombineerd["datum"]).dt.date

        match_mask = datum_genormaliseerd == blue_datum
        if match_mask.any():
            gecombineerd.loc[match_mask, "ss_watertemp_blueriiot_c"] = blue_temp
            print(f"  → Blueriiot temperatuur ({blue_temp}°C) toegevoegd aan "
                  f"bestaande rij voor {blue_datum} (kolom ss_watertemp_blueriiot_c).")
        else:
            nieuwe_rij = pd.DataFrame([{"datum": blue_datum,
                                         "ss_watertemp_blueriiot_c": blue_temp}])
            gecombineerd = pd.concat([gecombineerd, nieuwe_rij],
                                      ignore_index=True)
            print(f"  → Blueriiot temperatuur ({blue_temp}°C) als nieuwe rij "
                  f"toegevoegd voor {blue_datum} (kolom ss_watertemp_blueriiot_c).")

        gecombineerd = gecombineerd.sort_values("datum").reset_index(drop=True)

    # Stap 10: Exporteren
    exporteer_json(gecombineerd, metingen, watertemperatuur_actueel)

    print(f"\n  ✓ Klaar! Totaal: {len(gecombineerd)} dagen in dataset.\n")


if __name__ == "__main__":
    main()
