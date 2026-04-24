"""
scripts/update_data.py  —  v12
=======================================================================
NEUE LOGIK:
- "Annual breakdown by powertrain" enthält NUR volle vergangene Jahre
  (bis current_year - 1).
- Das laufende Jahr (z. B. 2026) hat KEINE festen Market-Share-% mehr.
- Stattdessen neue Sektion "ytd" mit kumulierten Monatsdaten
  (Jan–März → später Jan–April usw.).
"""

import csv, io, json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

# NEU für Chart + Telegram-Bild
import matplotlib.pyplot as plt
import tempfile
import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
REPO_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR       = REPO_ROOT / "data" / "countries"
NOW            = datetime.now(timezone.utc)

COUNTRIES = {
    "AT": ("Austria",         "AT",  9.1),
    "BE": ("Belgium",         "BE", 11.6),
    "BG": ("Bulgaria",        "BG",  6.5),
    "CY": ("Cyprus",          "CY",  1.2),
    "CZ": ("Czech Republic",  "CZ", 10.9),
    "DE": ("Germany",         "DE", 84.4),
    "DK": ("Denmark",         "DK",  5.9),
    "EE": ("Estonia",         "EE",  1.4),
    "EL": ("Greece",          "EL", 10.4),
    "ES": ("Spain",           "ES", 47.4),
    "FI": ("Finland",         "FI",  5.6),
    "FR": ("France",          "FR", 68.1),
    "HR": ("Croatia",         "HR",  3.9),
    "HU": ("Hungary",         "HU",  9.7),
    "IE": ("Ireland",         "IE",  5.1),
    "IT": ("Italy",           "IT", 59.1),
    "LT": ("Lithuania",       "LT",  2.8),
    "LU": ("Luxembourg",      "LU",  0.7),
    "LV": ("Latvia",          "LV",  1.8),
    "MT": ("Malta",           "MT",  0.5),
    "NL": ("Netherlands",     "NL", 17.9),
    "PL": ("Poland",          "PL", 37.6),
    "PT": ("Portugal",        "PT", 10.3),
    "RO": ("Romania",         "RO", 19.0),
    "SE": ("Sweden",          "SE", 10.5),
    "SI": ("Slovenia",        "SI",  2.1),
    "SK": ("Slovakia",        "SK",  5.5),
    "NO": ("Norway",          "NO",  5.5),
    "CH": ("Switzerland",     "CH",  8.8),
    "GB": ("United Kingdom",  "UK", 67.4),
}

# ACEA exact annual totals 2023-2025 (verified from official press releases)
ACEA_TOTALS = {
    "AT": {2023: 239150, 2024: 253789, 2025: 284978},
    "BE": {2023: 476675, 2024: 448277, 2025: 414770},
    "BG": {2023: 37724,  2024: 42941,  2025: 49419},
    "CY": {2023: 14740,  2024: 15057,  2025: 14634},
    "CZ": {2023: 221419, 2024: 231597, 2025: 248719},
    "DE": {2023: 2844609,2024: 2817331,2025: 2857591},
    "DK": {2023: 172745, 2024: 173114, 2025: 184641},
    "EE": {2023: 22820,  2024: 25386,  2025: 13055},
    "EL": {2023: 134484, 2024: 137075, 2025: 144199},
    "ES": {2023: 949362, 2024: 1016885,2025: 1148650},
    "FI": {2023: 87502,  2024: 74064,  2025: 71881},
    "FR": {2023: 1774722,2024: 1718412,2025: 1632152},
    "HR": {2023: 57694,  2024: 65020,  2025: 69841},
    "HU": {2023: 107720, 2024: 121611, 2025: 129440},
    "IE": {2023: 122400, 2024: 121196, 2025: 124954},
    "IT": {2023: 1567151,2024: 1559229,2025: 1524843},
    "LT": {2023: 27666,  2024: 30122,  2025: 41974},
    "LU": {2023: 49105,  2024: 46659,  2025: 47158},
    "LV": {2023: 18928,  2024: 17329,  2025: 22506},
    "MT": {2023: 7436,   2024: 7663,   2025: 6468},
    "NL": {2023: 369631, 2024: 381227, 2025: 388024},
    "PL": {2023: 475032, 2024: 551568, 2025: 597435},
    "PT": {2023: 199623, 2024: 209715, 2025: 225039},
    "RO": {2023: 143080, 2024: 151105, 2025: 156803},
    "SE": {2023: 289820, 2024: 269582, 2025: 272998},
    "SI": {2023: 48924,  2024: 53018,  2025: 57556},
    "SK": {2023: 88003,  2024: 93409,  2025: 93103},
    "NO": {2023: 126953, 2024: 128687, 2025: 179632},
    "CH": {2023: 252214, 2024: 239535, 2025: 233737},
    "GB": {2023: 1903054,2024: 1952778,2025: 2020523},
}

# ACEA verified BEV % full-year 2025
ACEA_BEV_2025 = {
    "AT": 21.3, "BE": 34.7, "BG": 4.9,  "CY": 10.1, "CZ": 5.6,
    "DE": 19.1, "DK": 68.5, "EE": 6.6,  "EL": 6.2,  "ES": 8.8,
    "FI": 37.2, "FR": 20.0, "HR": 1.8,  "HU": 8.5,  "IE": 18.9,
    "IT": 6.2,  "LT": 7.5,  "LU": 26.9, "LV": 7.1,  "MT": 37.9,
    "NL": 40.2, "PL": 7.2,  "PT": 23.2, "RO": 5.6,  "SE": 36.5,
    "SI": 11.2, "SK": 4.7,  "NO": 95.9, "CH": 22.8, "GB": 23.4,
}

FUEL_MAP_GRANULAR = {
    "ELC": "bev", "ELC_PET_PI": "phev", "ELC_DIE_PI": "phev",
    "ELC_PET_HYB": "hybrid", "ELC_DIE_HYB": "hybrid",
    "PET_X_HYB": "petrol", "DIE_X_HYB": "diesel",
    "LPG": "other", "GAS": "other", "HYD_FCELL": "other",
    "BIOETH": "other", "BIODIE": "other", "BIFUEL": "other", "OTH": "other",
}
FUEL_MAP_FALLBACK = {
    "ELC": "bev", "PET": "petrol", "DIE": "diesel",
    "LPG": "other", "GAS": "other", "OTH": "other",
}

# Monthly seasonal weights (sum = 1.0) — typical EU pattern
SEASON_DEFAULT = {
    "01": 0.070, "02": 0.065, "03": 0.120, "04": 0.080,
    "05": 0.090, "06": 0.095, "07": 0.075, "08": 0.065,
    "09": 0.100, "10": 0.090, "11": 0.080, "12": 0.070,
}
# Country-specific seasonal patterns
SEASON_OVERRIDE = {
    "NO": {"01":0.065,"02":0.060,"03":0.130,"04":0.075,"05":0.095,"06":0.090,
           "07":0.060,"08":0.055,"09":0.110,"10":0.095,"11":0.085,"12":0.080},
    "DK": {"01":0.060,"02":0.060,"03":0.130,"04":0.085,"05":0.095,"06":0.090,
           "07":0.065,"08":0.060,"09":0.105,"10":0.095,"11":0.080,"12":0.075},
    "DE": {"01":0.068,"02":0.062,"03":0.118,"04":0.079,"05":0.091,"06":0.098,
           "07":0.074,"08":0.062,"09":0.102,"10":0.091,"11":0.082,"12":0.073},
    "SE": {"01":0.068,"02":0.063,"03":0.122,"04":0.081,"05":0.092,"06":0.097,
           "07":0.073,"08":0.062,"09":0.101,"10":0.090,"11":0.081,"12":0.070},
    "FR": {"01":0.072,"02":0.066,"03":0.118,"04":0.079,"05":0.089,"06":0.096,
           "07":0.076,"08":0.064,"09":0.099,"10":0.091,"11":0.081,"12":0.069},
}

PARTIAL_2026_MONTHS = ["01", "02", "03"]

def http_get(url, timeout=30):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "EV-Map-Bot/12.0 (github.com/Altair02/EV-adoption-worldmap)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def parse_csv(raw):
    result = {}
    try:
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            row_u  = {k.upper(): v for k, v in row.items()}
            period = row_u.get("TIME_PERIOD", "").strip()
            value  = row_u.get("OBS_VALUE",   "").strip()
            if period and value:
                try:
                    v = int(float(value))
                    if v > 0:
                        result[period] = v
                except ValueError:
                    pass
    except Exception:
        pass
    return result

def ecb_fetch(dataset, key_tmpl, geo, start):
    key = key_tmpl.replace("XX", geo)
    url = (f"https://data-api.ecb.europa.eu/service/data/{dataset}/{key}"
           f"?format=csvdata&startPeriod={start}&detail=dataonly")
    try:
        raw = http_get(url, timeout=20).decode("utf-8")
        if "TIME_PERIOD" not in raw.upper():
            return {}
        return parse_csv(raw)
    except Exception:
        return {}

def fetch_ecb_monthly():
    STS_KEYS = ["M.XX.N.CREG.PC0000.3.ABS", "M.XX.W.CREG.PC0000.3.ABS"]
    CAR_KEYS = ["M.XX.N.CREG.PC0000.3.ABS", "M.XX.N.CREG.PC0000..ABS",
                "M.XX..CREG.PC0000.3.ABS", "M.XX.N.NEWCARS.N", "M.XX.N.NEWCARS.NSA"]

    print("[ECB] Finding working STS key...")
    sts_key = None
    for k in STS_KEYS:
        d = ecb_fetch("STS", k, "DE", "2020-01")
        if d:
            sts_key = k
            print(f"[ECB] STS key: {k} -> {len(d)} months")
            break
    print("[ECB] Finding working CAR key (2023+)...")
    car_key = None
    for k in CAR_KEYS:
        d = ecb_fetch("CAR", k, "DE", "2023-01")
        if d:
            car_key = k
            print(f"[ECB] CAR key: {k} -> {len(d)} months")
            break

    if not sts_key and not car_key:
        print("[ECB] No working key -- monthly from ACEA estimates only")
        return {}

    print(f"\n[ECB] Fetching {len(COUNTRIES)} countries...")
    results = {}
    for geo in COUNTRIES:
        merged = {}
        if sts_key:
            merged.update(ecb_fetch("STS", sts_key, geo, "2015-01"))
        if car_key:
            merged.update(ecb_fetch("CAR", car_key, geo, "2023-01"))
        if merged:
            months = sorted(merged.keys())
            results[geo] = {"labels": months, "total": [merged[m] for m in months]}
            print(f"[ECB]   {geo}: {len(months)} months")
        time.sleep(0.2)
    print(f"[ECB] Done -- {len(results)} countries")
    return results

def build_acea_monthly(geo, existing_labels_set):
    season = SEASON_OVERRIDE.get(geo, SEASON_DEFAULT)
    result = {}
    for year in [2023, 2024, 2025]:
        annual = ACEA_TOTALS.get(geo, {}).get(year)
        if not annual:
            continue
        for mm in [f"{m:02d}" for m in range(1, 13)]:
            period = f"{year}-{mm}"
            if period not in existing_labels_set:
                result[period] = round(annual * season[mm])
    annual_2026_est = ACEA_TOTALS.get(geo, {}).get(2025)
    if annual_2026_est:
        annual_2026_est = round(annual_2026_est * 1.02)
        for mm in PARTIAL_2026_MONTHS:
            period = f"2026-{mm}"
            if period not in existing_labels_set:
                result[period] = round(annual_2026_est * season[mm])
    return result

def fetch_eurostat_annual():
    url = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
           "road_eqr_carpda?format=JSON&lang=EN&unit=NR")
    print(f"\n[Eurostat] Fetching annual powertrain breakdown...")
    try:
        raw = json.loads(http_get(url, timeout=60).decode("utf-8"))
    except Exception as e:
        print(f"[Eurostat] Error: {e}")
        return {}, "2024"

    dims      = raw.get("dimension", {})
    values    = raw.get("value", {})
    dim_order = raw.get("id",   [])
    dim_sizes = raw.get("size", [])

    def idx(d): return dims.get(d, {}).get("category", {}).get("index", {})
    geo_idx  = idx("geo")
    time_idx = idx("time")
    mot_idx  = idx("mot_nrg")

    strides = {}
    for i, d in enumerate(dim_order):
        s = 1
        for j in range(i + 1, len(dim_order)):
            s *= dim_sizes[j]
        strides[d] = s

    years = sorted([y for y in time_idx if int(y) >= 2015])
    latest_year = years[-1] if years else "2024"
    print(f"[Eurostat] Available years: {years[0]}-{latest_year}")

    estat_to_ecb = {v[1]: k for k, v in COUNTRIES.items()}

    def get_val(g, t, fuel):
        f = mot_idx.get(fuel)
        if f is None:
            return 0
        linear = (g * strides.get("geo",     1)
                  + t * strides.get("time",    1)
                  + f * strides.get("mot_nrg", 1))
        v = values.get(str(linear)) or values.get(linear)
        return int(v) if v is not None else 0

    result = {}
    for estat_geo, g_pos in geo_idx.items():
        ecb_key = estat_to_ecb.get(estat_geo)
        if not ecb_key:
            continue
        country_data = {}
        for year in years:
            t_pos = time_idx.get(year)
            if t_pos is None:
                continue
            yv = {f: 0 for f in ["bev", "phev", "hybrid", "petrol", "diesel", "other"]}
            for fuel_code, field in FUEL_MAP_GRANULAR.items():
                yv[field] += get_val(g_pos, t_pos, fuel_code)
            if yv["petrol"] == 0 and yv["diesel"] == 0:
                for fuel_code, field in FUEL_MAP_FALLBACK.items():
                    v = get_val(g_pos, t_pos, fuel_code)
                    if v > 0:
                        yv[field] += v
            country_data[year] = yv
        if country_data:
            result[ecb_key] = country_data

    if "DE" in result:
        last = max(result["DE"].keys())
        de = result["DE"][last]
        print(f"[Eurostat] DE {last}: BEV={de['bev']:,} PHEV={de['phev']:,} "
              f"Petrol={de['petrol']:,} Diesel={de['diesel']:,}")
    print(f"[Eurostat] Done -- {len(result)} countries, {years[0]}-{latest_year}")
    return result, latest_year

def inject_acea_2025(annual_data, eurostat_latest_year):
    if eurostat_latest_year >= "2025":
        print("[Annual] Eurostat has 2025 -- no injection needed")
        return annual_data

    print("[Annual] Eurostat ends at 2024 -- injecting ACEA 2025 estimates...")

    DIESEL_2015 = {
        "AT":0.50,"BE":0.52,"BG":0.28,"CY":0.44,"CZ":0.40,"DE":0.48,"DK":0.33,
        "EE":0.25,"EL":0.44,"ES":0.65,"FI":0.12,"FR":0.57,"HR":0.38,"HU":0.32,
        "IE":0.69,"IT":0.56,"LT":0.25,"LU":0.38,"LV":0.25,"MT":0.38,"NL":0.19,
        "PL":0.30,"PT":0.55,"RO":0.28,"SE":0.40,"SI":0.42,"SK":0.35,
        "NO":0.30,"CH":0.35,"GB":0.49,
    }
    HIGH_PHEV = {"SE","DE","GB","FR","BE","NL","AT","FI","CH","IE","LU"}

    for geo, bev_pct in ACEA_BEV_2025.items():
        total = ACEA_TOTALS.get(geo, {}).get(2025)
        if not total:
            continue
        bev   = round(total * bev_pct / 100)
        phev_ratio = 0.35 if geo in HIGH_PHEV else 0.20
        if geo == "NO":
            phev_ratio = 0.03
        phev  = round(bev * phev_ratio)
        hybrid = round(total * 0.22)
        d_base = DIESEL_2015.get(geo, 0.35)
        diesel = round(total * max(0.03, d_base - 10 * 0.025))
        other  = round(total * 0.012)
        petrol = max(0, total - bev - phev - hybrid - diesel - other)

        if geo not in annual_data:
            annual_data[geo] = {}
        annual_data[geo]["2025"] = {
            "bev": bev, "phev": phev, "hybrid": hybrid,
            "petrol": petrol, "diesel": diesel, "other": other,
        }
        print(f"[Annual]   {geo} 2025: BEV={bev:,} ({bev_pct}%) PHEV={phev:,} "
              f"Petrol={petrol:,} Diesel={diesel:,}")

    return annual_data

def write_files(ecb_monthly, annual, current_year):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    changed = []

    for ecb_code, (name, estat_code, pop) in COUNTRIES.items():
        a = annual.get(ecb_code, {})

        # NUR volle vergangene Jahre für "Annual breakdown by powertrain"
        years = sorted([y for y in a.keys() if int(y) < current_year]) if a else []

        existing_ecb = ecb_monthly.get(ecb_code, {})
        existing_labels = set(existing_ecb.get("labels", []))
        acea_ext = build_acea_monthly(ecb_code, existing_labels)

        merged_m = {}
        for lbl, val in zip(existing_ecb.get("labels", []), existing_ecb.get("total", [])):
            merged_m[lbl] = val
        merged_m.update(acea_ext)

        all_months = sorted(merged_m.keys())
        monthly_block = {
            "labels": all_months,
            "total":  [merged_m[m] for m in all_months],
        }

        annual_block = {
            "labels": years,
            "bev":    [a[y].get("bev",    0) for y in years],
            "phev":   [a[y].get("phev",   0) for y in years],
            "hybrid": [a[y].get("hybrid", 0) for y in years],
            "petrol": [a[y].get("petrol", 0) for y in years],
            "diesel": [a[y].get("diesel", 0) for y in years],
            "other":  [a[y].get("other",  0) for y in years],
        }

        # NEU: YTD für das laufende Jahr (Jan–März, später Jan–April usw.)
        ytd_months = [m for m in all_months if m.startswith(f"{current_year}-")]
        ytd = {}
        if ytd_months:
            ytd_total = sum(merged_m[m] for m in ytd_months)
            ytd = {
                "year": current_year,
                "months": len(ytd_months),
                "total": ytd_total,
                "labels": ytd_months,
                "monthly_totals": [merged_m[m] for m in ytd_months]
            }

        payload = {
            "name":           name,
            "ecb_code":       ecb_code,
            "population_mio": pop,
            "source_monthly": "ECB STS + ACEA seasonal estimates",
            "source_annual":  "Eurostat road_eqr_carpda + ACEA 2025",
            "last_updated":   NOW.isoformat(),
            "monthly":        monthly_block,
            "annual":         annual_block,
            "ytd":            ytd,          # ← NEU
        }

        fname = name.lower().replace(" ", "_") + ".json"
        path  = DATA_DIR / fname
        old   = {}
        if path.exists():
            try:
                old = json.loads(path.read_text("utf-8"))
                if "monthly" not in old:
                    old["monthly"] = {"labels": [], "total": []}
            except Exception:
                pass

        def no_ts(d):
            d2 = dict(d)
            d2.pop("last_updated", None)
            return d2

        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), "utf-8")
        if no_ts(payload) != no_ts(old):
            changed.append(name)
            latest = all_months[-1] if all_months else "n/a"
            annual_yrs = ",".join(years[-3:]) if years else "none"
            print(f"[Write] {name} OK  ({len(all_months)} months -> {latest}, "
                  f"annual: {annual_yrs}, YTD {current_year}: {len(ytd_months)} months)")
        else:
            print(f"[Write] {name} (no change)")

    return changed

# ── Telegram-Funktionen (unverändert) ─────────────────────────────────────
def generate_chart_image(changed, latest_month):
    bev_list = sorted(ACEA_BEV_2025.items(), key=lambda x: x[1], reverse=True)[:10]
    countries = [COUNTRIES[code][0] for code, _ in bev_list]
    values = [pct for _, pct in bev_list]

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(countries, values, color="#00cc66")
    ax.set_xlabel("BEV-Anteil 2025 (%)")
    ax.set_title(f"🚗 EV-Adoption Worldmap Update\n{latest_month} • {len(changed)} Länder geändert")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)

    for bar in bars:
        width = bar.get_width()
        ax.text(width + 1, bar.get_y() + bar.get_height()/2,
                f"{width:.1f}%", va="center", fontsize=11, fontweight="bold")

    fd, path = tempfile.mkstemp(suffix=".png")
    plt.tight_layout()
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close()
    os.close(fd)
    return path

def send_telegram_photo(img_path):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    with open(img_path, "rb") as f:
        files = {"photo": f}
        data = {"chat_id": TELEGRAM_CHAT, "caption": f"📊 EV-Adoption Top 10\n{datetime.now(timezone.utc).strftime('%d.%m.%Y')}"}
        try:
            r = requests.post(url, data=data, files=files, timeout=20)
            r.raise_for_status()
            print("[Telegram] Chart image sent successfully")
        except Exception as e:
            print(f"[Telegram] Image error: {e}")

def send_telegram(changed, n_countries, latest_month):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    if not changed:
        print("[Telegram] No changes → nothing sent")
        return

    date_str = NOW.strftime("%d.%m.%Y %H:%M UTC")
    lines = "\n".join(f"  - {c}" for c in changed[:25])
    body = (f"🚗 Car Registrations - Updated\n"
            f"{date_str}\n"
            f"{n_countries} countries\n"
            f"Monthly data up to: {latest_month}\n\n"
            f"Changed ({len(changed)}):\n{lines}\n\n"
            f"https://altair02.github.io/EV-adoption-worldmap/")

    data = json.dumps({
        "chat_id": TELEGRAM_CHAT,
        "text": body,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            msg_id = json.loads(r.read()).get("result", {}).get("message_id", "?")
        print(f"[Telegram] Message sent (id={msg_id})")
    except Exception as e:
        print(f"[Telegram] Message error: {e}")

    img_path = generate_chart_image(changed, latest_month)
    if img_path and os.path.exists(img_path):
        send_telegram_photo(img_path)
        try:
            os.unlink(img_path)
        except:
            pass

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print(f"  Car Registration Updater v12  --  {NOW.strftime('%d.%m.%Y %H:%M UTC')}")
    print("=" * 65)

    current_year = NOW.year

    ecb_monthly = fetch_ecb_monthly()

    result = fetch_eurostat_annual()
    if isinstance(result, tuple):
        annual_raw, eurostat_latest = result
    else:
        annual_raw, eurostat_latest = result, "2024"

    annual = inject_acea_2025(annual_raw, eurostat_latest)

    if not ecb_monthly and not annual:
        print("WARNING: No data from either source")
        send_telegram([], 0, "n/a")
        sys.exit(1)

    changed = write_files(ecb_monthly, annual, current_year)

    latest = "2026-03 (ACEA est.)"
    for v in ecb_monthly.values():
        if v.get("labels") and v["labels"][-1] > "2026-03":
            latest = v["labels"][-1]

    send_telegram(changed, len(COUNTRIES), latest)
    print(f"\nDone -- {len(changed)} countries updated, monthly up to: {latest}")

if __name__ == "__main__":
    main()
