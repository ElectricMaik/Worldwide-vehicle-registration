#!/usr/bin/env python3
"""
build_all_countries.py
======================
Combines all individual country JSON files in data/countries/ into a single
all_countries.json file that index.html loads at startup.

Usage:
    python3 build_all_countries.py

This script:
1. Reads every .json file in data/countries/
2. Combines them into a single master structure
3. Writes the result to data/all_countries.json
4. Includes metadata with all data source attributions

Run this script after editing any individual country JSON to regenerate
the combined file. The website will pick up changes automatically on reload.
"""

import json
import os
import sys
from datetime import datetime

# === CONFIGURATION ===
COUNTRIES_DIR = 'data/countries'
OUTPUT_FILE = 'data/all_countries.json'

# Data source attributions - kept in code for easy editing
DATA_SOURCES = {
    # === EUROPEAN COUNTRIES ===
    'germany': 'KBA (Kraftfahrt-Bundesamt) - kba.de',
    'united_kingdom': 'SMMT (Society of Motor Manufacturers and Traders) - smmt.co.uk',
    'norway': 'OFV (Opplysningsrådet for Veitrafikken) - ofv.no',
    'france': 'SDES / PFA (Plateforme Automobile Française) / AAA Data',
    'italy': 'UNRAE (Unione Nazionale Rappresentanti Autoveicoli Esteri) - unrae.it',
    'spain': 'ANFAC / DGT - anfac.com',
    'netherlands': 'RAI Vereniging / BOVAG / RDC - raivereniging.nl',
    'belgium': 'FEBIAC - febiac.be',
    'sweden': 'Mobility Sweden - mobilitysweden.se',
    'denmark': 'De Danske Bilimportører / Danmarks Statistik - mobility.dk',
    'austria': 'Statistik Austria - statistik.at',
    'switzerland': 'auto-schweiz / ASTRA - auto.swiss',
    'finland': 'Traficom / Statistics Finland - traficom.fi',
    'iceland': 'Samgöngustofa / FÍB - samgongustofa.is',
    'portugal': 'ACAP - acap.pt',
    'ireland': 'SIMI - simi.ie',
    'poland': 'PZPM - pzpm.org.pl',
    'czech_republic': 'SDA / Ministerstvo dopravy ČR - portal.sda-cia.cz',
    'romania': 'APIA / DRPCIV - apia.ro',
    'hungary': 'GÉMÉSZ - gemesz.hu',
    'luxembourg': 'SNCA / STATEC',
    'malta': 'NSO Malta / Transport Malta - nso.gov.mt',
    'cyprus': 'CYSTAT - cystat.mof.gov.cy',
    'estonia': 'Transpordiamet / Statistikaamet - mnt.ee',
    'latvia': 'CSDD / Auto asociācija - csdd.lv',
    'lithuania': 'Regitra / ENA - regitra.lt',
    'croatia': 'HAK / MUP - hak.hr',
    'slovenia': 'SURS - stat.si',
    'slovakia': 'ZAP SR - zapsr.sk',
    'bulgaria': 'AAB / MoI - aab-bg.com',
    'greece': 'SEAA / ELSTAT - seaa.gr',

    # === OTHER COUNTRIES ===
    'united_states': 'BEA / DOT / Wards Intelligence',
    'china': 'CAAM (China Association of Automobile Manufacturers)',
    'japan': 'JADA (Japan Automobile Dealers Association)',
    'south_korea': 'KAMA / KAIDA',
    'india': 'SIAM (Society of Indian Automobile Manufacturers)',
    'australia': 'FCAI VFACTS',
    'canada': 'DesRosiers Automotive / GAC',
    'brazil': 'Fenabrave / Anfavea',
    'turkey': 'ODD / ODMD',
    'russia': 'AEB Automotive Manufacturers Committee',
    'mexico': 'AMDA / AMIA / INEGI',
    'argentina': 'ACARA',
    'indonesia': 'Gaikindo',
    'thailand': 'Federation of Thai Industries',
    'malaysia': 'MAA (Malaysian Automotive Association)',
    'new_zealand': 'NZTA / MIA',
    'taiwan': 'CAMVR (Taiwan)',
    'vietnam': 'VAMA',
    'saudi_arabia': 'GASTAT / national licensing data',
}


def build_master_json(countries_dir=COUNTRIES_DIR, output_file=OUTPUT_FILE):
    """Combine all country JSONs into a single master file."""

    # Resolve paths relative to script location if not absolute
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isabs(countries_dir):
        countries_dir = os.path.join(script_dir, countries_dir)
    if not os.path.isabs(output_file):
        output_file = os.path.join(script_dir, output_file)

    if not os.path.isdir(countries_dir):
        print(f'ERROR: {countries_dir} does not exist', file=sys.stderr)
        sys.exit(1)

    files = sorted([f for f in os.listdir(countries_dir) if f.endswith('.json')])
    if not files:
        print(f'ERROR: No JSON files found in {countries_dir}', file=sys.stderr)
        sys.exit(1)

    print(f'Found {len(files)} country JSON files in {countries_dir}')

    master = {
        'metadata': {
            'description': 'Worldwide Vehicle Registration Data - All Countries',
            'last_updated': datetime.now().strftime('%Y-%m-%d'),
            'years_covered': '2015-2025',
            'data_sources': DATA_SOURCES,
        },
        'countries': {}
    }

    for filename in files:
        country_key = filename.replace('.json', '')
        path = os.path.join(countries_dir, filename)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                master['countries'][country_key] = json.load(f)
            print(f'  ✓ {country_key}')
        except Exception as e:
            print(f'  ✗ {country_key}: {e}', file=sys.stderr)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(master, f, indent=2, ensure_ascii=False)

    size_kb = os.path.getsize(output_file) / 1024
    print(f'\n✓ Wrote {output_file} ({size_kb:.1f} KB)')
    print(f'  Countries: {len(master["countries"])}')

    return master


def validate_master_json(output_file=OUTPUT_FILE):
    """Verify the generated file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isabs(output_file):
        output_file = os.path.join(script_dir, output_file)

    with open(output_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    issues = []
    for country, cdata in data['countries'].items():
        if 'annual' not in cdata:
            issues.append(f'{country}: missing annual data')
            continue
        labels = cdata['annual'].get('labels', [])
        if not labels:
            issues.append(f'{country}: empty labels')
            continue
        # Check all required arrays have same length
        for field in ['bev', 'phev', 'hybrid', 'petrol', 'diesel', 'other']:
            arr = cdata['annual'].get(field, [])
            if len(arr) != len(labels):
                issues.append(f'{country}.{field}: length {len(arr)} != labels {len(labels)}')

    if issues:
        print('\nValidation issues:')
        for i in issues[:10]:
            print(f'  ⚠ {i}')
        if len(issues) > 10:
            print(f'  ... and {len(issues) - 10} more')
    else:
        print('\n✓ All countries validated successfully')


if __name__ == '__main__':
    build_master_json()
    validate_master_json()
