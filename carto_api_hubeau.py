import pandas as pd
from sqlalchemy import create_engine
import os
import requests
from tqdm import tqdm
import time

# --- CONFIGURATION ---
DB_PATH = 'sqlite:///phyto_data.db'
# Nom EXACT de votre fichier
INPUT_CSV = 'data/Achats-de-produits-phytosanitaires-a-lechelle-du-code-postal-.2025-06.csv'
OUTPUT_CSV = 'resultat_carte_kepler.csv'

# Sévérité
SEVERITE_MAP = {
    'H300': 100, 'H310': 100, 'H330': 100,
    'H350': 50, 'H340': 50, 'H360': 50,
    'H351': 10, 'H361': 10,
    'H301': 5, 'H311': 5, 'H331': 5,
    'H372': 5, 'H410': 5,
    'H314': 2, 'H318': 2,
}


def load_severity_index():
    if not os.path.exists("datacreation/phyto_data.db"):
        print("ERREUR : Base phyto_data.db introuvable.")
        return {}
    engine = create_engine(DB_PATH)
    try:
        df = pd.read_sql(
            "SELECT s.cas_number, t.valeur FROM substance s JOIN toxicite t ON s.id = t.substance_id WHERE t.categorie = 'GHS'",
            engine)
    except:
        return {}

    cas_score = {}
    for idx, row in df.iterrows():
        cas = str(row['cas_number']).strip()
        code = str(row['valeur']).split('+')[0].strip()
        score = SEVERITE_MAP.get(code, 1)
        if cas not in cas_score or score > cas_score[cas]:
            cas_score[cas] = score
    print(f"Index Toxicité chargé : {len(cas_score)} substances.")
    return cas_score


def get_gps_for_cp(cp_list):
    """Récupère Lat/Lon pour une liste de codes postaux via API Géo"""
    print(f"Récupération GPS pour {len(cp_list)} codes postaux...")
    mapping = {}

    # On fait des requêtes par lot si possible, ou boucle simple
    # Boucle simple avec session pour aller vite
    session = requests.Session()

    for cp in tqdm(cp_list, desc="Géolocalisation"):
        try:
            # On demande le centre de la commune principale du CP
            url = f"https://geo.api.gouv.fr/communes?codePostal={cp}&fields=centre&boost=population&limit=1"
            r = session.get(url, timeout=1)
            if r.status_code == 200 and len(r.json()) > 0:
                coords = r.json()[0]['centre']['coordinates']
                mapping[cp] = {'lon': coords[0], 'lat': coords[1]}
        except:
            pass
    return mapping


def process():
    print("--- TRAITEMENT DU FICHIER CSV LOCAL ---")

    # 1. Charger Risques
    risk_index = load_severity_index()
    if not risk_index: return

    if not os.path.exists(INPUT_CSV):
        print(f"ERREUR CRITIQUE: Fichier {INPUT_CSV} introuvable.")
        print("Vérifiez le nom et l'emplacement (dossier data/)")
        return

    # 2. Détection des colonnes (Lecture des 5 premières lignes)
    print("Analyse du fichier...")
    try:
        # Essai encodage utf-8
        sample = pd.read_csv(INPUT_CSV, sep=';', nrows=5)
    except:
        # Essai encodage latin-1 (Windows)
        sample = pd.read_csv(INPUT_CSV, sep=';', encoding='latin-1', nrows=5)

    cols = [c.lower() for c in sample.columns]
    print(f"Colonnes trouvées : {cols}")

    # Mapping dynamique des colonnes
    col_cas = next((c for c in cols if 'cas' in c), None)
    col_cp = next((c for c in cols if 'postal' in c or 'insee' in c), None)
    col_qty = next((c for c in cols if 'quantit' in c), None)

    if not all([col_cas, col_cp, col_qty]):
        print("Erreur: Colonnes clés manquantes (CAS, Code Postal ou Quantité).")
        return

    # 3. Lecture par morceaux (Streaming)
    print(f"Lecture et calcul en cours (Fichier: {INPUT_CSV})...")

    chunk_size = 100000
    aggregated_risk = {}  # Stockage {CodePostal: Score}
    total_lines = 0

    # On relance la lecture complète
    reader = pd.read_csv(INPUT_CSV, sep=';', encoding='latin-1', chunksize=chunk_size, on_bad_lines='skip',
                         dtype={col_cp: str})

    for chunk in tqdm(reader, desc="Traitement des blocs"):
        # Normalisation
        chunk.columns = [c.lower() for c in chunk.columns]

        # Nettoyage données
        chunk['cas_clean'] = chunk[col_cas].astype(str).str.strip()
        # Gestion virgule française
        chunk['qty_clean'] = pd.to_numeric(
            chunk[col_qty].astype(str).str.replace(',', '.').str.replace(' ', ''),
            errors='coerce'
        ).fillna(0)

        # Mapping Sévérité
        chunk['severity'] = chunk['cas_clean'].map(risk_index).fillna(0)  # 0 si inconnu (prudence) ou 1

        # Calcul Risque
        chunk['risk_score'] = chunk['qty_clean'] * chunk['severity']

        # On ne garde que ce qui a du sens
        chunk = chunk[chunk['risk_score'] > 0]

        if chunk.empty: continue

        # Agrégation locale
        grouped = chunk.groupby(col_cp)['risk_score'].sum()

        # Fusion avec le total global
        for cp, score in grouped.items():
            # Nettoyage CP (5 chiffres)
            cp_clean = str(cp).split('.')[0].strip().zfill(5)
            if len(cp_clean) == 5:
                aggregated_risk[cp_clean] = aggregated_risk.get(cp_clean, 0) + score

        total_lines += len(chunk)

    print(f"\nTerminé. {len(aggregated_risk)} codes postaux analysés.")

    if not aggregated_risk:
        print(
            "ATTENTION: Aucun risque calculé. Vérifiez la correspondance des CAS entre votre base et le fichier BNVD.")
        return

    # 4. Ajout GPS
    unique_cps = list(aggregated_risk.keys())
    gps_map = get_gps_for_cp(unique_cps)

    # 5. Export Final
    final_data = []
    for cp, score in aggregated_risk.items():
        if cp in gps_map:
            final_data.append({
                'CodePostal': cp,
                'RiskScore': round(score, 2),
                'Lat': gps_map[cp]['lat'],
                'Lon': gps_map[cp]['lon']
            })

    if final_data:
        df_final = pd.DataFrame(final_data)
        df_final.to_csv(OUTPUT_CSV, index=False)
        print(f"\n✅ SUCCESS ! Fichier généré : {OUTPUT_CSV}")
        print(f"Contient {len(df_final)} points géolocalisés.")
        print("👉 Importez ce fichier dans Kepler.gl")
    else:
        print("Echec lors de la géolocalisation.")


if __name__ == "__main__":
    process()