import pandas as pd
from sqlalchemy import create_engine
import os
import requests
from tqdm import tqdm

# --- CONFIGURATION ---
DB_PATH = 'sqlite:///phyto_data.db'
INPUT_CSV = 'data/Achats-de-produits-phytosanitaires-a-lechelle-du-code-postal-.2025-06.csv'
OUTPUT_CSV = 'resultat_detail_temporel.csv'

# Dictionnaire de traduction des codes pour lecture facile
GHS_DESC = {
    'H350': 'Cancer', 'H351': 'Cancer suspecté',
    'H360': 'Reprotoxique', 'H361': 'Reprotoxique suspecté',
    'H340': 'Mutagène', 'H341': 'Mutagène suspecté',
    'H300': 'Mortel', 'H330': 'Mortel (Inhalation)', 'H310': 'Mortel (Peau)',
    'H370': 'Dommages organes', 'H372': 'Dommages organes (long terme)',
    'H400': 'Ecotoxique', 'H410': 'Ecotoxique (long terme)',
    'H318': 'Lésions oculaires', 'H314': 'Brûlures'
}


def load_product_details():
    """Charge un dictionnaire complet {CAS: {'Nom': '...', 'Dangers': '...'}}"""
    print("Chargement des définitions toxicologiques...")
    if not os.path.exists("datacreation/phyto_data.db"):
        print("ERREUR : Base phyto_data.db introuvable.")
        return {}

    engine = create_engine(DB_PATH)

    # 1. Récupérer les Noms
    df_subst = pd.read_sql("SELECT id, cas_number, nom_ephy FROM substance", engine)

    # 2. Récupérer les Dangers (GHS)
    df_tox = pd.read_sql("SELECT substance_id, valeur FROM toxicite WHERE categorie='GHS'", engine)

    # Création du dictionnaire
    details = {}

    # On groupe les dangers par substance ID
    tox_grouped = df_tox.groupby('substance_id')['valeur'].apply(list)

    for idx, row in df_subst.iterrows():
        sid = row['id']
        cas = str(row['cas_number']).strip()
        nom = row['nom_ephy']

        # Récupération et traduction des dangers
        dangers_bruts = tox_grouped.get(sid, [])
        dangers_clairs = set()

        for d in dangers_bruts:
            code = d.split('+')[0].strip()
            if code in GHS_DESC:
                dangers_clairs.add(GHS_DESC[code])
            else:
                dangers_clairs.add(code)  # On garde le code si pas de traduction

        dangers_str = ", ".join(list(dangers_clairs))

        details[cas] = {
            'Nom': nom,
            'Dangers': dangers_str
        }

    print(f"Base chargée : {len(details)} substances documentées.")
    return details


def get_gps_for_cp(cp_list):
    """Récupère Lat/Lon via API Géo (Optimisé)"""
    print(f"Géolocalisation de {len(cp_list)} communes...")
    mapping = {}
    session = requests.Session()

    for cp in tqdm(cp_list):
        try:
            url = f"https://geo.api.gouv.fr/communes?codePostal={cp}&fields=nom,centre&boost=population&limit=1"
            r = session.get(url, timeout=1)
            if r.status_code == 200 and len(r.json()) > 0:
                data = r.json()[0]
                coords = data['centre']['coordinates']
                mapping[cp] = {
                    'Ville': data['nom'],
                    'Lon': coords[0],
                    'Lat': coords[1]
                }
        except:
            pass
    return mapping


def process_time_series():
    print("--- EXTRACTION DÉTAILLÉE (TEMPORELLE) ---")

    # 1. Charger les infos produits
    prod_db = load_product_details()
    if not prod_db: return

    if not os.path.exists(INPUT_CSV):
        print(f"ERREUR : Fichier {INPUT_CSV} introuvable.")
        return

    # 2. Lecture du fichier pour détecter les colonnes
    try:
        sample = pd.read_csv(INPUT_CSV, sep=';', nrows=5, encoding='latin-1')
    except:
        sample = pd.read_csv(INPUT_CSV, sep=';', nrows=5)

    cols = [c.lower() for c in sample.columns]
    col_cas = next((c for c in cols if 'cas' in c), None)
    col_cp = next((c for c in cols if 'postal' in c or 'insee' in c), None)
    col_qty = next((c for c in cols if 'quantit' in c), None)
    col_year = next((c for c in cols if 'annee' in c), None)  # Nouvelle colonne critique

    if not all([col_cas, col_cp, col_qty, col_year]):
        print(f"Colonnes manquantes (CAS, CP, Qty ou Année). Trouvé : {cols}")
        return

    # 3. Lecture et Agrégation par (Année + CP + CAS)
    # On ne peut pas garder chaque ligne de vente individuelle (trop gros),
    # on somme par année pour chaque produit dans chaque ville.

    print("Lecture et agrégation des données...")
    chunk_size = 100000
    aggregated_data = {}  # Clé = (Annee, CP, CAS), Valeur = Quantité

    reader = pd.read_csv(INPUT_CSV, sep=';', encoding='latin-1', chunksize=chunk_size, on_bad_lines='skip',
                         dtype={col_cp: str, col_year: int})

    for chunk in tqdm(reader, desc="Traitement"):
        chunk.columns = [c.lower() for c in chunk.columns]

        # Nettoyage
        chunk['qty'] = pd.to_numeric(chunk[col_qty].astype(str).str.replace(',', '.').str.replace(' ', ''),
                                     errors='coerce').fillna(0)
        chunk['cas'] = chunk[col_cas].astype(str).str.strip()
        chunk['cp'] = chunk[col_cp].astype(str).str.strip().str.zfill(5)

        # On ne garde que les lignes avec quantité > 0
        chunk = chunk[chunk['qty'] > 0]

        # Groupby local pour réduire la taille du dictionnaire
        grouped = chunk.groupby([col_year, 'cp', 'cas'])['qty'].sum()

        for (year, cp, cas), qty in grouped.items():
            key = (year, cp, cas)
            aggregated_data[key] = aggregated_data.get(key, 0) + qty

    # 4. Géolocalisation
    unique_cps = list(set([k[1] for k in aggregated_data.keys()]))
    gps_map = get_gps_for_cp(unique_cps)

    # 5. Construction du fichier final
    print("Construction du fichier final...")
    final_rows = []

    for (year, cp, cas), qty in aggregated_data.items():
        if cp in gps_map and cas in prod_db:
            info_gps = gps_map[cp]
            info_prod = prod_db[cas]

            final_rows.append({
                'Annee': year,
                'CodePostal': cp,
                'Ville': info_gps['Ville'],
                'Latitude': info_gps['Lat'],
                'Longitude': info_gps['Lon'],
                'Produit': info_prod['Nom'],
                'Effets_Secondaires': info_prod['Dangers'],
                'Quantite_kg': round(qty, 2)
            })

    df_final = pd.DataFrame(final_rows)
    df_final.to_csv(OUTPUT_CSV, index=False)

    print(f"\nSUCCÈS ! Fichier généré : {OUTPUT_CSV}")
    print(f"Contient {len(df_final)} lignes.")
    print("Dans Kepler.gl :")
    print("1. Ajoutez un filtre sur le champ 'Annee' pour avoir la barre de lecture.")
    print("2. Ajoutez un Tooltip sur 'Produit', 'Quantite_kg' et 'Effets_Secondaires'.")


if __name__ == "__main__":
    process_time_series()