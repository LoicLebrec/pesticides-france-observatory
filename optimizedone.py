import duckdb
import pandas as pd
import os
import requests

# --- CONFIGURATION ---
INPUT_CSV = 'data/Achats-de-produits-phytosanitaires-a-lechelle-du-code-postal-.2025-06.csv'
DB_RISK = 'phyto_data.db'
OUTPUT_FILE = 'datacreation/donnees_kepler_FINAL.parquet'


def get_gps_reference():
    """Télécharge un référentiel CP -> GPS léger"""
    print("Récupération des coordonnées GPS...")
    url = "https://unpkg.com/codes-postaux@4.0.0/codes-postaux.json"
    try:
        df = pd.read_json(url)
        return df.groupby('codePostal').first().reset_index()[['codePostal', 'nomCommune', 'codeCommune']]
    except:
        return pd.DataFrame()


def detect_columns(csv_path):
    """Trouve les vrais noms des colonnes dans votre fichier CSV"""
    print(f"Inspection des colonnes de {csv_path}...")
    try:
        # On lit juste la première ligne pour voir les en-têtes
        # Essai avec séparateur ; (fréquent)
        df = pd.read_csv(csv_path, sep=';', nrows=1, encoding='latin-1')
        if len(df.columns) < 2:
            # Si échec, essai virgule
            df = pd.read_csv(csv_path, sep=',', nrows=1, encoding='latin-1')

        cols = [c.lower() for c in df.columns]
        real_cols = list(df.columns)  # On garde les vrais noms (avec majuscules) pour SQL

        # Mapping intelligent
        mapping = {}

        # 1. CAS
        idx = next((i for i, c in enumerate(cols) if 'cas' in c), None)
        if idx is not None: mapping['cas'] = real_cols[idx]

        # 2. Code Postal
        idx = next((i for i, c in enumerate(cols) if 'postal' in c or 'insee' in c), None)
        if idx is not None: mapping['cp'] = real_cols[idx]

        # 3. Quantité
        idx = next((i for i, c in enumerate(cols) if 'quantit' in c), None)
        if idx is not None: mapping['qty'] = real_cols[idx]

        # 4. Année
        idx = next((i for i, c in enumerate(cols) if 'annee' in c or 'year' in c), None)
        if idx is not None: mapping['year'] = real_cols[idx]

        print(f"Colonnes identifiées : {mapping}")
        return mapping
    except Exception as e:
        print(f"Erreur detection colonnes : {e}")
        return {}


def run_big_data_pipeline():
    print("--- PIPELINE BIG DATA (DUCKDB + PARQUET) V2 ---")

    if not os.path.exists(INPUT_CSV):
        print(f"Erreur : Fichier {INPUT_CSV} introuvable.")
        return

    # 1. Détection des colonnes
    cols = detect_columns(INPUT_CSV)
    if not all(k in cols for k in ['cas', 'cp', 'qty', 'year']):
        print("ERREUR CRITIQUE : Impossible de trouver les colonnes (CAS, CP, QTY, ANNEE).")
        print("Vérifiez votre fichier CSV.")
        return

    # 2. Connexion DuckDB
    con = duckdb.connect(database=':memory:')

    # 3. Chargement des Risques
    print("Importation des risques...")
    import sqlite3
    sq_con = sqlite3.connect(DB_RISK)
    df_risk = pd.read_sql(
        "SELECT s.cas_number, t.valeur FROM substance s JOIN toxicite t ON s.id=t.substance_id WHERE t.categorie='GHS'",
        sq_con)

    SEV = {'H300': 100, 'H310': 100, 'H330': 100, 'H350': 50, 'H340': 50, 'H360': 50, 'H410': 5}
    risk_map = {}
    for _, r in df_risk.iterrows():
        c = str(r['cas_number']).strip()
        code = str(r['valeur']).split('+')[0].strip()
        s = SEV.get(code, 1)
        if c not in risk_map or s > risk_map[c]: risk_map[c] = s

    df_risk_clean = pd.DataFrame(list(risk_map.items()), columns=['cas', 'score'])
    con.register('risk_table', df_risk_clean)

    # 4. La Requête Magique (DYNAMIQUE)
    # On insère les vrais noms de colonnes trouvés plus haut
    print("Traitement du fichier géant...")

    query = f"""
    SELECT 
        CAST(replace("{cols['cp']}", ' ', '') AS VARCHAR) as CodePostal,
        CAST("{cols['year']}" AS INTEGER) as Annee,

        -- Calcul du Risque Total
        SUM(
            TRY_CAST(replace(replace(CAST("{cols['qty']}" AS VARCHAR), ',', '.'), ' ', '') AS DOUBLE) 
            * COALESCE(r.score, 1)
        ) as Score_Toxicite,

        -- Calcul du Poids Total
        SUM(
            TRY_CAST(replace(replace(CAST("{cols['qty']}" AS VARCHAR), ',', '.'), ' ', '') AS DOUBLE)
        ) as Quantite_Kg,

        -- Liste des 5 produits principaux
        LIST(DISTINCT "{cols['cas']}") as Produits_CAS

    FROM read_csv_auto('{INPUT_CSV}', normalize_names=False) as achats
    LEFT JOIN risk_table r ON trim(achats."{cols['cas']}") = r.cas
    GROUP BY 1, 2
    HAVING Quantite_Kg > 0
    ORDER BY Annee DESC
    """

    try:
        df_agg = con.execute(query).df()
        print(f"Agrégation terminée : {len(df_agg)} lignes.")
    except Exception as e:
        print(f"Erreur SQL DuckDB : {e}")
        return

    # 5. Ajout GPS (Python)
    print("Ajout des coordonnées GPS...")
    try:
        import pgeocode
        nomi = pgeocode.Nominatim('fr')
        cps = df_agg['CodePostal'].unique()
        # On ignore les erreurs de code postal (country_bias)
        geo_data = nomi.query_postal_code(cps)[['postal_code', 'place_name', 'latitude', 'longitude']]
        geo_data.rename(
            columns={'postal_code': 'CodePostal', 'place_name': 'Ville', 'latitude': 'Lat', 'longitude': 'Lon'},
            inplace=True)

        df_final = df_agg.merge(geo_data, on='CodePostal', how='inner')

    except ImportError:
        print("⚠️ Installez 'pgeocode' pour les GPS (pip install pgeocode).")
        print("Utilisation du référentiel statique de secours...")
        ref_geo = get_gps_reference()
        ref_geo.rename(columns={'codePostal': 'CodePostal', 'nomCommune': 'Ville'}, inplace=True)
        # Attention, ce ref n'a pas lat/lon direct, il faut un fichier avec lat/lon
        # Pour simplifier, on sauve sans GPS si pas pgeocode
        df_final = df_agg

    # 6. Export PARQUET
    print(f"Création du fichier optimisé : {OUTPUT_FILE}")
    df_final.to_parquet(OUTPUT_FILE, index=False)
    print("✅ SUCCÈS. Glissez ce fichier .parquet dans Kepler.gl !")


if __name__ == "__main__":
    run_big_data_pipeline()