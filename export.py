import pandas as pd
from sqlalchemy import create_engine
import os

# Configuration
DB_PATH = 'sqlite:///phyto_data.db'
OUTPUT_FILE = 'datacreation/Resultats_Phyto_AVEC_DESCRIPTION.xlsx'

# --- DICTIONNAIRE DE TRADUCTION (CODES H -> FRANÇAIS) ---
GHS_MAP = {
    # Toxicité aiguë
    'H300': 'Mortel en cas d\'ingestion',
    'H301': 'Toxique en cas d\'ingestion',
    'H302': 'Nocif en cas d\'ingestion',
    'H304': 'Peut être mortel en cas d\'ingestion et de pénétration dans les voies respiratoires',
    'H310': 'Mortel par contact cutané',
    'H311': 'Toxique par contact cutané',
    'H312': 'Nocif par contact cutané',
    'H330': 'Mortel par inhalation',
    'H331': 'Toxique par inhalation',
    'H332': 'Nocif par inhalation',

    # Corrosion / Irritation
    'H314': 'Provoque des brûlures de la peau et des lésions oculaires graves',
    'H315': 'Provoque une irritation cutanée',
    'H317': 'Peut provoquer une allergie cutanée',
    'H318': 'Provoque des lésions oculaires graves',
    'H319': 'Provoque une sévère irritation des yeux',

    # Cancérogénicité / Mutagénicité / Reprotoxicité (CMR)
    'H340': 'Peut induire des anomalies génétiques',
    'H341': 'Susceptible d\'induire des anomalies génétiques',
    'H350': 'Peut provoquer le cancer',
    'H351': 'Susceptible de provoquer le cancer',
    'H360': 'Peut nuire à la fertilité ou au fœtus',
    'H360D': 'Peut nuire au fœtus',
    'H360F': 'Peut nuire à la fertilité',
    'H361': 'Susceptible de nuire à la fertilité ou au fœtus',
    'H361d': 'Susceptible de nuire au fœtus',

    # Organes cibles
    'H370': 'Risque avéré d\'effets graves pour les organes',
    'H371': 'Risque présumé d\'effets graves pour les organes',
    'H372': 'Risque avéré d\'effets graves pour les organes (exposition répétée)',
    'H373': 'Risque présumé d\'effets graves pour les organes (exposition répétée)',

    # Environnement (Écotoxicité)
    'H400': 'Très toxique pour les organismes aquatiques',
    'H410': 'Très toxique pour les organismes aquatiques, entraîne des effets néfastes à long terme',
    'H411': 'Toxique pour les organismes aquatiques, entraîne des effets néfastes à long terme',
    'H412': 'Nocif pour les organismes aquatiques, entraîne des effets néfastes à long terme',
    'H413': 'Peut être nocif à long terme pour les organismes aquatiques',

    # Abeilles / Ozone (EUH codes)
    'EUH401': 'Respectez les instructions d\'utilisation pour éviter les risques pour la santé humaine et l\'environnement',
}


def translate_ghs(code):
    """Traduit un code H ou retourne le code si inconnu"""
    if not code: return ""
    # Nettoyage (parfois le code est "H300+H310")
    code_clean = str(code).split('+')[0].strip()
    return GHS_MAP.get(code_clean, code)


def export_data():
    print("--- Exportation Enrichie ---")

    if not os.path.exists("datacreation/phyto_data.db"):
        print("Erreur : Base de données introuvable.")
        return

    engine = create_engine(DB_PATH)

    query = """
    SELECT 
        s.cas_number as 'CAS',
        s.nom_ephy as 'Substance',
        s.fonction as 'Fonction',
        t.source_db as 'Source',
        t.categorie as 'Type',
        t.parametre as 'Paramètre',
        t.valeur as 'Code_Valeur',
        t.unite as 'Unité'
    FROM substance s
    LEFT JOIN toxicite t ON s.id = t.substance_id
    WHERE t.valeur IS NOT NULL  -- On ne veut que ce qui a des effets
    ORDER BY s.nom_ephy
    """

    print("Lecture de la base...")
    df = pd.read_sql(query, engine)

    print(f"{len(df)} lignes trouvées avec des effets.")

    # --- APPLICATION DE LA TRADUCTION ---
    print("Traduction des codes dangers...")

    # On crée une nouvelle colonne 'Description_Claire'
    def decrypter(row):
        val = row['Code_Valeur']
        param = row['Paramètre']

        # Si c'est un code Danger (GHS)
        if param == 'Hazard':
            return translate_ghs(val)

        # Si c'est une valeur tox (ADI, ARfD)
        if row['Type'] == 'Tox':
            return f"Limite toxique: {val} {row['Unité']}"

        return val

    df['Description_Claire'] = df.apply(decrypter, axis=1)

    # Réorganisation des colonnes pour mettre la description en premier
    cols = ['Substance', 'Description_Claire', 'Code_Valeur', 'Paramètre', 'CAS', 'Fonction', 'Source']
    df = df[cols]

    print(f"Écriture Excel : {OUTPUT_FILE}...")
    df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
    print("Terminé ! Ouvrez le nouveau fichier Excel.")


if __name__ == "__main__":
    export_data()