import pandas as pd
import logging
from sqlalchemy.orm import Session
import os

# Importation des modules locaux
from models import init_db, Substance, Toxicite
from connectors.pubchem import PubChemConnector
from connectors.efsa import EfsaConnector

# --- CONFIGURATION FICHIERS ---
INPUT_FILE = "substance_active_Windows-1252.csv"
EFSA_CHAR = "SubstanceCharacterisation_KJ_2023.xlsx"
EFSA_REF = "ReferenceValues_KJ_2023.xlsx"

# --- DÉTECTION DES CHEMINS ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, 'data')

INPUT_PATH = os.path.join(DATA_DIR, INPUT_FILE)
EFSA_CHAR_PATH = os.path.join(DATA_DIR, EFSA_CHAR)
EFSA_REF_PATH = os.path.join(DATA_DIR, EFSA_REF)

# Configuration Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Orchestrator")


def run():
    print(f"--- Démarrage Final ---")

    if not os.path.exists(INPUT_PATH):
        logger.error(f"Fichier INTROUVABLE: {INPUT_PATH}")
        return

    # 1. Initialisation Base de Données
    db = init_db('sqlite:///phyto_data.db')
    session = Session(db)

    # --- MÉMOIRE ANTI-DOUBLONS ---
    # On charge tous les CAS déjà présents dans la base pour ne pas les refaire
    existing_cas = set(row[0] for row in session.query(Substance.cas_number).all())
    logger.info(f"Substances déjà en base : {len(existing_cas)}")

    # 2. Initialisation Connecteurs
    pubchem = PubChemConnector()
    efsa = EfsaConnector(EFSA_CHAR_PATH, EFSA_REF_PATH)
    efsa.load_data()

    # 3. Lecture CSV E-Phy
    logger.info(f"Lecture fichier E-Phy...")
    try:
        df = pd.read_csv(INPUT_PATH, sep=';', encoding='cp1252', on_bad_lines='skip', dtype=str)
    except Exception as e:
        logger.critical(f"Erreur lecture CSV: {e}")
        return

    count = 0
    # 4. Traitement
    for idx, row in df.iterrows():
        cas_raw = row.get('Numero CAS')
        nom = row.get('Nom substance active', 'Inconnu')

        if pd.isna(cas_raw) or str(cas_raw).strip() in ['nan', 'NC', '', 'None']:
            continue

        cas = str(cas_raw).strip()

        # --- DOUBLE VÉRIFICATION ---
        # 1. Est-ce qu'on l'a déjà fait avant ? (Base de données)
        if cas in existing_cas:
            continue

        # 2. Ajout immédiat à la liste "fait" pour éviter les doublons DANS le fichier CSV lui-même
        existing_cas.add(cas)

        logger.info(f"Traitement [{count}]: {nom} (CAS: {cas})")

        subst = Substance(cas_number=cas, nom_ephy=nom, fonction="Substance Active")

        # PubChem
        pc = pubchem.get_details_from_cas(cas)
        if pc:
            subst.cid_pubchem = pc['cid']
            subst.masse_molaire = pc['weight']
            subst.formule = pc['formula']
            # GHS
            ghs_codes = pubchem.get_ghs_classification(pc['cid'])
            for code in ghs_codes:
                subst.toxicites.append(Toxicite(source_db="PubChem", categorie="GHS", parametre="Hazard", valeur=code))

        # EFSA
        tox_values = efsa.get_tox_values(cas)
        if tox_values:
            for val in tox_values:
                subst.toxicites.append(Toxicite(
                    source_db="EFSA",
                    categorie="Tox",
                    parametre=val['parametre'],
                    valeur=str(val['valeur']),
                    unite=str(val['unite'])
                ))

        session.add(subst)
        count += 1

        # Sauvegarde par lot de 10
        if count % 10 == 0:
            try:
                session.commit()
            except Exception as e:
                logger.error(f"Erreur lors du commit: {e}")
                session.rollback()

    session.commit()
    logger.info("Terminé avec succès.")


if __name__ == "__main__":
    run()