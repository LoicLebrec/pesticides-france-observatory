import pandas as pd
import os

# Chemins (copiés de votre config qui marche)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
EPHY_FILE = os.path.join(DATA_DIR, "substance_active_Windows-1252.csv")
EFSA_FILE = os.path.join(DATA_DIR, "SubstanceCharacterisation_KJ_2023.xlsx")

print("--- ANALYSE DES EN-TÊTES ---")

# 1. Check E-Phy
print(f"\nLecture de : {EPHY_FILE}")
try:
    df_ephy = pd.read_csv(EPHY_FILE, sep=';', encoding='cp1252', nrows=2)
    print("COLONNES DISPONIBLES E-PHY :")
    print(list(df_ephy.columns))
except Exception as e:
    print(f"Erreur E-Phy : {e}")

# 2. Check EFSA
print(f"\nLecture de : {EFSA_FILE}")
try:
    df_efsa = pd.read_excel(EFSA_FILE, nrows=2)
    # Normalisation pour voir ce que le script voit
    cols_norm = [str(c).lower().replace(' ', '_') for c in df_efsa.columns]
    print("COLONNES DISPONIBLES EFSA (Normalisées) :")
    print(cols_norm)
    print("COLONNES ORIGINALES :")
    print(list(df_efsa.columns))
except Exception as e:
    print(f"Erreur EFSA : {e}")
