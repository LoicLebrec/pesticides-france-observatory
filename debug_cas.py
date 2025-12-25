import pandas as pd
import os

# --- CONFIGURATION ---
INPUT_CSV = 'data/Achats-de-produits-phytosanitaires-a-lechelle-du-code-postal-.2025-06.csv'


def diagnose_loss():
    print("--- DIAGNOSTIC DE PERTE DE DONNÉES ---")

    if not os.path.exists(INPUT_CSV):
        print(f"Erreur : {INPUT_CSV} introuvable.")
        return

    print("Lecture d'un échantillon (500 000 lignes)...")

    # On lit en mode 'low_memory=False' pour bien voir les types
    # On essaie de tout lire en string pour voir le format brut
    try:
        df = pd.read_csv(INPUT_CSV, sep=';', nrows=500000, encoding='latin-1', dtype=str)
    except:
        df = pd.read_csv(INPUT_CSV, sep=',', nrows=500000, encoding='latin-1', dtype=str)

    # Normalisation noms colonnes
    df.columns = [c.lower() for c in df.columns]

    print(f"Lignes lues : {len(df)}")

    # 1. ANALYSE DES ANNÉES
    col_annee = next((c for c in df.columns if 'annee' in c or 'year' in c), None)
    if col_annee:
        print(f"\n1. ANALYSE COLONNE ANNÉE ('{col_annee}')")
        print("Valeurs uniques trouvées (Top 10) :")
        print(df[col_annee].value_counts().head(10))

        nb_2023_brut = len(df[df[col_annee] == '2023'])
        nb_2023_float = len(df[df[col_annee] == '2023.0'])

        print(f" -> Lignes exactes '2023'   : {nb_2023_brut}")
        print(f" -> Lignes format '2023.0' : {nb_2023_float}")

        if nb_2023_brut == 0 and nb_2023_float > 0:
            print("⚠️ PROBLÈME DÉTECTÉ : L'année est stockée comme un nombre à virgule (2023.0).")
            print("   Le script précédent cherchait '2023' et a donc tout ignoré !")
    else:
        print("❌ Colonne Année introuvable !")

    # 2. ANALYSE DES QUANTITÉS
    col_qty = next((c for c in df.columns if 'quantit' in c), None)
    if col_qty:
        print(f"\n2. ANALYSE QUANTITÉS ('{col_qty}')")
        # Test de conversion
        sample_qty = df[col_qty].head(5).tolist()
        print(f"Exemples bruts : {sample_qty}")

        # Simulation nettoyage
        clean_qty = pd.to_numeric(
            df[col_qty].str.replace(',', '.').str.replace(' ', ''),
            errors='coerce'
        ).fillna(0)

        nb_sup_0 = len(clean_qty[clean_qty > 0])
        print(f" -> Lignes avec Quantité > 0 après nettoyage : {nb_sup_0} / {len(df)}")

        if nb_sup_0 < len(df) * 0.1:
            print("⚠️ ALERTE : Moins de 10% des lignes ont une quantité valide. Problème de conversion ?")

    # 3. ANALYSE DES CODES POSTAUX
    col_cp = next((c for c in df.columns if 'postal' in c), None)
    if col_cp:
        print(f"\n3. ANALYSE CODES POSTAUX ('{col_cp}')")
        # On regarde si les codes ont bien 5 chiffres
        sample_cp = df[col_cp].head(5).tolist()
        print(f"Exemples bruts : {sample_cp}")

        # Test format
        valid_cp = df[col_cp].str.len() == 5
        print(f" -> Codes postaux de 5 caractères : {valid_cp.sum()} / {len(df)}")

        # Test code 'manquant' ou 'NC'
        nb_nc = len(df[df[col_cp].astype(str).str.contains('NC|nan', case=False, na=True)])
        print(f" -> Codes Postaux invalides/manquants : {nb_nc}")


if __name__ == "__main__":
    diagnose_loss()