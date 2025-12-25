import pandas as pd
import requests
import json
import os
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_DATA = 'resultat_kepler_OPTIMISE.csv'
OUTPUT_FILE = 'datacreation/resultat_kepler_FINAL_POLYGONES.csv'

# 1. Source des formes (GeoJSON simplifié pour être léger)
GEOJSON_URL = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes-version-simplifiee.geojson"

# 2. Source de la correspondance Code Postal <-> Code INSEE
MAPPING_URL = "https://unpkg.com/codes-postaux@4.0.0/codes-postaux.json"


def convert_geojson_to_wkt(geometry):
    """
    Traduit la géométrie JSON en texte WKT (Well Known Text) pour Kepler.
    Ex: POLYGON ((3.14 48.2, 3.15 48.3 ...))
    """
    try:
        g_type = geometry['type'].upper()
        coords = geometry['coordinates']

        if g_type == 'POLYGON':
            rings = []
            for ring in coords:
                # Kepler veut "Longitude Latitude" séparés par espace
                pts = ", ".join([f"{p[0]} {p[1]}" for p in ring])
                rings.append(f"({pts})")
            return f"POLYGON ({', '.join(rings)})"

        elif g_type == 'MULTIPOLYGON':
            polys = []
            for poly in coords:
                rings = []
                for ring in poly:
                    pts = ", ".join([f"{p[0]} {p[1]}" for p in ring])
                    rings.append(f"({pts})")
                polys.append(f"({', '.join(rings)})")
            return f"MULTIPOLYGON ({', '.join(polys)})"
    except:
        return None


def merge_geometry():
    print("--- FUSION DES FRONTIÈRES (POLYGONES) ---")

    if not os.path.exists(INPUT_DATA):
        print(f"Erreur : Fichier {INPUT_DATA} introuvable.")
        return

    # 1. Chargement des données d'achats
    print("Lecture de vos données optimisées...")
    try:
        df_data = pd.read_csv(INPUT_DATA, dtype={'CodePostal': str})
    except Exception as e:
        print(f"Erreur de lecture : {e}")
        return

    print(f" -> {len(df_data)} lignes à géolocaliser.")

    # 2. Chargement du Mapping (CP -> INSEE)
    print("Téléchargement du dictionnaire CP <-> INSEE...")
    try:
        r_map = requests.get(MAPPING_URL)
        mapping_data = r_map.json()

        # Dictionnaire : {CodePostal: [Liste de Codes INSEE]}
        cp_to_insee = {}
        for item in mapping_data:
            cp = item.get('codePostal')
            insee = item.get('codeCommune')
            if cp and insee:
                if cp not in cp_to_insee: cp_to_insee[cp] = []
                # On évite les doublons
                if insee not in cp_to_insee[cp]:
                    cp_to_insee[cp].append(insee)
    except Exception as e:
        print(f"Erreur mapping : {e}")
        return

    # 3. Chargement des Formes (GeoJSON)
    print("Téléchargement des formes des communes (GeoJSON)... Patience.")
    try:
        r_geo = requests.get(GEOJSON_URL)
        geojson = r_geo.json()

        # Dictionnaire : {CodeINSEE: Texte_WKT}
        insee_to_wkt = {}
        features = geojson['features']

        for f in tqdm(features, desc="Conversion Formes"):
            props = f['properties']
            code_insee = props.get('code')
            geometry = f['geometry']

            # Conversion en texte pour le CSV
            wkt = convert_geojson_to_wkt(geometry)
            if wkt and code_insee:
                insee_to_wkt[code_insee] = wkt

    except Exception as e:
        print(f"Erreur GeoJSON : {e}")
        return

    # 4. FUSION FINALE
    print("Assemblage final (Données + Formes)...")

    final_rows = []

    # Pour chaque ligne de vente (qui est liée à un Code Postal et une Année)
    for idx, row in tqdm(df_data.iterrows(), total=len(df_data), desc="Jointure"):
        cp = str(row['CodePostal']).zfill(5)

        # On cherche les communes associées à ce Code Postal
        target_insees = cp_to_insee.get(cp, [])

        if not target_insees:
            # Cas rare : CP inconnu ou sans commune mappée -> On garde le point GPS par défaut
            row_dict = row.to_dict()
            row_dict['Geometry'] = None  # Kepler utilisera Lat/Lon par défaut
            final_rows.append(row_dict)
            continue

        # Si on trouve des communes, on duplique la donnée pour colorier toute la zone
        # (Ex: Si CP 33000 = Bordeaux + Talence, on colorie les deux polygones avec les données du 33000)
        found_polygon = False
        for insee in target_insees:
            if insee in insee_to_wkt:
                row_dict = row.to_dict()
                row_dict['Code_INSEE'] = insee
                row_dict['Geometry'] = insee_to_wkt[insee]  # LA FORME !
                final_rows.append(row_dict)
                found_polygon = True

        # Si on a trouvé des codes INSEE mais qu'ils n'ont pas de forme dans le GeoJSON
        if not found_polygon:
            row_dict = row.to_dict()
            row_dict['Geometry'] = None
            final_rows.append(row_dict)

    # 5. Export
    df_final = pd.DataFrame(final_rows)
    print(f"Écriture du fichier final ({len(df_final)} lignes)...")
    df_final.to_csv(OUTPUT_FILE, index=False)

    print(f"\n✅ TERMINÉ ! Fichier généré : {OUTPUT_FILE}")
    print("Instructions Kepler.gl :")
    print("1. Glissez ce fichier.")
    print("2. Kepler détecte la colonne 'Geometry' automatiquement.")
    print("3. Dans Layers, vérifiez que c'est bien 'Polygon'.")
    print("4. Mettez 'Total_Kg_An' en couleur de remplissage.")


if __name__ == "__main__":
    merge_geometry()