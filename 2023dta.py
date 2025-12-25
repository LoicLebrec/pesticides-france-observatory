import pandas as pd
import requests
import plotly.graph_objects as go
from shapely.geometry import shape
from tqdm import tqdm
import json

# --- CONFIGURATION ---
ANNEE_CIBLE = '2023'
INPUT_CSV = 'data/Achats-de-produits-phytosanitaires-a-lechelle-du-code-postal-.2025-06.csv'
OUTPUT_HTML = f'Observatoire_National_Pesticides_{ANNEE_CIBLE}.html'

# URL optimisée (Géométrie simplifiée pour affichage national fluide)
GEOJSON_URL = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes-version-simplifiee.geojson"
MAPPING_URL = "https://unpkg.com/codes-postaux@4.0.0/codes-postaux.json"

# --- BASE TOXICOLOGIQUE (TOP 50 FRANCE) ---
TOX_DB = {
    '1071-83-6': {'Nom': 'GLYPHOSATE', 'Danger': '[CMR SUSPECTÉ]'},
    '52888-80-9': {'Nom': 'PROSULFOCARBE', 'Danger': '[POLLUANT EAU]'},
    '133-07-3': {'Nom': 'FOLPEL', 'Danger': '[CMR SUSPECTÉ]'},
    '133-06-2': {'Nom': 'CAPTANE', 'Danger': '[CMR SUSPECTÉ]'},
    '203313-25-1': {'Nom': 'SPIROTETRAMAT', 'Danger': '[REPROTOXIQUE]'},
    '35554-44-0': {'Nom': 'IMAZALIL', 'Danger': '[CMR SUSPECTÉ]'},
    '119446-68-3': {'Nom': 'DIFENOCONAZOLE', 'Danger': '[PERTURBATEUR]'},
    '51218-45-2': {'Nom': 'S-METOLACHLORE', 'Danger': '[CMR SUSPECTÉ]'},
    '107-18-6': {'Nom': 'ALCOOL ALLYLIQUE', 'Danger': '[MORTEL]'},
    '7704-34-9': {'Nom': 'SOUFRE', 'Danger': '[IRRITANT]'},
    '124-40-3': {'Nom': 'DIMETHYLAMINE', 'Danger': '[IRRITANT]'},
    '1582-09-8': {'Nom': 'TRIFLURALINE', 'Danger': '[POLLUANT EAU]'}
    # (Liste à compléter selon besoins)
}

def resolve_info(cas):
    if cas in TOX_DB:
        return TOX_DB[cas]['Nom'], TOX_DB[cas]['Danger']
    return f"CAS {cas}", "[NON CLASSÉ]"

def main():
    print("--- GÉNÉRATION NATIONALE (FRANCE ENTIÈRE) ---")
    
    # 1. GÉOGRAPHIE
    print("1. Téléchargement et indexation de la France (patience)...")
    try:
        geo_raw = requests.get(GEOJSON_URL).json()
    except Exception as e:
        print(f"Erreur téléchargement GeoJSON: {e}")
        return

    geo_index = {} # {INSEE: {'area': float, 'nom': str}}
    # On garde toutes les communes de France
    for f in tqdm(geo_raw['features'], desc="Indexation Géo"):
        code = f['properties']['code']
        if f.get('geometry'):
            # Calcul surface pour ventilation
            area = shape(f['geometry']).area
            geo_index[code] = {'area': area, 'nom': f['properties']['nom']}

    # 2. MAPPING CP -> INSEE
    print("2. Chargement du mapping Codes Postaux...")
    raw_map = requests.get(MAPPING_URL).json()
    cp_map = {}
    for item in raw_map:
        cp, insee = item.get('codePostal'), item.get('codeCommune')
        if cp and insee and insee in geo_index:
            if cp not in cp_map: cp_map[cp] = []
            if insee not in cp_map[cp]: cp_map[cp].append(insee)

    # 3. TRAITEMENT DONNÉES
    print("3. Lecture et Ventilation (C'est l'étape la plus longue)...")
    commune_stats = {} 
    
    cols = pd.read_csv(INPUT_CSV, sep=';', nrows=1, encoding='latin-1').columns
    c_cp = next(c for c in cols if 'postal' in c.lower())
    c_qty = next(c for c in cols if 'quantit' in c.lower())
    c_annee = next(c for c in cols if 'annee' in c.lower())
    c_cas = next(c for c in cols if 'cas' in c.lower())

    chunk_size = 100000 # Gros blocs pour aller vite
    reader = pd.read_csv(INPUT_CSV, sep=';', encoding='latin-1', chunksize=chunk_size, 
                         low_memory=False, on_bad_lines='skip', dtype={c_cp: str})

    for chunk in tqdm(reader, desc="Traitement CSV"):
        chunk = chunk[chunk[c_annee].astype(str) == ANNEE_CIBLE].copy()
        if chunk.empty: continue
        
        chunk['qty'] = pd.to_numeric(chunk[c_qty].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        chunk = chunk[chunk['qty'] > 0]
        # On ne filtre plus par département, on prend tout ce qui est mappé
        chunk = chunk[chunk[c_cp].isin(cp_map.keys())]
        
        if chunk.empty: continue

        for row in chunk.itertuples():
            cp = getattr(row, c_cp)
            qty = getattr(row, 'qty')
            cas = getattr(row, c_cas)
            
            targets = cp_map.get(cp, [])
            if not targets: continue
            
            # Surface totale du CP (somme des communes le composant)
            total_area = sum([geo_index[i]['area'] for i in targets])
            if total_area == 0: continue
            
            for insee in targets:
                ratio = geo_index[insee]['area'] / total_area
                val = qty * ratio
                
                if insee not in commune_stats:
                    commune_stats[insee] = {'Total': 0.0, 'Prods': {}}
                
                commune_stats[insee]['Total'] += val
                commune_stats[insee]['Prods'][cas] = commune_stats[insee]['Prods'].get(cas, 0) + val

    # 4. PRÉPARATION VISUALISATION
    print("4. Préparation des données pour la carte...")
    ids = []
    z_vals = []
    hover_texts = []
    table_rows = []

    # On ne garde que les communes ayant des données pour alléger
    valid_communes = [c for c in geo_raw['features'] if c['properties']['code'] in commune_stats]
    
    # Création du GeoJSON filtré pour Plotly (pour ne pas dessiner les communes vides)
    filtered_geojson = {"type": "FeatureCollection", "features": valid_communes}

    for f in tqdm(valid_communes, desc="Génération Tooltips"):
        insee = f['properties']['code']
        data = commune_stats[insee]
        total = data['Total']
        
        ids.append(insee)
        z_vals.append(total)

        # Top 5 pour l'infobulle
        sorted_prods = sorted(data['Prods'].items(), key=lambda x: x[1], reverse=True)
        top_5 = sorted_prods[:5]
        
        tooltip = f"<b>{geo_index[insee]['nom']}</b><br>"
        tooltip += f"TOTAL : {total:,.0f} kg<br><hr style='margin:2px;'>"
        
        for i, (cas, q) in enumerate(top_5):
            nom, danger = resolve_info(cas)
            color = "#e74c3c" if "CMR" in danger or "MORTEL" in danger else "#2c3e50"
            tooltip += f"{i+1}. {nom} <b style='color:{color}'>{danger}</b><br>"
            
        hover_texts.append(tooltip)

        # Données pour le tableau (Seulement si > 10kg pour alléger le HTML)
        if total > 10:
            details_str = ", ".join([f"{resolve_info(c)[0]} ({q:.0f}kg)" for c, q in top_5])
            table_rows.append({
                'Commune': geo_index[insee]['nom'],
                'Code INSEE': insee,
                'Volume (kg)': round(total, 1),
                'Top Substances': details_str
            })

    # 5. CARTE PLOTLY
    print("5. Assemblage HTML...")
    fig = go.Figure(go.Choroplethmapbox(
        geojson=filtered_geojson,
        locations=ids,
        z=z_vals,
        featureidkey="properties.code",
        colorscale="Reds", # Ou "Turbo" pour plus de contraste
        text=hover_texts,
        hoverinfo="text",
        marker_opacity=0.7,
        marker_line_width=0,
        name="Pesticides"
    ))

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=5,
        mapbox_center={"lat": 46.5, "lon": 2.5}, # Centre France
        margin={"r":0,"t":0,"l":0,"b":0},
        height=700
    )

    plotly_html = fig.to_html(include_plotlyjs='cdn', full_html=False)

    # 6. TABLEAU HTML
    df_table = pd.DataFrame(table_rows).sort_values('Volume (kg)', ascending=False)
    # On limite le tableau aux 5000 premières lignes pour ne pas crasher le navigateur
    # (L'utilisateur utilise la recherche pour trouver sa ville si elle est majeure)
    df_display = df_table.head(5000)
    
    html_table = df_display.to_html(classes='display compact', index=False, table_id='fullTable')

    full_html = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <title>Observatoire National Pesticides {ANNEE_CIBLE}</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
        <style>
            body {{ font-family: 'Arial', sans-serif; margin: 0; padding: 20px; background: #f4f4f9; }}
            h1 {{ text-align: center; color: #333; }}
            .container {{ max-width: 1400px; margin: auto; background: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
            .warning {{ color: #e74c3c; font-size: 12px; text-align: center; margin-bottom: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>CARTOGRAPHIE NATIONALE {ANNEE_CIBLE}</h1>
            <div class="warning">Données ventilées par surface communale. Affichage des 5000 plus gros volumes dans le tableau.</div>
            
            {plotly_html}
            
            <h2 style="margin-top:30px;">Détails par Commune (Top 5000)</h2>
            {html_table}
        </div>
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
        <script>
            $(document).ready(function () {{
                $('#fullTable').DataTable({{
                    "pageLength": 10,
                    "order": [[ 2, "desc" ]],
                    "language": {{ "url": "//cdn.datatables.net/plug-ins/1.13.4/i18n/fr-FR.json" }}
                }});
            }});
        </script>
    </body>
    </html>
    """
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(full_html)
        
    print(f"✅ FICHIER NATIONAL CRÉÉ : {OUTPUT_HTML}")
    print("Attention : Le fichier peut faire 50-80 Mo. Envoyez-le via WeTransfer ou un lien Drive.")

if __name__ == "__main__":
    main()