import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import requests
from shapely.geometry import shape

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Observatoire National Pesticides")

# Constantes
ANNEE_CIBLE = '2023'
INPUT_CSV = 'data/Achats-de-produits-phytosanitaires-a-lechelle-du-code-postal-.2025-06.csv'

# URLs Géo
# Niveau 1 : Départements (Léger pour la vue France)
GEOJSON_DEPTS = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson"
# Niveau 2 : Communes (Lourd, on filtrera)
GEOJSON_COMMUNES = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes-version-simplifiee.geojson"
MAPPING_URL = "https://unpkg.com/codes-postaux@4.0.0/codes-postaux.json"

# Base Toxico (Version Pro - Sans Emojis)
TOX_DB = {
    '1071-83-6': {'Nom': 'GLYPHOSATE', 'Danger': 'CMR SUSPECTE', 'Score': 90},
    '52888-80-9': {'Nom': 'PROSULFOCARBE', 'Danger': 'POLLUANT EAU', 'Score': 60},
    '133-07-3': {'Nom': 'FOLPEL', 'Danger': 'CMR SUSPECTE', 'Score': 90},
    '133-06-2': {'Nom': 'CAPTANE', 'Danger': 'CMR SUSPECTE', 'Score': 90},
    '203313-25-1': {'Nom': 'SPIROTETRAMAT', 'Danger': 'REPROTOXIQUE', 'Score': 100},
    'NON_TROUVE': {'Nom': 'AUTRE', 'Danger': 'NON CLASSE', 'Score': 0}
}

def resolve_tox(cas):
    return TOX_DB.get(cas, {'Nom': f'CAS {cas}', 'Danger': 'NON CLASSE', 'Score': 0})

# --- GESTION DE L'ÉTAT (SESSION STATE) ---
if 'selected_dept' not in st.session_state:
    st.session_state['selected_dept'] = None

def reset_view():
    st.session_state['selected_dept'] = None

# --- CHARGEMENT DES DONNÉES ---
@st.cache_data
def load_national_data():
    """Charge et agrège les données pour toute la France"""
    
    # 1. Chargement Référentiels Géo
    geo_depts = requests.get(GEOJSON_DEPTS).json()
    geo_communes = requests.get(GEOJSON_COMMUNES).json()
    
    # Indexation spatiale
    dept_index = {f['properties']['code']: f for f in geo_depts['features']}
    
    commune_index = {} # {INSEE: {area, nom, geometry, dept_code}}
    for f in geo_communes['features']:
        code = f['properties']['code']
        if f.get('geometry'):
            commune_index[code] = {
                'area': shape(f['geometry']).area,
                'nom': f['properties']['nom'],
                'geom': f,
                'dept': code[:2] # Les 2 premiers chiffres = Dept
            }

    # 2. Mapping CP
    raw_map = requests.get(MAPPING_URL).json()
    cp_map = {}
    for item in raw_map:
        cp, insee = item.get('codePostal'), item.get('codeCommune')
        if cp and insee and insee in commune_index:
            if cp not in cp_map: cp_map[cp] = []
            if insee not in cp_map[cp]: cp_map[cp].append(insee)

    # 3. Lecture CSV
    cols = pd.read_csv(INPUT_CSV, sep=';', nrows=1, encoding='latin-1').columns
    c_cp = next(c for c in cols if 'postal' in c.lower())
    c_qty = next(c for c in cols if 'quantit' in c.lower())
    c_annee = next(c for c in cols if 'annee' in c.lower())
    c_cas = next(c for c in cols if 'cas' in c.lower())

    df = pd.read_csv(INPUT_CSV, sep=';', encoding='latin-1', 
                     usecols=[c_cp, c_qty, c_annee, c_cas], dtype={c_cp: str})
    
    df = df[df[c_annee].astype(str) == ANNEE_CIBLE]
    df['qty'] = pd.to_numeric(df[c_qty].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    df = df[df['qty'] > 0]

    # 4. Ventilation & Agrégation
    # On prépare deux datasets : un par Dept (pour la vue nationale) et un détaillé
    
    # Agrégation brute par CP
    gb = df.groupby([c_cp, c_cas])['qty'].sum().reset_index()
    
    data_by_dept = {}     # {DeptCode: TotalVolume}
    data_by_commune = []  # Liste détaillée
    
    for row in gb.itertuples():
        cp = getattr(row, c_cp).split('.')[0].strip().zfill(5)
        cas = getattr(row, c_cas)
        qty = getattr(row, 'qty')
        
        targets = cp_map.get(cp, [])
        if not targets: continue
        
        total_area = sum([commune_index[i]['area'] for i in targets])
        if total_area == 0: continue
        
        for insee in targets:
            ratio = commune_index[insee]['area'] / total_area
            val = qty * ratio
            dept_code = insee[:2]
            
            # Ajout au total départemental
            data_by_dept[dept_code] = data_by_dept.get(dept_code, 0) + val
            
            # Ajout au détail communal
            data_by_commune.append({
                'INSEE': insee,
                'Commune': commune_index[insee]['nom'],
                'Dept': dept_code,
                'CAS': cas,
                'Volume': val
            })
            
    df_communes = pd.DataFrame(data_by_commune)
    df_depts = pd.DataFrame(list(data_by_dept.items()), columns=['Dept', 'Volume'])
    
    return df_depts, df_communes, dept_index, commune_index

# --- INTERFACE ---

def main():
    st.title(f"OBSERVATOIRE NATIONAL DES PESTICIDES {ANNEE_CIBLE}")
    st.markdown(
        """
        <div style='background-color:#fff3cd;padding:10px;border-radius:6px;border:1px solid #ffeeba;margin-bottom:15px;'>
        <b>Note scientifique :</b> Les données présentées ici sont issues des achats de pesticides (et non de la consommation directe). Selon l’INRAE : <i>« Les quantités de produits phytopharmaceutiques achetées sont très proches de celles effectivement utilisées à l’échelle nationale, car la quasi-totalité des produits achetés est utilisée la même année. »</i><br>
        <a href='https://www.inrae.fr/actualites/pesticides-france-etat-lieux-perspectives' target='_blank'>INRAE - Les pesticides en France : état des lieux et perspectives</a>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    with st.spinner("Chargement des données nationales (cela peut prendre quelques secondes)..."):
        df_depts, df_communes, geo_depts, geo_communes = load_national_data()

    # --- ÉCRAN 1 : VUE NATIONALE (Si aucun département sélectionné) ---
    if st.session_state['selected_dept'] is None:
        
        st.header("Vue Nationale : Analyse par Département")
        st.markdown("Cliquez sur un département sur la carte pour voir le détail des communes.")
        
        col_map, col_stats = st.columns([2, 1])
        
        with col_map:
            # Carte des Départements
            m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="CartoDB positron")
            
            cp = folium.Choropleth(
                geo_data={"type": "FeatureCollection", "features": list(geo_depts.values())},
                data=df_depts,
                columns=['Dept', 'Volume'],
                key_on='feature.properties.code',
                fill_color='YlOrRd',
                fill_opacity=0.7,
                line_opacity=0.2,
                legend_name='Volume Total (kg)',
                name="Departements"
            ).add_to(m)
            
            # Gestion du Clic sur la carte
            # On utilise GeoJson pour rendre les polygones cliquables via st_folium
            folium.GeoJson(
                {"type": "FeatureCollection", "features": list(geo_depts.values())},
                style_function=lambda x: {'fillColor': '#00000000', 'color': '#00000000'}, # Invisible
                tooltip=folium.GeoJsonTooltip(fields=['nom', 'code'], aliases=['Département:', 'Code:'])
            ).add_to(m)

            map_output = st_folium(m, width=None, height=600)
            
            # Détection du clic
            if map_output['last_object_clicked_tooltip']:
                # Le tooltip contient "Département: Ain \n Code: 01"
                # On extrait le code (c'est un peu brut mais fonctionnel)
                try:
                    clicked_text = map_output['last_object_clicked_tooltip']
                    # On suppose que le code est la dernière partie ou on le récupère via l'objet properties
                    # Méthode plus robuste : si on clique, st_folium renvoie l'objet feature
                    if map_output['last_active_drawing']:
                        code_dept = map_output['last_active_drawing']['properties']['code']
                        st.session_state['selected_dept'] = code_dept
                        st.rerun() # On recharge la page pour passer à l'écran 2
                except:
                    pass

        with col_stats:
            st.subheader("Classement Départemental")
            df_depts_sorted = df_depts.sort_values('Volume', ascending=False).head(15)
            
            st.dataframe(
                df_depts_sorted,
                column_config={
                    "Volume": st.column_config.ProgressColumn(
                        "Volume (kg)",
                        format="%.0f kg",
                        min_value=0,
                        max_value=float(df_depts['Volume'].max())
                    )
                },
                hide_index=True,
                use_container_width=True
            )

    # --- ÉCRAN 2 : VUE DÉPARTEMENTALE (Si un département est sélectionné) ---
    else:
        dept_code = st.session_state['selected_dept']
        dept_name = geo_depts[dept_code]['properties']['nom']
        
        # Bouton Retour
        if st.button("⬅️ Retour à la carte de France"):
            reset_view()
            st.rerun()

        st.header(f"Détail Département : {dept_name} ({dept_code})")
        
        # Filtrage des données pour ce dept
        df_local = df_communes[df_communes['Dept'] == dept_code]
        # Agrégation par commune pour la carte
        map_local_data = df_local.groupby(['INSEE', 'Commune'])['Volume'].sum().reset_index()
        
        col_local_map, col_local_details = st.columns([2, 1])
        
        with col_local_map:
            # On récupère les géométries de ce département uniquement
            features_local = [geo_communes[i]['geom'] for i in map_local_data['INSEE'] if i in geo_communes]
            
            if not features_local:
                st.warning("Pas de géométries disponibles pour ce département.")
            else:
                # Centrage de la carte
                # On prend la première commune comme centre approx
                centroid = shape(features_local[0]['geometry']).centroid
                
                m_local = folium.Map(location=[centroid.y, centroid.x], zoom_start=9, tiles="CartoDB positron")
                
                folium.Choropleth(
                    geo_data={"type": "FeatureCollection", "features": features_local},
                    data=map_local_data,
                    columns=['INSEE', 'Volume'],
                    key_on='feature.properties.code',
                    fill_color='YlOrRd',
                    fill_opacity=0.7,
                    line_opacity=0.1,
                    legend_name='Volume (kg)'
                ).add_to(m_local)
                
                # Couche interactive pour le clic commune
                folium.GeoJson(
                    {"type": "FeatureCollection", "features": features_local},
                    style_function=lambda x: {'fillColor': '#00000000', 'color': '#00000000'},
                    tooltip=folium.GeoJsonTooltip(fields=['nom'], aliases=['Commune:'])
                ).add_to(m_local)
                
                local_map_output = st_folium(m_local, width=None, height=600)

        with col_local_details:
            st.subheader("Analyse Communale")
            
            selected_commune = None
            # Détection clic commune
            if local_map_output['last_active_drawing']:
                clicked_insee = local_map_output['last_active_drawing']['properties']['code']
                selected_commune = clicked_insee
            
            if selected_commune:
                commune_name = geo_communes[selected_commune]['nom']
                st.markdown(f"### 📍 {commune_name}")
                
                # Données de la commune
                data_ville = df_local[df_local['INSEE'] == selected_commune]
                total_vol = data_ville['Volume'].sum()
                st.metric("Volume Total", f"{total_vol:,.1f} kg")
                
                st.markdown("#### Substances Achetées")
                
                # Agrégation par CAS
                prods = data_ville.groupby('CAS')['Volume'].sum().reset_index()
                prods['Nom'] = prods['CAS'].apply(lambda x: resolve_tox(x)['Nom'])
                prods['Danger'] = prods['CAS'].apply(lambda x: resolve_tox(x)['Danger'])
                prods = prods.sort_values('Volume', ascending=False)
                
                # Top 5
                st.dataframe(
                    prods.head(5)[['Nom', 'Danger', 'Volume']],
                    hide_index=True,
                    use_container_width=True
                )
                
                # Lire la suite
                if len(prods) > 5:
                    with st.expander(f"Lire la suite ({len(prods)-5} autres)"):
                        st.dataframe(
                            prods.iloc[5:][['Nom', 'Danger', 'Volume']],
                            hide_index=True,
                            use_container_width=True
                        )
                        
                        csv = prods.to_csv(index=False).encode('utf-8')
                        st.download_button("Télécharger CSV", csv, f"Data_{commune_name}.csv")
            else:
                st.info("Cliquez sur une commune de la carte pour voir son bilan toxicologique.")

if __name__ == "__main__":
    main()