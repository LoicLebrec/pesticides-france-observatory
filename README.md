# Pesticides France Observatory


This project generates a national map and interactive table of pesticide usage in France for 2023, using public CSV and GeoJSON data. The output is a standalone HTML file with a Plotly map and searchable table.

**Scientific note:**
> The data presented here are based on pesticide purchases ("achats") and not direct consumption. However, according to INRAE research, purchases are an excellent proxy for actual use at the national scale: "Les quantités de produits phytopharmaceutiques achetées sont très proches de celles effectivement utilisées à l’échelle nationale, car la quasi-totalité des produits achetés est utilisée la même année."  
> Source: [INRAE - Les pesticides en France : état des lieux et perspectives](https://www.inrae.fr/actualites/pesticides-france-etat-lieux-perspectives)

## Features
- Downloads and processes national pesticide purchase data by postal code
- Maps data to French communes using GeoJSON and postal code mappings
- Visualizes total pesticide volume per commune on an interactive map
- Provides a searchable, sortable table of the top 5000 communes by volume

## Usage
1. Place the required CSV data in the `data/` directory (see script for expected filenames).
2. Run the script:
   ```bash
   python 2023dta.py
   ```
3. The output HTML file will be generated in the project root.

## Requirements
- Python 3.8+
- See `datacreation/requirements.txt` for dependencies

## Data Sources
- [GeoJSON communes France](https://github.com/gregoiredavid/france-geojson)
- [Codes postaux](https://unpkg.com/codes-postaux)
- Official pesticide purchase CSV (see data/ directory)

## License
MIT
