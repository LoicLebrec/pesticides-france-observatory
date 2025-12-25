# Pesticides France Observatory

This project generates a national map and interactive table of pesticide usage in France for 2023, using public CSV and GeoJSON data. The output is a standalone HTML file with a Plotly map and searchable table.

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
