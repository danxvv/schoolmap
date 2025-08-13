# School Map Viewer

An interactive Streamlit application for visualizing school locations across Mexico using coordinate data and metadata.

## Features

- =ú Interactive map display using Folium
- = Search and filter schools by code, region, and education level
- =Ê Statistics dashboard showing school counts and distribution
- =Ë Data table view with download functionality
- <ë Detailed school information in map popups

## Data Sources

- **Coordinates**: `ct_codes_coords_googlelinks.txt` - Contains school codes with latitude/longitude coordinates
- **Metadata**: `EDUACION ESPECIAL FEDERAL(Detalle2).csv` - Contains school names, locations, levels, and regions

## Installation

1. Ensure you have Python 3.13+ installed
2. Install dependencies using uv:
   ```bash
   uv sync
   ```

## Running the Application

```bash
streamlit run streamlit_app.py
```

The application will be available at `http://localhost:8501`

## Usage

1. **View Schools**: The map displays all schools with blue markers by default
2. **Search**: Use the sidebar to search by school code
3. **Filter**: Filter schools by region (CORDE) or education level
4. **Details**: Click on map markers to see school information
5. **Data Table**: Check "Show Data Table" to view tabular data
6. **Export**: Download filtered data as CSV

## Data Statistics

- Total schools: 211 unique school codes
- Total coordinate entries: 389 (includes duplicates)
- Geographic coverage: Mexico (coordinates validated within country bounds)

## Technical Details

- Built with Streamlit and Folium
- Data caching for improved performance
- Responsive design with sidebar filters
- Error handling for data loading issues
- UTF-8 encoding support for special characters