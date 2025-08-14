import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import re
from typing import Dict, List, Tuple, Optional

st.set_page_config(
    page_title="School Map Viewer",
    page_icon="🏫",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_coordinates_data() -> pd.DataFrame:
    """Load and parse the coordinates data from the text file."""
    try:
        with open('ct_codes_coords_googlelinks_federal_primaria.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        data = []
        for line in lines:
            line = line.strip()
            if line:
                # Parse format: SCHOOL_CODE-LATITUDE,LONGITUDE
                match = re.match(r'([^-]+)-(-?\d+\.?\d*),(-?\d+\.?\d*)', line)
                if match:
                    school_code = match.group(1)
                    latitude = float(match.group(2))
                    longitude = float(match.group(3))
                    data.append({
                        'school_code': school_code,
                        'latitude': latitude,
                        'longitude': longitude
                    })
        
        df = pd.DataFrame(data)
        
        # Remove duplicates, keeping first occurrence
        df = df.drop_duplicates(subset=['school_code'], keep='first')
        
        return df
    
    except Exception as e:
        st.error(f"Error loading coordinates data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_school_metadata() -> pd.DataFrame:
    """Load school metadata from CSV file."""
    try:
        # Try different encodings
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                df = pd.read_csv('PRIMARIA FEDERAL(PRIMARIA FEDERAL).csv', encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            st.warning("Could not read CSV with any standard encoding")
            return pd.DataFrame()
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        # Rename key columns for consistency
        if 'CLAVE CT' in df.columns:
            df = df.rename(columns={'CLAVE CT': 'school_code'})
        
        return df
    
    except Exception as e:
        st.error(f"Error loading school metadata: {e}")
        return pd.DataFrame()

@st.cache_data
def merge_school_data() -> pd.DataFrame:
    """Merge coordinates and metadata."""
    coords_df = load_coordinates_data()
    metadata_df = load_school_metadata()
    
    if coords_df.empty:
        return pd.DataFrame()
    
    if not metadata_df.empty and 'school_code' in metadata_df.columns:
        # Merge on school code
        merged_df = coords_df.merge(metadata_df, on='school_code', how='left')
    else:
        merged_df = coords_df
    
    # Validate coordinates (Mexico bounds approximately)
    merged_df = merged_df[
        (merged_df['latitude'] >= 14) & (merged_df['latitude'] <= 33) &
        (merged_df['longitude'] >= -118) & (merged_df['longitude'] <= -86)
    ]
    
    return merged_df

def create_map(df: pd.DataFrame, selected_schools: List[str] = None) -> folium.Map:
    """Create a folium map with school markers."""
    if df.empty:
        # Default map centered on Mexico
        m = folium.Map(location=[23.6345, -102.5528], zoom_start=6)
        return m
    
    # Calculate center of visible schools
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=8)
    
    # Add markers for each school
    for idx, row in df.iterrows():
        # Create popup content
        popup_content = f"""
        <b>School Code:</b> {row['school_code']}<br>
        <b>Coordinates:</b> {row['latitude']:.6f}, {row['longitude']:.6f}
        """
        
        # Add metadata if available
        if 'NOMBRE CT' in row and pd.notna(row['NOMBRE CT']):
            popup_content += f"<br><b>Name:</b> {row['NOMBRE CT']}"
        if 'LOCALIDAD CT' in row and pd.notna(row['LOCALIDAD CT']):
            popup_content += f"<br><b>Location:</b> {row['LOCALIDAD CT']}"
        if 'CORDE' in row and pd.notna(row['CORDE']):
            popup_content += f"<br><b>Region:</b> {row['CORDE']}"
        if 'NIVEL' in row and pd.notna(row['NIVEL']):
            popup_content += f"<br><b>Level:</b> {row['NIVEL']}"
        
        # Add Google Maps button
        google_maps_url = f"https://www.google.com/maps/place/{row['latitude']},{row['longitude']}"
        popup_content += f"""<br><br>
        <a href="{google_maps_url}" target="_blank" style="
            background-color: #4285f4; 
            color: white; 
            padding: 8px 12px; 
            text-decoration: none; 
            border-radius: 4px; 
            font-size: 12px;
            display: inline-block;
        ">📍 Open in Google Maps</a>
        """
        
        # Determine marker color
        color = 'red' if selected_schools and row['school_code'] in selected_schools else 'blue'
        
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=f"School: {row['school_code']}",
            icon=folium.Icon(color=color, icon='graduation-cap', prefix='fa')
        ).add_to(m)
    
    return m

def main():
    st.title("🏫 School Map Viewer")
    st.markdown("Interactive map displaying school locations across Mexico")
    
    # Load data
    with st.spinner("Loading school data..."):
        df = merge_school_data()
    
    if df.empty:
        st.error("No school data could be loaded. Please check the data files.")
        return
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # School code search
    search_code = st.sidebar.text_input("Search by School Code:", placeholder="e.g. 21DPR0653I")
    
    # Region filter
    regions = ['All'] + sorted(df['CORDE'].dropna().unique().tolist()) if 'CORDE' in df.columns else ['All']
    selected_region = st.sidebar.selectbox("Select Region:", regions)
    
    # Level filter
    levels = ['All'] + sorted(df['NIVEL'].dropna().unique().tolist()) if 'NIVEL' in df.columns else ['All']
    selected_level = st.sidebar.selectbox("Select Education Level:", levels)
    
    # Apply filters
    filtered_df = df.copy()
    
    if search_code:
        filtered_df = filtered_df[filtered_df['school_code'].str.contains(search_code, case=False, na=False)]
    
    if selected_region != 'All' and 'CORDE' in df.columns:
        filtered_df = filtered_df[filtered_df['CORDE'] == selected_region]
    
    if selected_level != 'All' and 'NIVEL' in df.columns:
        filtered_df = filtered_df[filtered_df['NIVEL'] == selected_level]
    
    # Statistics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Schools", len(df))
    
    with col2:
        st.metric("Displayed Schools", len(filtered_df))
    
    with col3:
        unique_regions = df['CORDE'].nunique() if 'CORDE' in df.columns else 0
        st.metric("Regions", unique_regions)
    
    with col4:
        unique_levels = df['NIVEL'].nunique() if 'NIVEL' in df.columns else 0
        st.metric("Education Levels", unique_levels)
    
    # Create and display map
    if not filtered_df.empty:
        st.subheader("📍 School Locations")
        
        # Create map
        school_map = create_map(filtered_df)
        
        # Display map
        map_data = st_folium(school_map, width=700, height=500)
        
        # Display filtered data table
        if st.checkbox("Show Data Table", value=False):
            st.subheader("📊 School Data")
            
            # Select columns to display
            display_columns = ['school_code', 'latitude', 'longitude']
            if 'NOMBRE CT' in filtered_df.columns:
                display_columns.append('NOMBRE CT')
            if 'LOCALIDAD CT' in filtered_df.columns:
                display_columns.append('LOCALIDAD CT')
            if 'CORDE' in filtered_df.columns:
                display_columns.append('CORDE')
            if 'NIVEL' in filtered_df.columns:
                display_columns.append('NIVEL')
            
            st.dataframe(
                filtered_df[display_columns].reset_index(drop=True),
                use_container_width=True
            )
            
            # Download filtered data
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data as CSV",
                data=csv,
                file_name=f"school_data_filtered_{len(filtered_df)}_schools.csv",
                mime="text/csv"
            )
    
    else:
        st.warning("No schools match the current filters. Try adjusting your search criteria.")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data Sources:**")
    st.sidebar.markdown("- Coordinates: ct_codes_coords_googlelinks_federal_primaria.txt")
    st.sidebar.markdown("- Metadata: PRIMARIA FEDERAL(PRIMARIA FEDERAL).csv")

if __name__ == "__main__":
    main()