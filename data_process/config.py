import os

# =============================================================================
# CONFIGURATION
# =============================================================================

# Directory containing the raw CSV files
DATA_DIR = "../data/montreuil"

# Directory where processed GeoJSON files will be saved
PROCESSED_DIR = "../output"
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Target CRS (e.g., Lambert-93)
TARGET_CRS = "EPSG:2154"

# Configuration for each CSV data layer.
csv_files = {
    "postes_source": {
        "file_path": "poste-source.csv",
    },
    "postes_electrique": {
        "file_path": "poste-electrique.csv",
    },
    "position_geographique": {
        "file_path": "position-geographique-des-poteaux-hta-et-bt.csv",
    },
    "reseau_souterrain_bt": {
        "file_path": "reseau-souterrain-bt.csv",
    },
    "reseau_bt": {
        "file_path": "reseau-bt.csv",
    },
    "reseau_souterrain_hta": {
        "file_path": "reseau-souterrain-hta.csv",
    },
}
