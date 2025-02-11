import os
import json
import logging
import time
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points  # Optional: not used in this snippet

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_DIR = "./data"
PROCESSED_DIR = "./output"
CONNECTION_RADIUS_METERS = 10  # adjust as needed

# Define your layer configurations.
# (Make sure your GeoJSON files have an "id" property for each feature.)
csv_files = {
    "postes_source": {
        "file_path": "postes_source.geojson",
        "layer_name": "Postes Source",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 10,
    },
    "postes_electrique": {
        "file_path": "postes_electrique.geojson",
        "layer_name": "Postes Electrique",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 5,
    },
    "position_geographique": {
        "file_path": "position_geographique.geojson",
        "layer_name": "Position Geographique",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 5,
    },
    "reseau_souterrain_bt": {
        "file_path": "reseau_souterrain_bt.geojson",
        "layer_name": "Reseau Souterrain BT",
        "priority_connections": {
            "postes_source": 1,
            "postes_electrique": 2,
            "position_geographique": 3,
        },
        "mono_connection_per_endpoint": True,
        "radius": 5,
    },
    "reseau_bt": {
        "file_path": "reseau_bt.geojson",
        "layer_name": "Reseau BT",
        "priority_connections": {
            "postes_source": 1,
            "postes_electrique": 2,
            "position_geographique": 3,
        },
        "mono_connection_per_endpoint": True,
        "radius": 5,
    },
    "reseau_souterrain_hta": {
        "file_path": "reseau_souterrain_hta.geojson",
        "layer_name": "Reseau Souterrain HTA",
        "solo_connection_if": [  # When connecting, if the candidate is from one of these layers,
            "postes_source",  # then the endpoint should only connect to that one candidate.
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 5,
    },
}


def create_directory(directory: str) -> None:
    os.makedirs(directory, exist_ok=True)


def load_geojson_files():
    """
    Load all GeoJSON files into a dictionary of GeoDataFrames.
    Also add a 'source_layer' column based on the key.
    """
    all_layers = {}
    for layer, config in csv_files.items():
        file_path = os.path.join(PROCESSED_DIR, config["file_path"])
        logging.info(f"Loading GeoJSON file for layer '{layer}' from {file_path}...")
        try:
            gdf = gpd.read_file(file_path)
            print(f"{layer} : count of gdf : {len(gdf)}")
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            # Add a column to track the source layer.
            gdf["source_layer"] = layer
            all_layers[layer] = gdf
        except Exception as e:
            logging.error(f"Error loading GeoJSON file {file_path}: {e}")
            continue
    return all_layers


def find_connections(
    gdf,
    all_features,
    radius,
    exclude_list=None,
    priority_connections=None,
    mono_connection_per_endpoint=False,
):
    """
    For each feature in the GeoDataFrame, find nearby features within a given radius.

    - For Point features (or other non-LineString geometries), the connection is based on the
      geometry (or its centroid).

    - For LineString features, two separate buffers are computed:
      one around the starting point and one around the ending point.

      If mono_connection_per_endpoint is True and priority_connections is provided,
      then for each endpoint only a single candidate is chosen:
          * Filter candidates (by spatial query and exclude_list)
          * Further filter to those with source_layer in priority_connections.
          * Compute each candidate's distance from the endpoint.
          * Sort by priority (lowest number is highest priority) and then by distance.
          * Select the best candidate (if any).

      Additionally, if the current feature’s configuration (based on its source_layer)
      defines a "solo_connection_if" list, then for each endpoint the candidate set is
      first filtered to only those with a source_layer in that list. If any exist, then only
      the single closest candidate (by distance) is kept.

    The resulting GeoDataFrame will contain three new columns:
      • "connections" – for non-LineString features, and for LineStrings it equals the union of
                         the selected start and end connections,
      • "start_connections" – the connection(s) at the starting endpoint (only for LineStrings),
      • "end_connections" – the connection(s) at the ending endpoint (only for LineStrings).

    Any candidate whose 'source_layer' is in exclude_list is filtered out.
    """
    if exclude_list is None:
        exclude_list = []

    # Project to EPSG:3857 (meters) for distance calculations.
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    spatial_index = all_features_proj.sindex

    connections = []
    start_connections = []
    end_connections = []

    for idx, feature in gdf_proj.iterrows():
        geom = feature.geometry

        # For LineString features, process endpoints separately.
        if geom.geom_type == "LineString":
            start_point = Point(geom.coords[0])
            end_point = Point(geom.coords[-1])
            # Get the configuration for this feature’s layer.
            config = csv_files.get(feature["source_layer"], {})
            solo_list = config.get("solo_connection_if", None)

            # --- Process Start Endpoint ---
            buf_start = start_point.buffer(radius)
            start_possible_idx = list(spatial_index.intersection(buf_start.bounds))
            start_possible = all_features_proj.iloc[start_possible_idx]
            start_matches = start_possible[start_possible.intersects(buf_start)]
            if exclude_list:
                start_matches = start_matches[
                    ~start_matches["source_layer"].isin(exclude_list)
                ]

            # If solo_connection_if is defined, try to restrict to those candidates.
            if solo_list is not None:
                solo_candidates = start_matches[
                    start_matches["source_layer"].isin(solo_list)
                ]
                if not solo_candidates.empty:
                    solo_candidates = solo_candidates.copy()
                    solo_candidates["distance"] = solo_candidates.geometry.distance(
                        start_point
                    )
                    solo_candidates_sorted = solo_candidates.sort_values(by="distance")
                    start_ids = {solo_candidates_sorted.iloc[0]["id"]}
                else:
                    # Fall back to mono connection if enabled.
                    if mono_connection_per_endpoint and priority_connections:
                        candidates = start_matches[
                            start_matches["source_layer"].isin(
                                priority_connections.keys()
                            )
                        ]
                        if not candidates.empty:
                            candidates = candidates.copy()
                            candidates["distance"] = candidates.geometry.distance(
                                start_point
                            )
                            candidates["priority"] = candidates["source_layer"].map(
                                priority_connections
                            )
                            candidates_sorted = candidates.sort_values(
                                by=["priority", "distance"]
                            )
                            start_ids = {candidates_sorted.iloc[0]["id"]}
                        else:
                            start_ids = set()
                    else:
                        start_ids = set(start_matches["id"].tolist())
            else:
                if mono_connection_per_endpoint and priority_connections:
                    candidates = start_matches[
                        start_matches["source_layer"].isin(priority_connections.keys())
                    ]
                    if not candidates.empty:
                        candidates = candidates.copy()
                        candidates["distance"] = candidates.geometry.distance(
                            start_point
                        )
                        candidates["priority"] = candidates["source_layer"].map(
                            priority_connections
                        )
                        candidates_sorted = candidates.sort_values(
                            by=["priority", "distance"]
                        )
                        start_ids = {candidates_sorted.iloc[0]["id"]}
                    else:
                        start_ids = set()
                else:
                    start_ids = set(start_matches["id"].tolist())

            if feature["id"] in start_ids:
                start_ids.remove(feature["id"])

            # --- Process End Endpoint ---
            buf_end = end_point.buffer(radius)
            end_possible_idx = list(spatial_index.intersection(buf_end.bounds))
            end_possible = all_features_proj.iloc[end_possible_idx]
            end_matches = end_possible[end_possible.intersects(buf_end)]
            if exclude_list:
                end_matches = end_matches[
                    ~end_matches["source_layer"].isin(exclude_list)
                ]

            if solo_list is not None:
                solo_candidates = end_matches[
                    end_matches["source_layer"].isin(solo_list)
                ]
                if not solo_candidates.empty:
                    solo_candidates = solo_candidates.copy()
                    solo_candidates["distance"] = solo_candidates.geometry.distance(
                        end_point
                    )
                    solo_candidates_sorted = solo_candidates.sort_values(by="distance")
                    end_ids = {solo_candidates_sorted.iloc[0]["id"]}
                else:
                    if mono_connection_per_endpoint and priority_connections:
                        candidates = end_matches[
                            end_matches["source_layer"].isin(
                                priority_connections.keys()
                            )
                        ]
                        if not candidates.empty:
                            candidates = candidates.copy()
                            candidates["distance"] = candidates.geometry.distance(
                                end_point
                            )
                            candidates["priority"] = candidates["source_layer"].map(
                                priority_connections
                            )
                            candidates_sorted = candidates.sort_values(
                                by=["priority", "distance"]
                            )
                            end_ids = {candidates_sorted.iloc[0]["id"]}
                        else:
                            end_ids = set()
                    else:
                        end_ids = set(end_matches["id"].tolist())
            else:
                if mono_connection_per_endpoint and priority_connections:
                    candidates = end_matches[
                        end_matches["source_layer"].isin(priority_connections.keys())
                    ]
                    if not candidates.empty:
                        candidates = candidates.copy()
                        candidates["distance"] = candidates.geometry.distance(end_point)
                        candidates["priority"] = candidates["source_layer"].map(
                            priority_connections
                        )
                        candidates_sorted = candidates.sort_values(
                            by=["priority", "distance"]
                        )
                        end_ids = {candidates_sorted.iloc[0]["id"]}
                    else:
                        end_ids = set()
                else:
                    end_ids = set(end_matches["id"].tolist())

            if feature["id"] in end_ids:
                end_ids.remove(feature["id"])

            union_ids = start_ids.union(end_ids)
            connections.append(list(union_ids))
            start_connections.append(list(start_ids))
            end_connections.append(list(end_ids))
        else:
            # For non-LineString features.
            if geom.geom_type == "Point":
                endpoints = [geom]
            else:
                endpoints = [geom.centroid]
            candidate_ids = set()
            for endpoint in endpoints:
                buf = endpoint.buffer(radius)
                possible_idx = list(spatial_index.intersection(buf.bounds))
                possible = all_features_proj.iloc[possible_idx]
                matches = possible[possible.intersects(buf)]
                if exclude_list:
                    matches = matches[~matches["source_layer"].isin(exclude_list)]
                candidate_ids.update(matches["id"].tolist())
            if feature["id"] in candidate_ids:
                candidate_ids.remove(feature["id"])
            connections.append(list(candidate_ids))
            start_connections.append([])
            end_connections.append([])

    gdf_proj["connections"] = connections
    gdf_proj["start_connections"] = start_connections
    gdf_proj["end_connections"] = end_connections

    # Return the GeoDataFrame in its original CRS (EPSG:4326).
    gdf_final = gdf_proj.to_crs(epsg=4326)
    return gdf_final


def save_updated_layers(updated_layers):
    """
    Save each updated layer into GeoJSON files.
    """
    output_dir = PROCESSED_DIR + "_con"
    create_directory(output_dir)
    for layer, gdf in updated_layers.items():
        output_path = os.path.join(output_dir, f"{layer}.geojson")
        try:
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(f"Saved updated GeoJSON for layer '{layer}' to {output_path}.")
        except Exception as e:
            logging.error(f"Error writing GeoJSON file {output_path}: {e}")


if __name__ == "__main__":
    start_time = time.time()
    logging.info("Loading GeoJSON files into memory...")
    all_layers = load_geojson_files()
    print(f"Count of all_layers: {len(all_layers)}")

    logging.info("Merging all features from all layers...")
    # Merge all layers into one GeoDataFrame for spatial queries.
    # (This assumes all layers have the same CRS, EPSG:4326.)
    all_features = gpd.GeoDataFrame(
        pd.concat(all_layers.values(), ignore_index=True), crs="EPSG:4326"
    )

    logging.info("Computing connections for each layer...")
    updated_layers = {}
    for layer, gdf in all_layers.items():
        logging.info(f"Processing connections for layer '{layer}'...")
        exclude_list = csv_files[layer].get("exclude_connections", [])
        priority_connections = csv_files[layer].get("priority_connections", None)
        mono_connection = csv_files[layer].get("mono_connection_per_endpoint", False)
        radius = csv_files[layer].get("radius", CONNECTION_RADIUS_METERS)
        updated_gdf = find_connections(
            gdf,
            all_features,
            radius,
            exclude_list,
            priority_connections,
            mono_connection,
        )
        updated_layers[layer] = updated_gdf

    logging.info("Saving updated GeoJSON files...")
    save_updated_layers(updated_layers)
    count_all_features = sum([len(layer) for layer in all_layers.values()])
    elapsed = time.time() - start_time
    logging.info(f"Processed {count_all_features} features in {elapsed:.2f} seconds.")
    logging.info(
        f"Average features processed per second: {count_all_features/elapsed:.2f}"
    )
