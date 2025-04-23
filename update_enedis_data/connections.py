# connections.py
"""
Module de calcul des connexions spatiales.

Ce module parallélise le calcul des connexions entre entités à l'aide d'un pool de processus.
"""

from concurrent.futures import ProcessPoolExecutor
import geopandas as gpd
from shapely.geometry import Point
from config import LAYERS_CONFIG
import pandas as pd

_global_all_features_proj = None
_global_spatial_index = None
_global_base_radius = None
_global_exclude_list = None
_global_priority_connections = None
_global_mono_connection = None


def init_worker(
    all_features_proj, base_radius, exclude_list, priority_connections, mono_connection
):
    """
    Initialise les variables globales dans chaque processus de travail.

    Parameters:
        all_features_proj (GeoDataFrame): Ensemble des entités projetées (CRS en EPSG:3857) pour la recherche spatiale.
        base_radius (float): Rayon de base utilisé pour le tampon autour d'un point lors de la recherche.
        exclude_list (list): Liste des noms de couches à exclure lors de la recherche de connexions.
        priority_connections (dict): Dictionnaire des priorités pour certaines couches.
            Exemple : {'nom_couche': {'priority': 1, 'radius': 7}}
        mono_connection (bool): Indique si une seule connexion par extrémité doit être sélectionnée.
    """
    global _global_all_features_proj, _global_spatial_index, _global_base_radius, _global_exclude_list, _global_priority_connections, _global_mono_connection
    _global_all_features_proj = all_features_proj
    _global_spatial_index = _global_all_features_proj.sindex
    _global_base_radius = base_radius
    _global_exclude_list = exclude_list
    _global_priority_connections = priority_connections
    _global_mono_connection = mono_connection


def select_connection_candidates(
    endpoint,
    all_features_proj,
    spatial_index,
    base_radius,
    exclude_list,
    priority_connections,
    mono_connection,
    solo_dict,
):
    """
    Sélectionne les identifiants de connexions candidats pour un point donné.

    Parameters:
        endpoint (shapely.geometry): Point ou objet géométrique servant de référence.
        all_features_proj (GeoDataFrame): Ensemble complet des entités, projeté en EPSG:3857.
        spatial_index: Index spatial construit sur all_features_proj pour accélérer les requêtes spatiales.
        base_radius (float): Rayon pour la création du tampon autour de l'endpoint.
        exclude_list (list): Liste des noms de couches à exclure de la recherche.
        priority_connections (dict): Dictionnaire de priorités et rayons pour certaines couches.
        mono_connection (bool): Si True, sélectionne une seule connexion par extrémité selon la priorité.
        solo_dict (dict or None): Optionnel. Dictionnaire indiquant des règles spéciales pour restreindre les connexions (ex. solo_connection_if).

    Returns:
        set: Un ensemble d'identifiants (strings) pour les candidats retenus comme connexions.
    """
    buf = endpoint.buffer(base_radius)
    possible_idx = list(spatial_index.intersection(buf.bounds))
    possible = all_features_proj.iloc[possible_idx]
    matches = possible[possible.intersects(buf)]

    if exclude_list:
        matches = matches[~matches["source_layer"].isin(exclude_list)]

    if solo_dict is not None:
        candidates = matches[matches["source_layer"].isin(solo_dict.keys())].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)
            valid = candidates[
                candidates.apply(
                    lambda row: row["distance"]
                    <= solo_dict[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]
            if not valid.empty:
                valid = valid.sort_values(by="distance")
                return {valid.iloc[0]["id"]}

    if mono_connection and priority_connections:
        candidates = matches[
            matches["source_layer"].isin(priority_connections.keys())
        ].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)
            valid = candidates[
                candidates.apply(
                    lambda row: row["distance"]
                    <= priority_connections[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]
            if not valid.empty:
                valid["priority"] = valid["source_layer"].map(
                    lambda s: priority_connections[s]["priority"]
                )
                valid = valid.sort_values(by=["priority", "distance"])
                return {valid.iloc[0]["id"]}
        return set(matches["id"].tolist())

    return set(matches["id"].tolist())


def process_feature_worker(item):
    """
    Traite une entité pour calculer ses connexions spatiales.

    Parameters:
        item (tuple): Tuple (idx, feature) où 'idx' est l'indice et 'feature' une ligne du GeoDataFrame.

    Returns:
        tuple: Trois listes correspondant respectivement aux identifiants:
            - Union des connexions (start + end)
            - Connexions au point de départ
            - Connexions au point d'arrivée
    """
    idx, feature = item
    all_features_proj = _global_all_features_proj
    spatial_index = _global_spatial_index
    base_radius = _global_base_radius
    exclude_list = _global_exclude_list
    priority_connections = _global_priority_connections
    mono_connection = _global_mono_connection

    geom = feature.geometry
    feature_id = feature["id"]

    if geom.geom_type == "LineString":
        start_point = Point(geom.coords[0])
        end_point = Point(geom.coords[-1])
        config = LAYERS_CONFIG.get(feature["source_layer"], {})
        solo_dict = config.get("solo_connection_if", None)
        start_ids = select_connection_candidates(
            start_point,
            all_features_proj,
            spatial_index,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict,
        )
        end_ids = select_connection_candidates(
            end_point,
            all_features_proj,
            spatial_index,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict,
        )
        start_ids.discard(feature_id)
        end_ids.discard(feature_id)
        union_ids = start_ids.union(end_ids)
        return list(union_ids), list(start_ids), list(end_ids)
    else:
        endpoint = geom if geom.geom_type == "Point" else geom.centroid
        candidate_ids = select_connection_candidates(
            endpoint,
            all_features_proj,
            spatial_index,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict=None,
        )
        candidate_ids.discard(feature_id)
        return list(candidate_ids), [], []


def find_connections(
    gdf,
    all_features,
    base_radius,
    exclude_list=None,
    priority_connections=None,
    mono_connection_per_endpoint=False,
):
    """
    Calcule les connexions spatiales pour chaque entité d'un GeoDataFrame.

    Parameters:
        gdf (GeoDataFrame): Ensemble d'entités à traiter.
        all_features (GeoDataFrame): Ensemble complet des entités (pour la recherche de connexions).
        base_radius (float): Rayon de base pour la recherche de connexions.
        exclude_list (list, optional): Liste des couches à exclure.
        priority_connections (dict, optional): Dictionnaire de priorités et rayons pour certaines couches.
        mono_connection_per_endpoint (bool, optional): Si True, limite la connexion à une par extrémité.

    Returns:
        GeoDataFrame: Le GeoDataFrame original avec trois colonnes ajoutées :
            - "connections": liste des identifiants de connexions (union des connexions de début et fin)
            - "start_connections": liste des connexions au point de départ (pour les LineStrings)
            - "end_connections": liste des connexions au point d'arrivée (pour les LineStrings)
    """
    if exclude_list is None:
        exclude_list = []
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    with ProcessPoolExecutor(
        initializer=init_worker,
        initargs=(
            all_features_proj,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection_per_endpoint,
        ),
    ) as executor:
        results = list(executor.map(process_feature_worker, list(gdf_proj.iterrows())))
    connections, start_connections, end_connections = zip(*results)
    gdf_proj["connections"] = connections
    gdf_proj["start_connections"] = start_connections
    gdf_proj["end_connections"] = end_connections
    return gdf_proj.to_crs(epsg=4326)


def compute_connections(layers):
    """
    Reprojette les GeoDataFrames en EPSG:4326, les concatène pour créer un GeoDataFrame global,
    et calcule les connexions spatiales pour chaque couche.

    Parameters:
        layers (dict): Dictionnaire des GeoDataFrames pour chaque couche.

    Returns:
        dict: Dictionnaire mis à jour associant chaque couche à un GeoDataFrame enrichi avec
        les colonnes "connections", "start_connections" et "end_connections".
    """
    reprojected_layers = {}
    for layer_key, gdf in layers.items():
        if "source_layer" not in gdf.columns:
            gdf["source_layer"] = layer_key
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")
        reprojected_layers[layer_key] = gdf

    all_features = gpd.GeoDataFrame(
        pd.concat(list(reprojected_layers.values()), ignore_index=True), crs="EPSG:4326"
    )
    updated_layers = {}
    for layer_key, gdf in layers.items():
        cfg = LAYERS_CONFIG.get(layer_key, {})
        exclude = cfg.get("exclude_connections", [])
        priority = cfg.get("priority_connections", None)
        mono = cfg.get("mono_connection_per_endpoint", False)
        radius = cfg.get("radius", 3)
        updated_gdf = find_connections(
            gdf, all_features, radius, exclude, priority, mono
        )
        updated_layers[layer_key] = updated_gdf
    return updated_layers
