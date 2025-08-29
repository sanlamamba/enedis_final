from dataclasses import dataclass


@dataclass
class Config:
    project_id: str = "ofr-2kt-valo-reseau-1-lab-prd"
    batch_size: int = 20
    max_depth: int = 10
    exploration_limit: int = 50

    sites_table: str = "ofr-bdf-stor-reseau-1-prd.bdf_angele_prd.raw_dim_reh_site_t"
    enedis_table: str = "ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.enedis_full"
    temp_table: str = "ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.temp_site_analysis"
    final_table: str = "ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.site_grid_analysis"

    poste_source_layer: str = "postes_source"
    bt_layers: tuple = ("reseau_bt", "reseau_souterrain_bt")
