from pathlib import Path
import pandas as pd

_PACKAGE_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _PACKAGE_DIR.parent

DATA_DIR = _PROJECT_DIR / "data" / "raw"
PANELS_DIR = _PROJECT_DIR / "data" / "panels"
SHOCKS_FILENAME = "trade_policy_shocks_2000_2024.csv"

WINDOW_MONTHS = 12

DATASETS_METADATA = {
    "intra" : {
        "file_name" : "synthetic_intra.parquet",
        "unique_firms" : set(),
        "year_label" : "BERJ",
        "month_label" : "BERM",
        "id_label" : "kzr",
        "columns":{"kzr": "firm",
                   "BERJ": "year",
                   "BERM": "month",
                   "EV": "flow_direction",
                   "VL": "dispatch_country",
                   "UL": "country_of_origin",
                   "BL": "destination_country",
                #    "GW": "net_weight",
                #    "SM": "special_unit_of_measurement",
                #    "GART": "business_type",
                   "SW": "value"
                   }
        },                 
    "extra" : {
        "file_name" : "synthetic_extra.parquet",
        "unique_firms" : set(),
        "year_label" : "BERJ",
        "month_label" : "BERM",
        "id_label" : "kzr",
        "columns":{"kzr": "firm",
                   "BERJ": "year",
                   "BERM": "month",
                   "EV": "flow_direction",
                   "VL": "dispatch_country",
                   "UL": "country_of_origin",
                   "BL": "destination_country",
                #    "GW": "net_weight",
                #    "SM": "special_unit_of_measurement",
                #    "GART": "business_type",
                #    "PRAEF": "customs_code",
                #    "VERKINL": "domestic_transport",
                #    "VERKGRE": "external_transport",  
                   "SW": "value"
                   }
        },                 
    "prodcom" : {
        "file_name" : "synthetic_prodcom.parquet",
        "unique_firms" : set(),
        "year_label" : "PJA",
        "month_label" : "PMO",
        "id_label" : "kzz",
        "columns":{"kzz": "firm",
                   "PJA": "year",
                   "PMO": "month",
                   "PCODE": "production_code",
                   "PART": "production_type",
                   "PEH1": "unit_1",
                   "PME1": "quantity_1",
                   "PEH2": "unit_2",
                   "PME2": "quantity_2",
                   "PWERT": "value"
                   }
        },
    "urs" : {
        "file_name" : "synthetic_urs.parquet",
        "unique_firms" : set(),
        "year_label" : "JAHR",
        "month_label" : None,
        "id_label" : "kzr",
        "columns":{"kzr": "firm",
                   "JAHR": "year",
                   "RF": "legal_form",
                   "FSW_NUTS3": "economic_location",
                   "FSW_LKZ": "country_code",
                   "OENACE": "OENACE",
                   "IS": "IS",
                   "USB": "employees_avg",
                   "UST": "tax_base",
                   "RE_DB_DAT_NG": "date_of_establishment",
                   "RE_DB_DAT_SL": "date_of_closure",
                   }
        },
}

DATASETS_METADATA_SMALL = {
    "intra" : {
        "file_name" : "synthetic_intra.parquet",
        "year_label" : "BERJ",
        "month_label" : "BERM",
        "id_label" : "kzr",
        "columns":{"kzr": "firm",
                   "BERJ": "year",
                   "BERM": "month",
                   }
        },                 
    "extra" : {
        "file_name" : "synthetic_extra.parquet",
        "year_label" : "BERJ",
        "month_label" : "BERM",
        "id_label" : "kzr",
        "columns":{"kzr": "firm",
                   "BERJ": "year",
                   "BERM": "month",
                   }
        },                 
    "prodcom" : {
        "file_name" : "synthetic_prodcom.parquet",
        "year_label" : "PJA",
        "month_label" : "PMO",
        "id_label" : "kzz",
        "columns":{"kzz": "firm",
                   "PJA": "year",
                   "PMO": "month",
                   }
        },
    "urs" : {
        "file_name" : "synthetic_urs.parquet",
        "year_label" : "JAHR",
        "month_label" : None,
        "id_label" : "kzr",
        "columns":{"kzr": "firm",
                   "JAHR": "year",
                   }
        },
}



