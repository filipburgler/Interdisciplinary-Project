from pathlib import Path

_PACKAGE_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _PACKAGE_DIR.parent

DATA_DIR = _PROJECT_DIR / "data" / "raw"
PANELS_DIR = _PROJECT_DIR / "data" / "panels"

WINDOW_MONTHS = 12

DATASETS_METADATA = {
    "intra" : {
        "file_name" : "synthetic_intra.parquet",
        "unique_firms" : set(),
        "month_label" : "BERM",
        "id_label" : "kzr",
        "columns":{"asd": "asdas"
                   }
        },                 
    "extra" : {
        "file_name" : "synthetic_extra.parquet",
        "unique_firms" : set(),
        "month_label" : "BERM",
        "id_label" : "kzr",
        "columns":{"asd": "asdas"
                   }
        },                 
    "prodcom" : {
        "file_name" : "synthetic_prodcom.parquet",
        "unique_firms" : set(),
        "month_label" : "PMO",
        "id_label" : "kzz",
        "columns":{"asd": "asdas"
                   }
        },
    "urs" : {
        "file_name" : "synthetic_urs.parquet",
        "unique_firms" : set(),
        "month_label" : None,
        "id_label" : "kzr",
        "columns":{"asd": "asdas"
                   }
        },
}

