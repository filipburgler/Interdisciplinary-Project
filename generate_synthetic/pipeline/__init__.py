from .io_utils import merge_large_parquet, save_df
from .pipeline_core import (
    build_dataframe,
    compute_row_layout,
    filter_metadata_by_panel_structure,
    generate_synthetic_from_dict,
    generate_synthetic_from_metadata,
)

__all__ = [
    "build_dataframe",
    "compute_row_layout",
    "filter_metadata_by_panel_structure",
    "generate_synthetic_from_dict",
    "generate_synthetic_from_metadata",
    "main",
    "merge_large_parquet",
    "save_df",
]


def main():
    from .main import main as _main

    return _main()
