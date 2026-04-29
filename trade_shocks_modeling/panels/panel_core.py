import json
from datetime import datetime

import pandas as pd
import sys

from ..config import (
    DATA_DIR,
    SHOCKS_FILENAME,
    DATASETS_METADATA,
    PANELS_DIR,
    WINDOW_MONTHS,
)
from .date_utils import get_event_window_bounds
from .filters import (
    combine_masks,
    filter_by_affected,
    filter_by_date,
    filter_by_event_name,
    filter_by_mentions,
)
from .firm_selection import prepare_event_firms, prepare_global_constraints
from .io_utils import (
    load_shock_filtered_dataset, 
    save_panel, 
    print_panel_stats, 
    print_shock_data,
)


def make_unique_shock_name(raw_name, shock_name_counts):
    shock_name = str(raw_name).strip().replace(" ", "_")

    if shock_name in shock_name_counts:
        shock_name_counts[shock_name] += 1
        shock_name = f"{shock_name}_{shock_name_counts[shock_name]}"
    else:
        shock_name_counts[shock_name] = 1

    return shock_name


def build_coverage_summary(coverage_dict):
    summary = {}

    for dataset_name, info in coverage_dict.items():
        if info["pre"] is not None:
            summary[dataset_name] = {
                "pre_mean": float(info["pre"].mean()),
                "post_mean": float(info["post"].mean()),
                "pre_median": float(info["pre"].median()),
                "post_median": float(info["post"].median()),
            }
        else:
            summary[dataset_name] = {
                "annual_relevant_years": sorted(info["annual_relevant_years"]),
                "annual_presence_mean": float(info["annual_presence"].mean()),
                "annual_presence_median": float(info["annual_presence"].median()),
            }

    return summary


def build_panels(
    datasets=DATASETS_METADATA,
    event_names=None,
    affected=None,
    mentioned=None,
    threshold=0.5,
):
    trade_shocks_df = pd.read_csv(DATA_DIR / SHOCKS_FILENAME)

    earliest_global, latest_global, feasible_firms = prepare_global_constraints(datasets)

    earliest_shock_date = earliest_global + pd.DateOffset(months=WINDOW_MONTHS)
    latest_shock_date = latest_global - pd.DateOffset(months=WINDOW_MONTHS)

    mask = combine_masks(
        filter_by_date(trade_shocks_df, earliest_shock_date, latest_shock_date),
        filter_by_event_name(trade_shocks_df, *event_names) if event_names else None,
        filter_by_affected(trade_shocks_df, *affected) if affected else None,
        filter_by_mentions(trade_shocks_df, *mentioned) if mentioned else None,
        mode="and",
    )

    filtered_shocks = trade_shocks_df[mask] if mask is not None else trade_shocks_df

    print(f"Number of shocks after filtering: {len(filtered_shocks)}")
    
    shock_name_counts = {}

    for _, row in filtered_shocks.iterrows():
        shock_date = pd.Timestamp(
            year=int(row["year"]),
            month=int(row["month"]),
            day=1,
        )
        earliest_window_date, latest_window_date = get_event_window_bounds(shock_date)

        shock_name = make_unique_shock_name(row["name"], shock_name_counts)

        shock_dir = PANELS_DIR / shock_name
        if shock_dir.exists():
            import shutil
            shutil.rmtree(shock_dir)
        shock_dir.mkdir(parents=True, exist_ok=True)

        event_firms, coverage_dict = prepare_event_firms(
            earliest_window_date,
            latest_window_date,
            shock_date,
            feasible_firms,
            threshold,
        )

        
        print_shock_data(
            event_firms, 
            earliest_window_date, 
            latest_window_date, 
            WINDOW_MONTHS, 
            shock_name
        )


        for name, items in datasets.items():
            df = load_shock_filtered_dataset(
                items,
                earliest_window_date,
                latest_window_date,
                event_firms,
            )
            print_panel_stats(name, df)
            save_panel(df, shock_dir, name)
        print("-" * 51)

        not_event_eligible_firms = feasible_firms - event_firms
        coverage_summary = build_coverage_summary(coverage_dict)

        print(f"Not event eligible firms: {len(not_event_eligible_firms):,}")
        print(f"% not eligible: {len(not_event_eligible_firms) / len(feasible_firms):.2%}")

        metadata = {
            "name": row["name"],
            "folder_name": shock_name,
            "created_at": datetime.now().isoformat(),
            "event": row["event"],
            "affected": row["affected"],
            "description": row["description"],
            "shock_date": shock_date.isoformat(),
            "n_event_firms": len(event_firms),
            "coverage_threshold": threshold,
            "n_not_event_eligible": len(not_event_eligible_firms),
            "window_months": WINDOW_MONTHS,
            "coverage_summary": coverage_summary,
        }

        metadata_path = shock_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)

        print("*" * 51)
        print()

    print(f"Created panels for {len(shock_name_counts)} events")