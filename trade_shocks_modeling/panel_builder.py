import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime
import time
import copy
import shutil
import sys

from .config import DATA_DIR, PANELS_DIR, DATASETS_METADATA, WINDOW_MONTHS

if not DATA_DIR.exists():
    raise FileNotFoundError(f"Data directory does not exist: {DATA_DIR}")

if PANELS_DIR.exists():
    shutil.rmtree(PANELS_DIR)

PANELS_DIR.mkdir(parents=True, exist_ok=True)

datasets = copy.deepcopy(DATASETS_METADATA)
trade_shocks_df = pd.read_csv(
    DATA_DIR / "trade_policy_shocks_2000_2024.csv"
)


def get_unique_firms(file_name, id_label):
    dataset = ds.dataset(DATA_DIR / file_name, format="parquet")
    table = dataset.to_table(columns=[id_label])
    firms = table.column(id_label).to_pandas().unique()
    return set(firms)

def get_date_range(file_name, month_label):
    dataset = ds.dataset(DATA_DIR / file_name, format="parquet")

    if month_label is not None:
        columns = ["year", month_label]
        table = dataset.to_table(columns=columns)

        years = table["year"]
        months = table[month_label]

        min_year = pc.min(years).as_py()
        max_year = pc.max(years).as_py()

        min_year_mask = pc.equal(years, min_year)
        max_year_mask = pc.equal(years, max_year)

        min_month = pc.min(months.filter(min_year_mask)).as_py()
        max_month = pc.max(months.filter(max_year_mask)).as_py()

        earliest = pd.Timestamp(year=min_year, month=min_month, day=1)
        latest = pd.Timestamp(year=max_year, month=max_month, day=1)

    else:
        table = dataset.to_table(columns=["year"])
        years = table["year"]

        min_year = pc.min(years).as_py()
        max_year = pc.max(years).as_py()

        earliest = pd.Timestamp(year=min_year, month=1, day=1)
        latest = pd.Timestamp(year=max_year, month=12, day=1)

    return earliest, latest

def build_window(year, month, window=12):
    t = pd.Timestamp(year=int(year), month=int(month), day=1)
    start = t - pd.DateOffset(months=window)
    end   = t + pd.DateOffset(months=window)

    dates = pd.date_range(start=start, end=end, freq="MS")

    return [(d.year, d.month) for d in dates]

def build_filter(earliest, latest, feasible_firms, firm_col, month_col):
    year = ds.field("year")

    if month_col is not None:
        month = ds.field(month_col)

        date_filter = (
            (year > earliest.year) |
            ((year == earliest.year) & (month >= earliest.month))
        ) & (
            (year < latest.year) |
            ((year == latest.year) & (month <= latest.month))
        )

    else:
        # year-only datasets (URS)
        date_filter = (
            (year >= earliest.year) &
            (year <= latest.year)
        )

    # --- FIRM FILTER ---
    firm_filter = ds.field(firm_col).isin(feasible_firms)

    return date_filter & firm_filter

def load_filtered_dataset(path, valid_dates, month_label):

    dataset = ds.dataset(path, format="parquet")

    filters = [
        (ds.field("year") == y) & (ds.field(month_label) == str(m))
        for y, m in valid_dates
    ]

    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter | f

    table = dataset.to_table(filter=combined_filter)

    return table.to_pandas()

def save_panel_with_metadata(df, path, metadata_dict):

    table = pa.Table.from_pandas(df)

    # Convert metadata to bytes
    existing_meta = table.schema.metadata or {}
    new_meta = {
        **existing_meta,
        **{k: str(v).encode() for k, v in metadata_dict.items()}
    }

    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, path)


start = time.time()
earliest_dates = []
latest_dates = []
for name, items in datasets.items():
    loop = time.time()
    month_label = items["month_label"]
    id_label = items["id_label"]
    file_name = items["file_name"]

    datasets[name]["unique_firms"] = get_unique_firms(file_name, id_label)

    earliest_in_df, latest_in_df = get_date_range(file_name, month_label)
    earliest_dates.append(earliest_in_df)
    latest_dates.append(latest_in_df)
    print(f"{name} finished in {time.time() - loop}")

earliest_global = max(earliest_dates)
latest_global = min(latest_dates)
feasible_firms = (
    (datasets["intra"]["unique_firms"]
     | datasets["extra"]["unique_firms"])
    & datasets["prodcom"]["unique_firms"]
    & datasets["urs"]["unique_firms"]
)
feasible_firms_list = list(feasible_firms)

print(earliest_global)
print(latest_global)
print(len(feasible_firms))
print("time: ", time.time() - start)


earliest = pd.Timestamp(year=2014, month=1, day=1)
latest = pd.Timestamp(year=2021, month=1, day=1)


for index, row in trade_shocks_df.iterrows():
    earliest_shock_date = earliest_global + pd.DateOffset(months=12)
    latest_shock_date = latest_global - pd.DateOffset(months=12)
    shock_date = pd.Timestamp(year=int(row["year"]), month=int(row["month"]), day=1)

    if shock_date < earliest_shock_date or shock_date > latest_shock_date:
        continue
    
    earliest_feasible_date = shock_date - pd.DateOffset(months=12)
    latest_feasible_date = shock_date + pd.DateOffset(months=12)

    #############################################################################################
    
    filter_expr = build_filter(
        earliest_feasible_date, 
        latest_feasible_date, 
        feasible_firms, 
        datasets[name]["id_label"], 
        datasets[name]["month_label"]
        )

    dataset = ds.dataset(DATA_DIR / datasets[name]["file_name"], format="parquet")

    cols_map = datasets[name]["columns"]

    selected_cols = []
    new_cols = []

    for old, new in cols_map.items():
        selected_cols.append(old)
        new_cols.append(new)

    table = dataset.to_table(
        columns=selected_cols,
        filter=filter_expr
    )

    table = table.rename_columns(new_cols)

    ##################################################################################################

    metadata = {
    "name": row["name"],
    "created_at": datetime.now(),
    "event": row["event"],
    "affected": row["affected"],
    "description": row["description"],
    "shock_date": shock_date,
    # "n_firms": df["firm"].nunique(),
    "window_months": WINDOW_MONTHS,
    }

    break





# df = load_filtered_dataset(DATA_DIR / "synthetic_intra.parquet", valid_dates, "BERM")

# print(df.info())
# print(len(df))