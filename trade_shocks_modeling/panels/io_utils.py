import shutil

import pyarrow.dataset as ds

from ..config import DATA_DIR


def ask_user(prompt, default="y"):
    valid_map = {
        "y": "y",
        "yes": "y",
        "n": "n",
        "no": "n"
    }
    options_str = "Y/n" if default == "y" else "y/N"

    while True:
        choice = input(f"{prompt} [{options_str}]: ").strip().lower()

        if choice == "":
            return default
        if choice in valid_map:
            return valid_map[choice]

        print(f"Please enter y/yes or n/no")


def safely_delete_dir(path):
    if path.exists():
        try:
            print(f"Deleting {path}...")
            shutil.rmtree(path)
        except Exception as e:
            raise RuntimeError(f"Failed to delete {path}: {e}")
        print("Done!")


def build_filter(earliest, latest, feasible_firms, firm_col, year_col, month_col):
    year = ds.field(year_col)

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
        date_filter = (
            (year >= earliest.year) &
            (year <= latest.year)
        )

    firm_filter = ds.field(firm_col).isin(feasible_firms)

    return date_filter & firm_filter


def load_shock_filtered_dataset(
    items,
    earliest_window_date,
    latest_window_date,
    feasible_firms,
    data_dir=DATA_DIR,
):
    filter_expr = build_filter(
        earliest_window_date,
        latest_window_date,
        feasible_firms,
        items["id_label"],
        items["year_label"],
        items["month_label"],
    )

    dataset = ds.dataset(data_dir / items["file_name"], format="parquet")

    cols_map = items["columns"]
    selected_cols = list(cols_map.keys())
    new_cols = list(cols_map.values())

    table = dataset.to_table(
        columns=selected_cols,
        filter=filter_expr,
    )

    table = table.rename_columns(new_cols)
    return table.to_pandas()


def get_unique_firms(file_name, id_label, data_dir=DATA_DIR):
    dataset = ds.dataset(data_dir / file_name, format="parquet")
    table = dataset.to_table(columns=[id_label])
    firms = table.column(id_label).to_pandas().unique()
    return set(firms)


def save_panel(df, shock_dir, name):
    file_path = shock_dir / f"{name}_panel.parquet"
    df.to_parquet(file_path, index=False)
    

def print_shock_data(event_firms, earliest, latest, window_size, shock_name, length=51):
    repeat = max(0, (length - len(shock_name)) // 2)
    print("#" * repeat, shock_name, "#" * repeat)
    print()
    print(f"Unique firms in shock window: {len(event_firms):,}")
    print("Earliest window date: ", earliest)
    print("Latest window date: ", latest)
    print("Window size: ", window_size)


def print_panel_stats(dataset_name, df):
    print("-" * 51)
    print(f"Dataset: {dataset_name}")
    print("- " * 26)
    print(f"Rows: {len(df):,}")
    print(f"Unique firms: {df['firm'].nunique():,}")
