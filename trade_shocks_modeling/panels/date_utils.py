import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

from ..config import DATA_DIR, WINDOW_MONTHS


def get_event_window_bounds(date: pd.Timestamp, months=WINDOW_MONTHS):
    earliest = date - pd.DateOffset(months=months)
    latest = date + pd.DateOffset(months=months)
    return earliest, latest


def get_date_range(file_name, year_label, month_label, data_dir=DATA_DIR):
    dataset = ds.dataset(data_dir / file_name, format="parquet")

    if month_label is not None:
        columns = [year_label, month_label]
        table = dataset.to_table(columns=columns)

        years = table[year_label]
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
        table = dataset.to_table(columns=[year_label])
        years = table[year_label]

        min_year = pc.min(years).as_py()
        max_year = pc.max(years).as_py()

        earliest = pd.Timestamp(year=min_year, month=1, day=1)
        latest = pd.Timestamp(year=max_year, month=12, day=1)

    return earliest, latest
