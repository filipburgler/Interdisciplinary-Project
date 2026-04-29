import pandas as pd

from ..config import DATA_DIR, DATASETS_METADATA_SMALL
from .date_utils import get_date_range
from .io_utils import get_unique_firms, load_shock_filtered_dataset


def prepare_global_constraints(datasets):
    earliest_dates = []
    latest_dates = []
    unique_firms_dict = {}

    for name, items in datasets.items():
        year_label = items["year_label"]
        month_label = items["month_label"]
        id_label = items["id_label"]
        file_name = items["file_name"]

        unique_firms = get_unique_firms(file_name, id_label)
        unique_firms_dict[name] = unique_firms

        earliest, latest = get_date_range(file_name, year_label, month_label)
        earliest_dates.append(earliest)
        latest_dates.append(latest)

    earliest_global = max(earliest_dates)
    latest_global = min(latest_dates)

    feasible_firms = (
        (unique_firms_dict["intra"]
         | unique_firms_dict["extra"])
        & unique_firms_dict["prodcom"]
        & unique_firms_dict["urs"]
    )

    return earliest_global, latest_global, feasible_firms


def prepare_event_firms(
    earliest,
    latest,
    shock_date,
    feasible_firms,
    threshold=0.5,
    datasets=DATASETS_METADATA_SMALL,
    data_dir=DATA_DIR
):
    firms_dict = {}
    coverage_dict = {}

    for name, items in datasets.items():
        df = load_shock_filtered_dataset(
            items,
            earliest,
            latest,
            feasible_firms,
            data_dir=data_dir
        )
        df = df.copy()

        all_firms = df["firm"].unique()
        
        has_month = items["month_label"] is not None

        if has_month:
            df["date"] = pd.to_datetime(
                dict(year=df["year"], month=df["month"], day=1)
            )

            pre_df  = df[df["date"] < shock_date]
            post_df = df[df["date"] > shock_date]

            expected_pre = len(pd.date_range(
                start=earliest,
                end=shock_date - pd.DateOffset(months=1),
                freq="MS"
            ))
            expected_post = len(pd.date_range(
                start=shock_date + pd.DateOffset(months=1),
                end=latest,
                freq="MS"
            ))

            if expected_post == 0 or expected_pre == 0:
                raise ValueError("Event must have data before and after the shock")
            
            pre_counts  = pre_df.groupby("firm")["date"].nunique()
            post_counts = post_df.groupby("firm")["date"].nunique()
            pre_counts  = pre_counts.reindex(all_firms, fill_value=0)
            post_counts = post_counts.reindex(all_firms, fill_value=0)

            pre_cov  = pre_counts / expected_pre
            post_cov = post_counts / expected_post

            valid_pre  = set(pre_cov[pre_cov >= threshold].index)
            valid_post = set(post_cov[post_cov >= threshold].index)

            valid_firms = valid_pre & valid_post

            firms_dict[name] = valid_firms
            coverage_dict[name] = {
                "pre": pre_cov,
                "post": post_cov,
                "annual_relevant_years": None,
                "annual_presence": None
            }

        else:
            relevant_years = {
                shock_date.year - 1,
                shock_date.year,
                shock_date.year + 1
            }
            valid_firms = set(
                df.loc[df["year"].isin(relevant_years), "firm"].unique()
            )

            firms_dict[name] = valid_firms
            coverage_dict[name] = {
                "pre": None,
                "post": None,
                "annual_relevant_years": relevant_years,
                "annual_presence": df.groupby("firm")["year"].nunique()
            }


    event_firms = (
        (firms_dict["intra"] | firms_dict["extra"])
        & firms_dict["prodcom"]
        & firms_dict["urs"]
    )

    return event_firms, coverage_dict


def find_incomplete_firms(df, earliest_date, latest_date, firm_col="firm", year_col="year", month_col="month"):
    has_month = month_col is not None and month_col in df.columns

    cols = [firm_col, year_col] + ([month_col] if has_month else [])
    df_small = df[cols].drop_duplicates()

    if has_month:
        df_small["date"] = pd.to_datetime(
            dict(
                year=df_small[year_col],
                month=df_small[month_col],
                day=1
            )
        )

        counts = df_small.groupby(firm_col)["date"].nunique()

        expected = len(pd.date_range(
            start=earliest_date,
            end=latest_date,
            freq="MS"
        ))

    else:
        counts = df_small.groupby(firm_col)[year_col].nunique()
        expected = latest_date.year - earliest_date.year + 1

    incomplete_firms = set(counts[counts < expected].index)

    return incomplete_firms