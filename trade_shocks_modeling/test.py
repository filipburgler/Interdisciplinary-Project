import pandas as pd
import sys

intra_data = [
    {"kzr": 1, "year": 2020, "BERM": 1, "value": 100, "VL": "UK"},
    {"kzr": 1, "year": 2020, "BERM": 2, "value": 150, "VL": "UK"},
    {"kzr": 2, "year": 2020, "BERM": 1, "value": 200, "VL": "DE"},
    {"kzr": 3, "year": 2020, "BERM": 1, "value": 250, "VL": "HR"},
]

extra_data = [
    {"kzr": 1, "year": 2020, "BERM": 1, "value": 50, "VL": "US"},
    {"kzr": 2, "year": 2020, "BERM": 1, "value": 80, "VL": "AT"},
    {"kzr": 4, "year": 2020, "BERM": 1, "value": 10, "VL": "CA"},
]

prodcom_data = [
    {"firm_id": 1, "yr": 2020, "mo": 1, "prod": 500},
    {"firm_id": 1, "yr": 2020, "mo": 2, "prod": 700},
    {"firm_id": 2, "yr": 2020, "mo": 1, "prod": 600},
    {"firm_id": 3, "yr": 2020, "mo": 1, "prod": 400},
    {"firm_id": 4, "yr": 2020, "mo": 1, "prod": 300},
]

urs_data = [
    {"kzz": 1, "year": 2020, "sector": "A"},
    {"kzz": 2, "year": 2020, "sector": "B"},
    {"kzz": 3, "year": 2020, "sector": "C"},
    {"kzz": 4, "year": 2020, "sector": "D"},
]

idf = pd.DataFrame(intra_data)
edf = pd.DataFrame(extra_data)
pdf = pd.DataFrame(prodcom_data)
udf = pd.DataFrame(urs_data)

prodcom = pdf.rename(columns={
    "firm_id": "firm",
    "yr": "year",
    "mo": "month",
    "prod": "production"
})

intra = idf.rename(columns={
    "kzr": "firm",
    "value": "intra_value",
    "VL": "intra_VL",
    "BERM": "month"
})
extra = edf.rename(columns={
    "kzr": "firm",
    "value": "extra_value",
    "VL": "extra_VL",
    "BERM": "month"
})
urs = udf.rename(columns={
    "kzz": "firm"
})

# -----------------------------
# STEP 2 — Merge monthly tables
# -----------------------------

panel = intra.merge(
    extra,
    on=["firm", "year", "month"],
    how="outer"
)


panel = panel.merge(
    prodcom,
    on=["firm", "year", "month"],
    how="outer"
)
print(panel)

# -----------------------------
# STEP 3 — Expand URS yearly → monthly
# -----------------------------

months = pd.DataFrame({"month": range(1, 13)})

urs_monthly = (
    urs.merge(months, how="cross")
)

# -----------------------------
# STEP 4 — Merge URS
# -----------------------------

panel = panel.merge(
    urs_monthly,
    on=["firm", "year", "month"],
    how="left"
)

print(panel)