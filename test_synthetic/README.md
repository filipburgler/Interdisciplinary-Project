# `test_synthetic`

This folder contains the synthetic-dataset validation suite.

It compares each generated Parquet dataset in `data/raw` against its matching metadata JSON in `data/metadata` and writes a text report to `test_synthetic/metadata_validation_report.txt`.

## What It Checks

- expected and unexpected columns
- total row counts
- per-year row counts
- per-column missing counts
- per-column per-year unique counts
- total unique counts
- exact `level_counts` for finite categorical variables
- numeric `mean`, `m_min`, and `m_max` differences, with values inside 15% reported as notes

## How To Run

```powershell
python -m test_synthetic.run_validation_report
```

## Optional unittest entrypoint

```powershell
python -m unittest test_synthetic.test_metadata_report -v
```
