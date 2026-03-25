# `generate_synthetic`

This package generates synthetic yearly tabular datasets from metadata JSON files and writes them as Parquet. Validation now lives under [`test_synthetic/`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/test_synthetic), where generated outputs are compared against the metadata contract.

The code is now organized by responsibility:

- `pipeline/`: orchestration and file I/O
- `numeric/`: numeric value generation and numeric repair/fallback logic
- `finite/`: finite categorical allocation math and generation
- `pools/`: reusable value pools for non-finite categorical variables
- `rows/`: row construction
- `types/`: variable type detection
- `config.py`: constants and defaults

If you want to understand how generation works at a high level, start with [`pipeline/pipeline_core.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/pipeline_core.py).

## Package Layout

### Top level

- [`__init__.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/__init__.py)
  Re-exports the main public generation API from the `pipeline` package so callers can import from `generate_synthetic` directly.

- [`config.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/config.py)
  Central configuration and constants.
  It defines:
  - output directories
  - metadata directories
  - default generation batches
  - large-file chunking threshold
  - country codes
  - synthetic identifier schemas

- [`functions.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/functions.py)
  Convenience export module for helper functions used elsewhere in the project. It exposes selected pool, type-detection, numeric, and row-building helpers.

### `pipeline/`

- [`pipeline/__init__.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/__init__.py)
  Re-exports the main orchestration and I/O functions from the `pipeline` package.

- [`pipeline/pipeline_core.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/pipeline_core.py)
  Main orchestration module.
  It is responsible for:
  - deterministic RNG creation via `make_rng()`
  - checking shared yearly layout via `compute_row_layout()`
  - dispatching each variable to the correct generator via `generate_column()`
  - assembling a full in-memory table via `build_dataframe()`
  - generating and saving a dataset from loaded metadata via `generate_synthetic_from_dict()`
  - loading metadata and generating from a metadata file via `generate_synthetic_from_metadata()`
  - filtering metadata to the majority panel structure via `filter_metadata_by_panel_structure()`

- [`pipeline/io_utils.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/io_utils.py)
  File and disk operations only.
  It is responsible for:
  - writing Parquet files via `save_df()`
  - merging chunked yearly Parquet files via `merge_large_parquet()`
  - reading metadata JSON via `load_metadata_file()`

- [`pipeline/main.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/main.py)
  Batch entrypoint.
  It reads `DEFAULT_GENERATION_BATCHES` from [`config.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/config.py) and runs them one by one.

### `numeric/`

- [`numeric/numeric_core.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/numeric/numeric_core.py)
  Pure numeric/stateless logic.
  It contains:
  - quantile interpolation
  - optional log-scale interpolation for extreme skew
  - zero-mass estimation
  - support-weight construction
  - conservative mean correction
  - numeric bounds and epsilon helpers

- [`numeric/numeric_support.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/numeric/numeric_support.py)
  Numeric validity, repair, and fallback logic.
  It contains:
  - clipping
  - zero anchoring
  - continuous uniqueness repair
  - integer uniqueness repair
  - best-effort fallback support generation

- [`numeric/numeric_generators.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/numeric/numeric_generators.py)
  High-level numeric generation.
  It coordinates the numeric pipeline:
  - interpolate support from metadata
  - apply conservative mean correction
  - clip
  - repair/enforce uniqueness
  - return the final numeric support and weights

### `finite/`

- [`finite/finite_math.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/finite/finite_math.py)
  Math-heavy finite categorical allocation functions.
  It contains:
  - `build_b_matrix_capacity_aware()`
  - `solve_a_with_constraints()`

- [`finite/finite_generators.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/finite/finite_generators.py)
  Finite categorical generation.
  It contains:
  - `generate_finite_categorical_column_math()`
  - `build_year_for_finite()`
  - `build_column_for_finite()`

### `pools/`

- [`pools/categorical.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pools/categorical.py)
  Reusable categorical pool generation for:
  - synthetic uppercase code identifiers
  - country-code pools
  - logical/boolean pools
  - integer-coded categorical pools

### `rows/`

- [`rows/row_builders.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/rows/row_builders.py)
  Centralized row construction.
  It contains:
  - generic pool-to-row builders for non-finite variables
  - numeric row construction using numeric support weights
  - per-year unique-value assignment

### `types/`

- [`types/type_detection.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/types/type_detection.py)
  Metadata-based type detection.
  It contains:
  - `detect_var_type()`
  - `detect_category_type()`

### Metadata and tests

- [`data/metadata/`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/metadata)
  Default metadata JSON files used by the batch runner.

- [`test_synthetic/`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/test_synthetic)
  Validation suite for generated datasets. See [`test_synthetic/README.md`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/test_synthetic/README.md).

## How Generation Works

At a high level, generation follows this flow:

1. Load metadata from JSON or receive it as a Python dictionary.
2. Filter metadata to the majority yearly panel structure.
3. Compute the shared yearly layout.
4. For each variable:
   - detect its type
   - generate a pool/support or finite allocation
   - build rows year by year
5. Assemble a PyArrow table.
6. Write a single Parquet file, or if the dataset is large, write one yearly Parquet file at a time and merge them afterward.

## How Numeric Generation Works

Numeric generation is quantile-driven, not parametric.

The numeric path uses metadata summaries such as:

- `m_min`
- `m_p25`
- `m_p50`
- `m_p75`
- `m_p90`
- `m_max`
- `mean`

The pipeline is:

1. Build quantile knots from metadata.
2. Interpolate synthetic support values from those quantiles.
3. Optionally interpolate in log-space for extremely skewed variables.
4. Estimate zero mass from zero-valued quantiles.
5. Apply conservative mean correction.
6. Clip to metadata bounds.
7. Repair uniqueness if needed.
8. Fall back gracefully if metadata is infeasible.

This is implemented across:

- [`numeric/numeric_core.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/numeric/numeric_core.py)
- [`numeric/numeric_support.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/numeric/numeric_support.py)
- [`numeric/numeric_generators.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/numeric/numeric_generators.py)

## How To Run The Code

There are three practical ways to run the generator.

### 1. Run the default batch job

From the project root:

```powershell
python -m generate_synthetic.pipeline.main
```

What this does:

1. Imports [`generate_synthetic/pipeline/main.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/main.py)
2. Reads `DEFAULT_GENERATION_BATCHES` from [`config.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/config.py)
3. For each configured batch:
   - loads the metadata file
   - filters variables to the majority panel structure
   - generates the synthetic dataset
   - writes the output Parquet file

By default, outputs are written under:

- [`data/raw`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/raw)

Expected default outputs:

- [`data/raw/synthetic_extra.parquet`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/raw/synthetic_extra.parquet)
- [`data/raw/synthetic_intra.parquet`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/raw/synthetic_intra.parquet)
- [`data/raw/synthetic_prodcom.parquet`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/raw/synthetic_prodcom.parquet)
- [`data/raw/synthetic_urs.parquet`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/raw/synthetic_urs.parquet)

### 2. Generate one dataset from a metadata file in Python

```python
from pathlib import Path
from generate_synthetic import generate_synthetic_from_metadata

generate_synthetic_from_metadata(
    metadata_path=Path("data/metadata/metadata_batch_prodcom.json"),
    save_path=Path("data/raw"),
    name="synthetic_prodcom",
    large_file=None,
    seed=64,
)
```

Use this when:

- you want one dataset only
- you want to choose the output directory
- you want to override the seed
- you want to force or disable chunked generation via `large_file`

### 3. Generate directly from already-loaded metadata

If you already have metadata in memory:

```python
import json
from pathlib import Path
from generate_synthetic import build_dataframe, generate_synthetic_from_dict

with Path("data/metadata/metadata_batch_prodcom.json").open("r", encoding="utf-8") as f:
    metadata = json.load(f)

table = build_dataframe(metadata, seed=64)

generate_synthetic_from_dict(
    metadata=metadata,
    save_path=Path("data/raw"),
    name="synthetic_prodcom",
    large_file=None,
    seed=64,
)
```

Use:

- `build_dataframe(...)` if you want a PyArrow table in memory
- `generate_synthetic_from_dict(...)` if you want the Parquet file written directly

## Large-File Behavior

If total rows exceed `LARGE_FILE_ROW_CUTOFF` from [`config.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/config.py), generation switches to chunked mode.

Chunked mode does this:

1. create a temporary `parts_of_large_file` directory
2. generate one year at a time
3. write one Parquet file per year
4. merge them into the final Parquet file
5. delete the temporary parts directory

This logic lives in:

- [`pipeline/pipeline_core.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/pipeline_core.py)
- [`pipeline/io_utils.py`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/generate_synthetic/pipeline/io_utils.py)

## Public Python API

These are the main imports intended for normal use:

```python
from generate_synthetic import (
    build_dataframe,
    compute_row_layout,
    filter_metadata_by_panel_structure,
    generate_synthetic_from_dict,
    generate_synthetic_from_metadata,
)
```

You can also import directly from the subpackages if you need internal building blocks, for example:

```python
from generate_synthetic.pipeline.pipeline_core import build_dataframe
from generate_synthetic.numeric.numeric_generators import generate_numeric_pool
from generate_synthetic.rows.row_builders import build_numeric_column
```

## What The Generator Preserves

- Shared yearly row layout for variables that survive majority-panel filtering
- Per-year row counts
- Per-year missing-value counts
- Per-year unique-value counts
- Deterministic generation for the same seed and variable name
- Exact global `level_counts` for finite categorical variables
- Robust numeric generation with clipping and fallback behavior

## What It Does Not Guarantee

- Exact preservation of every summary statistic for numeric variables
- Exact global frequency preservation for non-finite categorical variables
- Inclusion of metadata variables that do not match the majority panel structure
- A command-line interface with argument parsing or flags

## Validation

The generator and validator are separate. Generation writes data. Validation checks generated data against metadata.

To run the metadata validation suite:

```powershell
python -m test_synthetic.run_validation_report
```

That validation code reads generated Parquet files and compares them to metadata expectations. Full validation documentation is in:

- [`test_synthetic/README.md`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/test_synthetic/README.md)

## Practical Run Checklist

If you just want to run the generator:

1. Open a terminal in the project root.
2. Run:

```powershell
python -m generate_synthetic.pipeline.main
```

3. Wait for the batch runner to finish.
4. Check the generated files in:

- [`data/raw`](/C:/Users/filip/Documents/Faks/TU/TU_sem3/Interdisciplinary/Interdisciplinary-Project/data/raw)

5. If you want to verify them, run:

```powershell
python -m test_synthetic.run_validation_report
```

That is the current, actual way to run the code after the refactor.
