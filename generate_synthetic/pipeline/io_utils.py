import gc
import json
import time
from pathlib import Path
from shutil import rmtree

import pyarrow.parquet as pq

from ..config import LARGE_FILE_TMP_FOLDER


def merge_large_parquet(save_path, final_name, delete_parts=True):
    save_path = Path(save_path)
    parts_dir = save_path / LARGE_FILE_TMP_FOLDER
    output_file = save_path / f"{final_name}.parquet"

    if not parts_dir.exists():
        raise ValueError(f"Parts folder not found: {parts_dir}")

    files = sorted(parts_dir.glob("*.parquet"))
    if not files:
        raise ValueError("No parquet parts found")

    print(f"Merging {len(files)} parquet files -> {output_file}")

    schema = pq.ParquetFile(files[0]).schema_arrow
    writer = pq.ParquetWriter(output_file, schema, compression="zstd")

    try:
        total_rows = 0
        table = None
        for file_path in files:
            parquet_file = pq.ParquetFile(file_path)
            for row_group_index in range(parquet_file.num_row_groups):
                table = parquet_file.read_row_group(row_group_index)
                writer.write_table(table)
                total_rows += table.num_rows
            del parquet_file
            if table is not None:
                del table
                table = None
    finally:
        writer.close()

    time.sleep(0.5)
    gc.collect()

    print(f"Merged {total_rows:,} rows")

    if delete_parts:
        rmtree(parts_dir)
        print("Temporary parts folder deleted")

    return output_file


def save_df(df, save_path, name, large_file=False, year=None):
    save_path = Path(save_path)
    if (save_path.exists() and save_path.is_dir()) or save_path.suffix == "":
        base_dir = save_path
    else:
        base_dir = save_path.parent

    if large_file:
        if year is None:
            raise ValueError("year must be provided when large_file=True")

        out_dir = base_dir / LARGE_FILE_TMP_FOLDER
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"{int(year)}.parquet"
    else:
        base_dir.mkdir(parents=True, exist_ok=True)
        file_path = base_dir / f"{name}.parquet"

    pq.write_table(df, file_path, compression="zstd")
    return file_path


def load_metadata_file(metadata_path):
    with open(metadata_path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)
