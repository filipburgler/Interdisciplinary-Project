import time

from ..config import DEFAULT_DATA_OUTPUT_DIR, DEFAULT_GENERATION_BATCHES, DEFAULT_METADATA_DIR
from .pipeline_core import generate_synthetic_from_metadata


def main():
    start = time.time()

    data_path = DEFAULT_DATA_OUTPUT_DIR
    metadata_path = DEFAULT_METADATA_DIR

    for batch in DEFAULT_GENERATION_BATCHES:
        batch_start = time.time()
        generate_synthetic_from_metadata(
            metadata_path=(metadata_path / batch["metadata_file"]),
            save_path=data_path,
            name=batch["output_name"],
            seed=batch["seed"],
        )
        label = batch["output_name"].removeprefix("synthetic_")
        print(f"{label} done in {time.time() - batch_start}")

    print(f"Finished in {time.time() - start}")


if __name__ == "__main__":
    main()
