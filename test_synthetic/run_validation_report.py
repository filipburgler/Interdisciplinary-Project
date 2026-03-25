from generate_synthetic.config import DEFAULT_FILE_METADATA_PAIRS

from .validation import format_validation_report, run_full_validation


def main(file_metadata_pairs=None, fail_on_differences=False):
    report = run_full_validation(
        file_metadata_pairs=file_metadata_pairs or DEFAULT_FILE_METADATA_PAIRS,
        fail_on_differences=fail_on_differences,
    )
    print(format_validation_report(report))
    return report


if __name__ == "__main__":
    main()
