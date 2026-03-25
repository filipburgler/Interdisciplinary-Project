import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from generate_synthetic.config import DEFAULT_FILE_METADATA_PAIRS

from test_synthetic.validation import format_validation_report, run_full_validation


class TestSyntheticMetadataReport(unittest.TestCase):
    def test_default_configuration_covers_all_datasets(self):
        self.assertEqual(len(DEFAULT_FILE_METADATA_PAIRS), 4)

    def test_single_dataset_report_is_renderable(self):
        with TemporaryDirectory() as tmp_dir:
            report = run_full_validation(
                [("synthetic_prodcom.parquet", "metadata_batch_prodcom.json")],
                fail_on_differences=False,
                report_path=Path(tmp_dir) / "report.txt",
            )
            rendered = format_validation_report(report)
            self.assertIn("synthetic_prodcom.parquet", rendered)
            self.assertIn("Total differences", rendered)
            self.assertTrue(report.report_path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
