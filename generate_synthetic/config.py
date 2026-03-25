from pathlib import Path
import string


LARGE_FILE_ROW_CUTOFF = 40_000_000
LARGE_FILE_TMP_FOLDER = "parts_of_large_file"

_PACKAGE_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _PACKAGE_DIR.parent

DEFAULT_DATA_OUTPUT_DIR = _PROJECT_DIR / "data" / "raw"
DEFAULT_METADATA_DIR = _PROJECT_DIR / "data" / "metadata"
DEFAULT_VALIDATION_REPORT_PATH = _PROJECT_DIR / "test_synthetic" / "metadata_validation_report.txt"
DEFAULT_GENERATION_BATCHES = (
    {
        "metadata_file": "metadata_batch_extra.json",
        "output_name": "synthetic_extra",
        "seed": 42,
    },
    {
        "metadata_file": "metadata_batch_intra.json",
        "output_name": "synthetic_intra",
        "seed": 34,
    },
    {
        "metadata_file": "metadata_batch_prodcom.json",
        "output_name": "synthetic_prodcom",
        "seed": 64,
    },
    {
        "metadata_file": "metadata_batch_urs_re.json",
        "output_name": "synthetic_urs",
        "seed": 72,
    },
)
DEFAULT_FILE_METADATA_PAIRS = tuple(
    (f"{batch['output_name']}.parquet", batch["metadata_file"]) for batch in DEFAULT_GENERATION_BATCHES
)

FIRM_ID_NAMES = ("kzr", "KZ_R", "kz_r", "kzz", "kzk", "kzs")
ID_CODE_ALPHABET = string.ascii_uppercase + string.digits
ID_SCHEMAS = {
    "default": {
        "a": 22695477,
        "b": 1,
    },
    "firm_id": {
        "a": 1103515245,
        "b": 12345,
    },
    "food_code": {
        "a": 1664525,
        "b": 1013904223,
    },
}
COUNTRY_CODES = (
    "AT", "DE", "IT", "CZ", "SK", "HU", "SI", "CH", "LI",
    "BE", "BG", "CY", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "IE", "LT", "LU", "LV", "QX",
    "MT", "NL", "PL", "PT", "RO", "SE",
    "GB", "NO", "IS", "US", "CN", "JP", "KR", "TW", "CA",
    "AL", "BA", "ME", "MK", "RS", "XK", "UA", "MD", "BY", "RU",
    "TR", "IL", "EG", "TN", "DZ", "MA",
    "AE", "SA", "QA", "KW", "OM", "BH", "JO", "IQ", "IR", "YE",
    "VN", "TH", "MY", "SG", "ID", "PH", "KH", "LA", "MM", "BN", "HK", "MO",
    "MX", "BR", "AR", "CL", "CO", "PE", "VE", "EC", "UY", "PY", "BO",
    "GT", "CR", "PA", "DO", "CU", "HT", "JM", "TT", "BS", "BB", "BZ", "HN", "NI", "SV", "PR",
    "ZA", "NG", "KE", "GH", "SN", "CI", "CM", "ET", "TZ", "UG", "RW",
    "ZM", "ZW", "MW", "MZ", "NA", "BW", "AO", "CD", "CG", "GA", "GM",
    "GN", "GW", "SL", "LR", "SO", "SS", "SD", "SC", "ST", "TD", "NE", "BF", "BI", "BJ", "CF", "ER", "EH",
    "IN", "PK", "BD", "LK", "NP", "AF", "KZ", "KG", "UZ", "TJ", "TM", "MN",
    "AU", "NZ", "PG", "FJ", "SB", "VU", "WS", "TO", "TV",
    "FO", "GL",
    "AW", "AI", "BM", "KY", "VG", "VI", "MS", "TC", "SX", "CW", "BQ", "MF", "BL", "GP", "MQ", "RE",
    "PM", "NC", "PF", "WF", "YT",
    "AQ", "BV", "HM", "TF", "GS", "UM", "SJ", "SH", "PN", "TK", "NU", "NR", "CX", "CC", "NF", "AX",
    "AD", "AG", "AM", "AN", "AS", "AZ", "BT", "CK", "CV", "DJ", "DM", "FK", "FM", "GD", "GE", "GF", "GG",
    "GI", "GQ", "GU", "GY", "IM", "IO", "JE", "KI", "KM", "KN", "KP", "LB", "LC", "LS", "LY",
    "MC", "MG", "MH", "ML", "MP", "MR", "MU", "MV", "PS", "PW", "SM", "SR", "SY", "SZ", "TG", "TL", "VA", "VC",
)
