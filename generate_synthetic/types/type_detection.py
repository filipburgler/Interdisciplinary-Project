from ..config import FIRM_ID_NAMES


def detect_var_type(meta):
    if meta.get("unique_values_per_year") and not isinstance(meta["unique_values_per_year"], int) and all(
        value == 1 for value in meta["unique_values_per_year"]
    ):
        return "year_like"

    if meta.get("numeric_summary"):
        return "numeric"

    if meta.get("level_counts"):
        return "finite_categorical"

    if meta.get("class", "").startswith("character"):
        if meta.get("country_codes", "") == "yes":
            return "coded_countries"
        return "coded_categorical"

    if meta.get("class", "").startswith("integer"):
        return "number_categorical"

    if meta.get("class", "").startswith("logical"):
        return "logical"

    raise ValueError("Unknown variable type")


def detect_category_type(var_name):
    if any(name in var_name for name in FIRM_ID_NAMES):
        return ("firm_id", 0.75)
    return ("default", 0)
