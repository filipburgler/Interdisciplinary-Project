import operator
import re
from functools import reduce

import pandas as pd


def filter_by_date(df, earliest, latest, year_col="year", month_col="month"):
    if earliest is None or latest is None:
        return None

    dates = pd.to_datetime(
        dict(year=df[year_col], month=df[month_col], day=1)
    )
    return (dates >= earliest) & (dates <= latest)


def filter_by_event_name(df, *names, name_col="name"):
    if not names or names[0] is None:
        return None
    
    if len(names) == 1 and isinstance(names[0], (list, tuple, set)):
        names = names[0]
    
    names = [n for n in names if n]
    if not names: return None

    text = (df[name_col].fillna("").astype(str))

    pattern = "|".join(re.escape(name) for name in names)

    return text.str.contains(pattern, case=False, na=False)


def filter_by_affected(df, *affected, affected_col="affected"):
    if not affected or affected[0] is None:
        return None
    
    if len(affected) == 1 and isinstance(affected[0], (list, tuple, set)):
        affected = affected[0]

    return df[affected_col].isin(affected)


def filter_by_mentions(df, *mentioned, event_col="event", description_col="description"):
    if not mentioned or mentioned[0] is None:
        return None
    
    if len(mentioned) == 1 and isinstance(mentioned[0], (list, tuple, set)):
        mentioned = mentioned[0]

    mentioned = [m for m in mentioned if m]
    if not mentioned: return None

    text = (
        df[event_col].fillna("").astype(str) + " " +
        df[description_col].fillna("").astype(str)
    )

    pattern = "|".join(re.escape(mention) for mention in mentioned)

    return text.str.contains(pattern, case=False, na=False)


def combine_masks(*masks, mode="and"):
    masks = [m for m in masks if m is not None]

    if not masks:
        return None

    if mode.lower() == "and":
        return reduce(operator.and_, masks)
    elif mode.lower() == "or":
        return reduce(operator.or_, masks)
    else:
        raise ValueError("Mode must be 'and' or 'or'")