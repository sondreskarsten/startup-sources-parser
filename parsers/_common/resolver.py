"""Standalone name → orgnr resolver — NOT used by any parser in this repo.

This module exists as a tool for downstream consumers and ad-hoc query-time
work. **No parser in this repo calls it.** Each parser preserves the source
LUAS as-observed: an orgnr column is populated only when the source itself
provides one, never inferred via cross-pipeline name match. Cross-pipeline
joining is a query-time concern, not a write-time one.

When you do want to resolve names downstream, you can import this module
or run it as a standalone step. It joins against two name catalogues:

1. **Enhetsregisteret current snapshot** at
   ``gs://{bucket}/enheter/parsed/v1/state/{date}.parquet`` —
   ~1.1M legal entities, current state, with ``name``,
   ``legal_form``, ``deletion_date``.
2. **Foretakshendelser pool** at
   ``gs://{bucket}/foretakshendelser/state/pool.parquet`` —
   historical foretaksnavn for ~1.3M orgnrs including dissolved
   firms.

Resolution strategy
-------------------
1. Normalize names: uppercase, collapse whitespace, strip leading/trailing.
2. Pass A — exact match on the normalized name across both catalogues.
3. Pass B — strip the legal-form suffix (`AS`, `ASA`, `SA`) and retry.
4. Disambiguation when one normalized name maps to multiple orgnrs:
   alive AS/ASA from enheter > alive (any legal form) from enheter
   > foretakshendelser pool match.

Public surface
--------------
:func:`build_name_index`
    Loads enheter + foretakshendelser pool, builds the combined
    name → orgnr lookup. Cached after first call.
:func:`resolve_names`
    Takes a DataFrame with an ``org_name_raw`` column (and optionally
    an existing ``orgnr`` column) and returns the same frame with
    ``orgnr`` filled in where resolvable, plus a ``orgnr_source``
    column ("source" | "enheter_current_exact" |
    "enheter_current_stripped" | "foretakshendelser_pool_exact" |
    "foretakshendelser_pool_stripped" | None).
"""

import os
import re
import tempfile

import pandas as pd
from google.cloud import storage


GCS_BUCKET_DEFAULT = "sondre_brreg_data"

_NAME_INDEX_CACHE = {}


def normalize_name(value):
    """Normalize a Norwegian organization name for exact matching.

    Parameters
    ----------
    value : Any

    Returns
    -------
    str or None
        ``None`` if blank.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).upper().strip()
    s = re.sub(r"\s+", " ", s)
    if not s:
        return None
    return s


def _strip_legal_suffix(name_norm):
    """Strip a trailing AS / ASA / SA / BA / DA / ANS suffix.

    Parameters
    ----------
    name_norm : str
        Already-normalized name (uppercase, single-spaced).

    Returns
    -------
    str
        Name without legal-form suffix; identical to input if no
        suffix present.
    """
    if not name_norm:
        return name_norm
    return re.sub(r"\s+(AS|ASA|SA|BA|DA|ANS)\s*$", "", name_norm).strip()


def _latest_enheter(bucket):
    """Find the most recent enheter parsed snapshot.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket

    Returns
    -------
    str
        Full blob path of the latest snapshot.

    Raises
    ------
    FileNotFoundError
        If no snapshot is present.
    """
    blobs = sorted(
        bucket.list_blobs(prefix="enheter/parsed/v1/state/"),
        key=lambda b: b.name, reverse=True,
    )
    for b in blobs:
        if b.name.endswith(".parquet"):
            return b.name
    raise FileNotFoundError("No enheter parsed snapshot found")


def _download_to_tmp(bucket, blob_name, suffix=".parquet"):
    """Download a blob to a temp file.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    blob_name : str
    suffix : str

    Returns
    -------
    str
        Local path.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False).name
    bucket.blob(blob_name).download_to_filename(tmp)
    return tmp


def build_name_index(bucket_name=None):
    """Construct the combined enheter + foretakshendelser name index.

    Result is cached per bucket — repeated calls are cheap.

    Parameters
    ----------
    bucket_name : str or None
        Defaults to ``sondre_brreg_data``.

    Returns
    -------
    pandas.DataFrame
        Columns: ``orgnr``, ``name_norm``, ``name_norm_stripped``,
        ``name_canonical``, ``legal_form``, ``deletion_date``,
        ``name_source``, ``priority`` (int).
    """
    bucket_name = bucket_name or GCS_BUCKET_DEFAULT
    if bucket_name in _NAME_INDEX_CACHE:
        return _NAME_INDEX_CACHE[bucket_name]

    bucket = storage.Client().bucket(bucket_name)

    enh_path = _download_to_tmp(bucket, _latest_enheter(bucket))
    fh_path = _download_to_tmp(bucket, "foretakshendelser/state/pool.parquet")

    enh = pd.read_parquet(enh_path, columns=["org_nr", "name", "legal_form", "deletion_date"])
    enh = enh[enh["name"].notna()].copy()
    enh["orgnr"] = enh["org_nr"].astype(str).str.zfill(9)
    enh["name_norm"] = enh["name"].apply(normalize_name)
    enh["name_canonical"] = enh["name"]
    enh["name_source"] = "enheter_current"
    enh = enh[["orgnr", "name_norm", "name_canonical", "legal_form", "deletion_date", "name_source"]]
    enh = enh[enh["name_norm"].notna()]

    fh = pd.read_parquet(fh_path, columns=["orgnr", "foretaksnavn"])
    fh = fh[fh["foretaksnavn"].notna()].copy()
    fh["orgnr"] = fh["orgnr"].astype(str).str.zfill(9)
    fh["name_norm"] = fh["foretaksnavn"].apply(normalize_name)
    fh["name_canonical"] = fh["foretaksnavn"]
    fh["legal_form"] = pd.NA
    fh["deletion_date"] = pd.NaT
    fh["name_source"] = "foretakshendelser_pool"
    fh = fh[["orgnr", "name_norm", "name_canonical", "legal_form", "deletion_date", "name_source"]]
    fh = fh[fh["name_norm"].notna()]

    combined = pd.concat([enh, fh], ignore_index=True).drop_duplicates(
        subset=["orgnr", "name_norm", "name_source"]
    )

    is_alive = (
        (combined["name_source"] == "enheter_current")
        & combined["deletion_date"].isna()
    )
    is_as = combined["legal_form"].isin(["AS", "ASA"]).fillna(False)
    is_enheter = combined["name_source"] == "enheter_current"
    combined["priority"] = (
        is_alive.astype(int) * 100
        + is_as.astype(int) * 50
        + is_enheter.astype(int) * 10
    )

    combined["name_norm_stripped"] = combined["name_norm"].apply(_strip_legal_suffix)

    os.unlink(enh_path)
    os.unlink(fh_path)

    _NAME_INDEX_CACHE[bucket_name] = combined
    print(
        f"  name index built: {len(combined):,} (name, orgnr) pairs "
        f"covering {combined['orgnr'].nunique():,} distinct orgnrs",
        flush=True,
    )
    return combined


def _disambiguate(matches):
    """Select the best orgnr when one normalized name resolves to many.

    Parameters
    ----------
    matches : pandas.DataFrame
        Has at least ``priority``, ``orgnr``, ``name_canonical``,
        ``name_source``.

    Returns
    -------
    pandas.DataFrame
        One row per ``_input_index`` group, with the highest-priority
        match retained.
    """
    return (
        matches.sort_values("priority", ascending=False)
        .drop_duplicates(subset=["_input_index"], keep="first")
    )


def resolve_names(df, name_col="org_name_raw", orgnr_col="orgnr",
                  bucket_name=None):
    """Fill in ``orgnr`` for rows where it's null but ``name_col`` is set.

    Resolves via two passes: exact normalized match, then suffix-stripped
    match. Best match per row is selected by priority (alive AS >
    alive any > pool).

    Parameters
    ----------
    df : pandas.DataFrame
        Source frame. ``orgnr_col`` may be partially populated; only
        rows where it's null get a resolution attempt.
    name_col : str
        Column with the raw organization name.
    orgnr_col : str
        Column with the orgnr (will be filled in-place where resolvable).
    bucket_name : str or None
        GCS bucket holding the name index sources.

    Returns
    -------
    pandas.DataFrame
        Copy of ``df`` with ``orgnr`` filled where resolvable plus a
        new column ``orgnr_source`` documenting how each row's orgnr
        was obtained.
    """
    out = df.copy()
    if "orgnr_source" not in out.columns:
        out["orgnr_source"] = pd.NA
    out.loc[out[orgnr_col].notna(), "orgnr_source"] = "source"

    candidates = out[out[orgnr_col].isna() & out[name_col].notna()].copy()
    if candidates.empty:
        return out

    candidates["_input_index"] = candidates.index
    candidates["_name_norm"] = candidates[name_col].apply(normalize_name)
    candidates = candidates[candidates["_name_norm"].notna()]
    if candidates.empty:
        return out

    print(f"  resolving {len(candidates):,} rows by name", flush=True)

    idx = build_name_index(bucket_name)

    pass_a = candidates.merge(
        idx[["orgnr", "name_norm", "name_source", "priority"]].rename(
            columns={"orgnr": "_orgnr_resolved"}
        ),
        left_on="_name_norm", right_on="name_norm", how="inner",
    )
    pass_a = _disambiguate(pass_a)
    pass_a["orgnr_source"] = pass_a["name_source"].apply(
        lambda s: f"{s}_exact"
    )
    print(f"    pass A (exact): hits = {len(pass_a):,}", flush=True)

    matched_a = set(pass_a["_input_index"])
    remaining = candidates[~candidates["_input_index"].isin(matched_a)].copy()
    pass_b_hits = pd.DataFrame()
    if not remaining.empty:
        remaining["_name_norm_stripped"] = remaining["_name_norm"].apply(_strip_legal_suffix)
        remaining = remaining[
            remaining["_name_norm_stripped"].notna()
            & (remaining["_name_norm_stripped"] != remaining["_name_norm"])
        ]
        if not remaining.empty:
            pass_b = remaining.merge(
                idx[["orgnr", "name_norm_stripped", "name_source", "priority"]].rename(
                    columns={"orgnr": "_orgnr_resolved"}
                ),
                left_on="_name_norm_stripped", right_on="name_norm_stripped",
                how="inner",
            )
            pass_b = _disambiguate(pass_b)
            pass_b["orgnr_source"] = pass_b["name_source"].apply(
                lambda s: f"{s}_stripped"
            )
            pass_b_hits = pass_b
            print(f"    pass B (stripped): hits = {len(pass_b_hits):,}", flush=True)

    if not pass_b_hits.empty:
        all_hits = pd.concat([pass_a, pass_b_hits], ignore_index=True)
    else:
        all_hits = pass_a

    for _, row in all_hits.iterrows():
        idx_orig = row["_input_index"]
        out.at[idx_orig, orgnr_col] = row["_orgnr_resolved"]
        out.at[idx_orig, "orgnr_source"] = row["orgnr_source"]

    n_resolved = (out["orgnr_source"].notna() & (out["orgnr_source"] != "source")).sum()
    print(
        f"    total newly resolved: {n_resolved:,} of {len(candidates):,} "
        f"unresolved candidates ({100 * n_resolved / max(len(candidates), 1):.1f}%)",
        flush=True,
    )
    return out
