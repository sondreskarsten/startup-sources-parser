"""Parser for Prosjektbanken JSONL.gz → unified parquet of NO orgnrs.

Reads the gzipped JSONL files written by ``startup-sources-collector``
(``sources/prosjektbanken``), expands each project's ``organisations``
array to one row per (project, organisation) participation, applies
the shared ``_common.resolver`` to map organisation names to orgnrs,
and emits a parquet partitioned by source (FORISS / EU / SKATTEFUNN).

Input
-----
``gs://{GCS_BUCKET}/prosjektbanken/raw/source={kilde}/{date}.jsonl.gz``

Each JSONL line is one project record::

    {
      "id": <int>,
      "title": <str>,
      "organisations": [{"name": <str>, "role": <str>, ...}, ...],
      "yearsActive": [<int>, ...],
      "geographies": [...],
      "disciplines": [...],
      "currentActivity": <str>,
      "totalFunding": <float, -1 means redacted>,
      "_kilde": "FORISS"|"EU"|"SKATTEFUNN"
    }

The downstream-relevant LUAS is **(project_id, organisation_role)** —
one row per organisation that participated in the project.

Output
------
``gs://{GCS_BUCKET}/prosjektbanken/state/source={kilde}/{date}.parquet``
plus combined ``gs://{GCS_BUCKET}/prosjektbanken/state/all/{date}.parquet``.

Columns::

    project_id (int)
    project_title (str)
    org_name_raw (str)                 # name as observed in source
    org_role (str)                     # PROSJEKTANSVARLIG / SAMARBEIDSPARTNER / etc.
    orgnr (str, 9-digit zero-padded; nullable)
    orgnr_source (str)                 # how orgnr was obtained
    years_active (list of int)
    current_activity (str)
    total_funding_nok (float; NaN if redacted/-1)
    geographies (str — pipe-separated codes)
    disciplines (str — pipe-separated codes)
    kilde (str)                        # FORISS / EU / SKATTEFUNN
    snapshot_date (date)

Environment variables
---------------------
GCS_BUCKET : str
    Default ``sondre_brreg_data``.
SNAPSHOT_DATE : str or None
    Override; else picks the latest snapshot per kilde.
KILDER : str
    Comma-separated kilde codes. Default ``FORISS,EU,SKATTEFUNN``.
RESOLVE_NAMES : str
    ``"1"`` to enable name → orgnr resolution. Default ``"1"``.
"""

import gzip
import json
import os
import sys
import tempfile
from datetime import date

import pandas as pd
from google.cloud import storage


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX_RAW = "prosjektbanken/raw"
GCS_PREFIX_STATE = "prosjektbanken/state"
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")
KILDER = [
    k.strip().upper()
    for k in os.environ.get("KILDER", "FORISS,EU,SKATTEFUNN").split(",")
    if k.strip()
]
RESOLVE_NAMES = os.environ.get("RESOLVE_NAMES", "1") not in ("0", "false", "False", "")


def latest_snapshot(bucket, kilde):
    """Find the most recent JSONL.gz snapshot for a kilde.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    kilde : str
        ``FORISS`` / ``EU`` / ``SKATTEFUNN`` (case-sensitive — paths use
        lowercase).

    Returns
    -------
    str
        ISO date string of latest snapshot.

    Raises
    ------
    FileNotFoundError
    """
    prefix = f"{GCS_PREFIX_RAW}/source={kilde.lower()}/"
    blobs = sorted(
        bucket.list_blobs(prefix=prefix), key=lambda b: b.name, reverse=True,
    )
    for b in blobs:
        if b.name.endswith(".jsonl.gz"):
            return b.name.rsplit("/", 1)[-1].replace(".jsonl.gz", "")
    raise FileNotFoundError(f"No JSONL.gz under gs://{bucket.name}/{prefix}")


def download_jsonl(bucket, kilde, snapshot_date):
    """Download and decompress one kilde's JSONL.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    kilde : str
    snapshot_date : str

    Returns
    -------
    list of dict
        Parsed project records.
    """
    blob_name = f"{GCS_PREFIX_RAW}/source={kilde.lower()}/{snapshot_date}.jsonl.gz"
    tmp = tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False).name
    bucket.blob(blob_name).download_to_filename(tmp)
    print(
        f"  fetched gs://{bucket.name}/{blob_name} ({os.path.getsize(tmp):,} bytes)",
        flush=True,
    )
    records = []
    with gzip.open(tmp, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    os.unlink(tmp)
    return records


def _extract_org_names(record):
    """Extract organisation names from a Prosjektbanken record.

    The three Prosjektbanken sub-sources have different schemas:

    * FORISS / EU: ``organisations`` is a list of lists in the form
      ``[sector, group, INSTITUTION_NAME, faculty, dept]`` (3 to 5
      elements). The institution name is at index 2.
    * SKATTEFUNN (legacy flattened format): organisations field is
      absent; the company name is in the flat ``org_name`` /
      ``all_orgs`` keys. ``all_orgs`` may carry multiple comma- or
      semicolon-separated names if a project has more than one
      participant.

    Parameters
    ----------
    record : dict
        One Prosjektbanken record.

    Returns
    -------
    list of (name, role) tuples
        ``role`` may be ``None`` when the source does not specify it.
    """
    orgs = record.get("organisations")
    if isinstance(orgs, list) and orgs:
        out = []
        for o in orgs:
            if isinstance(o, list) and len(o) >= 3:
                name = o[2]
                if name:
                    out.append((str(name), None))
            elif isinstance(o, dict):
                name = o.get("name")
                if name:
                    out.append((str(name), o.get("role")))
            elif isinstance(o, str) and o:
                out.append((o, None))
        if out:
            return out

    flat_name = record.get("org_name")
    flat_all = record.get("all_orgs")
    if flat_all and isinstance(flat_all, str):
        parts = [p.strip() for p in flat_all.replace(";", ",").split(",")]
        parts = [p for p in parts if p]
        if parts:
            return [(p, None) for p in parts]
    if flat_name and isinstance(flat_name, str):
        return [(flat_name.strip(), None)]
    return []


def expand_to_participations(records, kilde, snapshot_date):
    """Expand project records to one row per (project, organisation).

    Parameters
    ----------
    records : list of dict
    kilde : str
    snapshot_date : str

    Returns
    -------
    pandas.DataFrame
    """
    rows = []
    for r in records:
        names = _extract_org_names(r)
        project_id = r.get("id") or r.get("project_id")
        years_active = r.get("yearsActive")
        if years_active is None:
            ys = r.get("startYear")
            ye = r.get("endYear")
            years_active = list(range(ys, ye + 1)) if (ys is not None and ye is not None) else []
        current_activity = r.get("currentActivity")
        if isinstance(current_activity, dict):
            current_activity = current_activity.get("activity")
        if current_activity is None:
            current_activity = r.get("sector_label")
        common = {
            "project_id": project_id,
            "project_title": r.get("title"),
            "years_active": "|".join(str(y) for y in (years_active or [])),
            "current_activity": current_activity,
            "total_funding_nok": _funding(r.get("totalFunding")),
            "geographies": "|".join(_codes(r.get("geographies"))),
            "disciplines": "|".join(_codes(r.get("disciplines"))),
            "kilde": kilde,
            "snapshot_date": pd.Timestamp(snapshot_date).date(),
        }
        if not names:
            rows.append(
                {**common, "org_name_raw": None, "org_role": None}
            )
            continue
        for name, role in names:
            rows.append(
                {**common, "org_name_raw": name, "org_role": role}
            )
    df = pd.DataFrame(rows)
    df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce").astype("Int64")
    df["orgnr"] = pd.NA
    return df


def _funding(value):
    """Convert Prosjektbanken's totalFunding to a NaN-safe NOK float.

    SkatteFUNN funding is redacted by Forskningsrådet and returned as
    ``-1``. EU contracts denominate in EUR sometimes — left as-is here
    since the Prosjektbanken JSON does not expose currency.

    Parameters
    ----------
    value : Any

    Returns
    -------
    float
    """
    if value is None:
        return float("nan")
    try:
        v = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if v < 0:
        return float("nan")
    return v


def _codes(maybe_list):
    """Coerce a Prosjektbanken codeList to a list of code strings.

    Parameters
    ----------
    maybe_list : list of dict or list of str or None

    Returns
    -------
    list of str
    """
    if not maybe_list:
        return []
    out = []
    for item in maybe_list:
        if isinstance(item, dict):
            code = item.get("code") or item.get("name") or item.get("id")
            if code is not None:
                out.append(str(code))
        else:
            out.append(str(item))
    return out


def write_parquet(df, bucket, blob_name):
    """Write parquet to GCS.

    Parameters
    ----------
    df : pandas.DataFrame
    bucket : google.cloud.storage.Bucket
    blob_name : str
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    df.to_parquet(tmp, index=False)
    bucket.blob(blob_name).upload_from_filename(tmp)
    size = os.path.getsize(tmp)
    os.unlink(tmp)
    print(
        f"  wrote gs://{bucket.name}/{blob_name} ({size:,} bytes, {len(df):,} rows)",
        flush=True,
    )


def main():
    """Run the Prosjektbanken parser for each configured kilde."""
    print(f"{'=' * 60}", flush=True)
    print(f"  prosjektbanken-parser", flush=True)
    print(f"  bucket: {GCS_BUCKET}", flush=True)
    print(f"  kilder: {KILDER}", flush=True)
    print(f"  resolve_names: {RESOLVE_NAMES}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"{'=' * 60}", flush=True)

    bucket = storage.Client().bucket(GCS_BUCKET)
    today = date.today().isoformat()

    all_frames = []
    for kilde in KILDER:
        snapshot = SNAPSHOT_DATE or latest_snapshot(bucket, kilde)
        print(f"  --- {kilde} (snapshot {snapshot}) ---", flush=True)
        records = download_jsonl(bucket, kilde, snapshot)
        print(f"  records: {len(records):,}", flush=True)
        df = expand_to_participations(records, kilde, snapshot)
        print(f"  participations: {len(df):,}", flush=True)
        if RESOLVE_NAMES:
            from _common.resolver import resolve_names
            df = resolve_names(df, name_col="org_name_raw", orgnr_col="orgnr",
                               bucket_name=GCS_BUCKET)
        n_orgnr = df["orgnr"].notna().sum()
        n_distinct = df["orgnr"].nunique()
        print(
            f"  {kilde}: {len(df):,} rows, {n_orgnr:,} with orgnr, "
            f"{n_distinct:,} distinct orgnrs",
            flush=True,
        )
        write_parquet(
            df, bucket, f"{GCS_PREFIX_STATE}/source={kilde.lower()}/{today}.parquet"
        )
        all_frames.append(df)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        n_orgnr = combined["orgnr"].notna().sum()
        n_distinct = combined["orgnr"].nunique()
        print(
            f"\n  combined: {len(combined):,} rows across {len(KILDER)} kilder; "
            f"{n_orgnr:,} with orgnr; {n_distinct:,} distinct orgnrs",
            flush=True,
        )
        write_parquet(combined, bucket, f"{GCS_PREFIX_STATE}/all/{today}.parquet")


if __name__ == "__main__":
    main()
