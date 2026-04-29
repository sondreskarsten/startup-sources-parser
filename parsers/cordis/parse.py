"""Parser for CORDIS bulk CSV exports → unified parquet of NO participations.

Reads the CORDIS programme ZIPs written by ``startup-sources-collector``
(``sources/cordis``), unzips ``organization.csv`` and ``project.csv``,
filters to ``country == "NO"``, extracts orgnr from ``vatNumber``, and
emits one parquet per programme plus a combined parquet across all
programmes.

Input
-----
``gs://{GCS_BUCKET}/cordis/raw/programme={programme}/{date}.zip``
for ``programme ∈ {fp7, h2020, horizon}``.

Each ZIP contains ``organization.csv`` and ``project.csv`` (plus
several auxiliary CSVs not consumed here).

Output
------
``gs://{GCS_BUCKET}/cordis/state/programme={programme}/{date}.parquet``
plus a combined ``gs://{GCS_BUCKET}/cordis/state/all/{date}.parquet``.

Columns::

    orgnr (str, 9-digit zero-padded, may be null if not resolvable)
    org_name_raw (str)
    short_name (str)
    sme (bool, nullable)
    activity_type (str)
    role (str)
    project_id (int)
    project_acronym (str)
    project_title (str)
    start_date (date)
    end_date (date)
    total_cost (float, NaN-safe)
    ec_max_contribution (float)
    ec_contribution (float)               # this organization's share
    net_ec_contribution (float)
    status (str)
    country (str, "NO")
    nuts_code (str)
    city (str)
    post_code (str)
    programme (str, "fp7" / "h2020" / "horizon")
    snapshot_date (date)

Norwegian VAT numbers can appear in several formats: ``NO123456789MVA``,
``NO 123 456 789 MVA``, ``123456789``, ``123 456 789``, or empty
(missing — common for HES institutions). The parser strips
non-digit characters; if the result is exactly 9 digits it is used
as the orgnr, else ``None``.

Environment variables
---------------------
GCS_BUCKET : str
    Source/destination GCS bucket. Default ``sondre_brreg_data``.
SNAPSHOT_DATE : str or None
    Override snapshot date. Default = pick the latest snapshot per
    programme.
PROGRAMMES : str
    Comma-separated programme codes. Default ``fp7,h2020,horizon``.
"""

import csv
import io
import os
import re
import tempfile
import zipfile
from datetime import date

import pandas as pd
from google.cloud import storage


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX_RAW = "cordis/raw"
GCS_PREFIX_STATE = "cordis/state"
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")
PROGRAMMES = [
    p.strip().lower()
    for p in os.environ.get("PROGRAMMES", "fp7,h2020,horizon").split(",")
    if p.strip()
]
RESOLVE_NAMES = os.environ.get("RESOLVE_NAMES", "1") not in ("0", "false", "False", "")


def normalize_orgnr_from_vat(value):
    """Extract a 9-digit orgnr from a CORDIS vatNumber string.

    Norwegian VAT numbers come in many shapes:
    ``NO123456789MVA``, ``NO 123 456 789 MVA``, ``123456789``,
    ``123 456 789``, or empty. This function strips all non-digit
    characters and returns the 9-digit suffix if present.

    Parameters
    ----------
    value : Any
        VAT number string or NaN.

    Returns
    -------
    str or None
        9-digit orgnr, or ``None`` if not resolvable.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) == 9:
        return digits
    if len(digits) > 9:
        # Some entries have country prefix digits or trailing zeros from MVA;
        # try the last 9 digits if they parse.
        tail = digits[-9:]
        if tail.isdigit():
            return tail
    return None


def latest_snapshot(bucket, programme):
    """Find the most recent snapshot date for a programme.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    programme : str
        ``fp7`` / ``h2020`` / ``horizon``.

    Returns
    -------
    str
        ISO date string of latest ZIP.

    Raises
    ------
    FileNotFoundError
        If no ZIP present for the programme.
    """
    prefix = f"{GCS_PREFIX_RAW}/programme={programme}/"
    blobs = sorted(
        bucket.list_blobs(prefix=prefix), key=lambda b: b.name, reverse=True,
    )
    for b in blobs:
        if b.name.endswith(".zip"):
            return b.name.rsplit("/", 1)[-1].replace(".zip", "")
    raise FileNotFoundError(f"No ZIP found under gs://{bucket.name}/{prefix}")


def download_zip_bytes(bucket, programme, snapshot_date):
    """Download one programme's ZIP into memory.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    programme : str
    snapshot_date : str

    Returns
    -------
    bytes
    """
    blob_name = f"{GCS_PREFIX_RAW}/programme={programme}/{snapshot_date}.zip"
    body = bucket.blob(blob_name).download_as_bytes()
    print(
        f"  fetched gs://{bucket.name}/{blob_name} "
        f"({len(body):,} bytes)",
        flush=True,
    )
    return body


def parse_programme(zip_body, programme, snapshot_date):
    """Extract NO rows from one programme's ZIP and join with project.csv.

    Parameters
    ----------
    zip_body : bytes
        ZIP file contents.
    programme : str
        Programme code.
    snapshot_date : str
        ISO snapshot date for provenance.

    Returns
    -------
    pandas.DataFrame
    """
    with zipfile.ZipFile(io.BytesIO(zip_body)) as zf:
        with zf.open("organization.csv") as f:
            org = pd.read_csv(
                f, sep=";", encoding="utf-8",
                dtype=str, keep_default_na=False, na_values=[""],
                quoting=csv.QUOTE_ALL, engine="python",
                on_bad_lines="skip",
            )
        with zf.open("project.csv") as f:
            proj = pd.read_csv(
                f, sep=";", encoding="utf-8",
                dtype=str, keep_default_na=False, na_values=[""],
                quoting=csv.QUOTE_ALL, engine="python",
                on_bad_lines="skip",
            )

    org_no = org[org["country"].str.upper() == "NO"].copy()
    print(
        f"  {programme}: organization rows = {len(org):,}; NO rows = {len(org_no):,}",
        flush=True,
    )

    org_no["orgnr"] = org_no["vatNumber"].apply(normalize_orgnr_from_vat)

    # Coerce numerics
    for col in ["ecContribution", "netEcContribution", "totalCost"]:
        if col in org_no.columns:
            org_no[col] = pd.to_numeric(org_no[col], errors="coerce")

    proj_subset = proj[
        ["id", "acronym", "title", "startDate", "endDate", "totalCost",
         "ecMaxContribution", "status"]
    ].copy()
    proj_subset.rename(
        columns={
            "id": "projectID",
            "acronym": "project_acronym",
            "title": "project_title",
            "startDate": "start_date",
            "endDate": "end_date",
            "totalCost": "project_total_cost",
            "ecMaxContribution": "ec_max_contribution",
            "status": "status",
        },
        inplace=True,
    )
    proj_subset["start_date"] = pd.to_datetime(
        proj_subset["start_date"], errors="coerce"
    ).dt.date
    proj_subset["end_date"] = pd.to_datetime(
        proj_subset["end_date"], errors="coerce"
    ).dt.date
    for col in ["project_total_cost", "ec_max_contribution"]:
        proj_subset[col] = pd.to_numeric(proj_subset[col], errors="coerce")

    org_no["projectID"] = org_no["projectID"].astype(str)
    proj_subset["projectID"] = proj_subset["projectID"].astype(str)

    merged = org_no.merge(proj_subset, on="projectID", how="left")

    out = pd.DataFrame(
        {
            "orgnr": merged["orgnr"],
            "org_name_raw": merged["name"],
            "short_name": merged["shortName"],
            "sme": merged["SME"].map({"true": True, "false": False}).astype("boolean"),
            "activity_type": merged["activityType"],
            "role": merged["role"],
            "project_id": pd.to_numeric(merged["projectID"], errors="coerce").astype("Int64"),
            "project_acronym": merged["project_acronym"],
            "project_title": merged["project_title"],
            "start_date": merged["start_date"],
            "end_date": merged["end_date"],
            "total_cost": merged["project_total_cost"],
            "ec_max_contribution": merged["ec_max_contribution"],
            "ec_contribution": merged["ecContribution"],
            "net_ec_contribution": merged["netEcContribution"],
            "status": merged["status"],
            "country": merged["country"],
            "nuts_code": merged["nutsCode"],
            "city": merged["city"],
            "post_code": merged["postCode"],
            "programme": programme,
            "snapshot_date": pd.Timestamp(snapshot_date).date(),
        }
    )
    return out


def write_parquet(df, bucket, blob_name):
    """Write a parquet to GCS.

    Parameters
    ----------
    df : pandas.DataFrame
    bucket : google.cloud.storage.Bucket
    blob_name : str
        Full object path.
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
    """Run the CORDIS parser across all configured programmes."""
    print(f"{'=' * 60}", flush=True)
    print(f"  cordis-parser", flush=True)
    print(f"  bucket: {GCS_BUCKET}", flush=True)
    print(f"  programmes: {PROGRAMMES}", flush=True)
    print(f"  resolve_names: {RESOLVE_NAMES}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"{'=' * 60}", flush=True)

    bucket = storage.Client().bucket(GCS_BUCKET)
    today = date.today().isoformat()

    all_frames = []
    for programme in PROGRAMMES:
        snapshot = SNAPSHOT_DATE or latest_snapshot(bucket, programme)
        print(f"  --- {programme} (snapshot {snapshot}) ---", flush=True)
        body = download_zip_bytes(bucket, programme, snapshot)
        df = parse_programme(body, programme, snapshot)
        if RESOLVE_NAMES:
            from _common.resolver import resolve_names
            df = resolve_names(df, name_col="org_name_raw", orgnr_col="orgnr",
                               bucket_name=GCS_BUCKET)
        n_orgnr = df["orgnr"].notna().sum()
        n_distinct = df["orgnr"].nunique()
        print(
            f"  {programme}: {len(df):,} NO rows, "
            f"{n_orgnr:,} with orgnr, {n_distinct:,} distinct orgnrs",
            flush=True,
        )
        write_parquet(df, bucket, f"{GCS_PREFIX_STATE}/programme={programme}/{today}.parquet")
        all_frames.append(df)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        n_orgnr = combined["orgnr"].notna().sum()
        n_distinct = combined["orgnr"].nunique()
        print(
            f"\n  combined: {len(combined):,} NO rows across "
            f"{len(PROGRAMMES)} programmes; "
            f"{n_orgnr:,} with orgnr; {n_distinct:,} distinct orgnrs",
            flush=True,
        )
        write_parquet(combined, bucket, f"{GCS_PREFIX_STATE}/all/{today}.parquet")


if __name__ == "__main__":
    main()
