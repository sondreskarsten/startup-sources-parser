"""Parser for Innovasjon Norge tildelingsrapport CSV → unified parquet.

Reads the latin-1 / semicolon-separated CSV written by
``startup-sources-collector`` (``sources/innovasjon_norge``) and
emits a parquet with normalized orgnr (9-digit zero-padded) and
unified column names.

Input
-----
``gs://{GCS_BUCKET}/innovasjon_norge/raw/{date}.csv``

Encoding latin-1, separator ``;``, columns:
``Fylkesnavn | Kommunenavn | Org-nr | Bedriftsnavn |
  Virkemiddelkategori | Underkategori | Innvilget beløp |
  Innvilget dato | Beslutningsenhet | Næringshovedområde |
  Næring | Type finansiering``.

Output
------
``gs://{GCS_BUCKET}/innovasjon_norge/state/{date}.parquet``

Columns::

    orgnr (str, 9-digit zero-padded)
    bedriftsnavn (str)
    fylke (str)
    kommune (str)
    virkemiddelkategori (str)
    underkategori (str)
    innvilget_belop_nok (float)
    innvilget_dato (date)
    beslutningsenhet (str)
    naeringshovedomrade (str)
    naering (str)
    type_finansiering (str)
    snapshot_date (date)

Environment variables
---------------------
GCS_BUCKET : str
    Source/destination GCS bucket. Default ``sondre_brreg_data``.
SNAPSHOT_DATE : str or None
    Override snapshot date. Default = pick the latest CSV present.
"""

import os
import re
import tempfile
from datetime import date

import pandas as pd
from google.cloud import storage


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX_RAW = "innovasjon_norge/raw"
GCS_PREFIX_STATE = "innovasjon_norge/state"
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")


def normalize_orgnr(value):
    """Convert any orgnr-shaped value to a 9-digit zero-padded string.

    Parameters
    ----------
    value : Any

    Returns
    -------
    str or None
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if not digits or len(digits) > 9:
        return None
    digits = digits.zfill(9)
    return digits if len(digits) == 9 else None


def parse_amount(value):
    """Parse a Norwegian-locale NOK amount to float.

    Norwegian conventions: ``;`` thousand separator (sometimes), ``,``
    decimal separator. Strips any non-numeric prefix/suffix.

    Parameters
    ----------
    value : Any

    Returns
    -------
    float or NaN
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    s = str(value).strip()
    if not s:
        return float("nan")
    s = s.replace(" ", "").replace("\xa0", "")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    elif "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def parse_dato(value):
    """Parse a DD.MM.YYYY date string.

    Parameters
    ----------
    value : Any

    Returns
    -------
    datetime.date or None
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return pd.to_datetime(s, format="%d.%m.%Y", errors="coerce").date()
    except Exception:
        return None


def latest_snapshot(bucket):
    """Find the most recent CSV snapshot.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket

    Returns
    -------
    str
        ISO date string.
    """
    prefix = f"{GCS_PREFIX_RAW}/"
    blobs = sorted(
        bucket.list_blobs(prefix=prefix), key=lambda b: b.name, reverse=True,
    )
    for b in blobs:
        if b.name.endswith(".csv"):
            return b.name.rsplit("/", 1)[-1].replace(".csv", "")
    raise FileNotFoundError(f"No CSV under gs://{bucket.name}/{prefix}")


def download_csv(bucket, snapshot_date):
    """Download the CSV into memory.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    snapshot_date : str

    Returns
    -------
    bytes
    """
    blob_name = f"{GCS_PREFIX_RAW}/{snapshot_date}.csv"
    body = bucket.blob(blob_name).download_as_bytes()
    print(
        f"  fetched gs://{bucket.name}/{blob_name} ({len(body):,} bytes)",
        flush=True,
    )
    return body


def parse_csv(body, snapshot_date):
    """Decode and parse the latin-1 CSV.

    Parameters
    ----------
    body : bytes
    snapshot_date : str

    Returns
    -------
    pandas.DataFrame
    """
    text = body.decode("latin-1")
    import io as _io

    df = pd.read_csv(
        _io.StringIO(text), sep=";", dtype=str, keep_default_na=False,
        na_values=[""],
    )
    print(f"  raw CSV rows: {len(df):,}; columns: {list(df.columns)}", flush=True)

    out = pd.DataFrame(
        {
            "orgnr": df["Org-nr"].apply(normalize_orgnr),
            "bedriftsnavn": df["Bedriftsnavn"].astype(str),
            "fylke": df["Fylkesnavn"].astype(str),
            "kommune": df["Kommunenavn"].astype(str),
            "virkemiddelkategori": df["Virkemiddelkategori"].astype(str),
            "underkategori": df["Underkategori"].astype(str),
            "innvilget_belop_nok": df["Innvilget beløp"].apply(parse_amount),
            "innvilget_dato": df["Innvilget dato"].apply(parse_dato),
            "beslutningsenhet": df["Beslutningsenhet"].astype(str),
            "naeringshovedomrade": df["Næringshovedområde"].astype(str),
            "naering": df["Næring"].astype(str),
            "type_finansiering": df["Type finansiering"].astype(str),
            "snapshot_date": pd.Timestamp(snapshot_date).date(),
        }
    )
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
    """Run the Innovasjon Norge parser."""
    print(f"{'=' * 60}", flush=True)
    print(f"  innovasjon-norge-parser", flush=True)
    print(f"  bucket: {GCS_BUCKET}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"{'=' * 60}", flush=True)

    bucket = storage.Client().bucket(GCS_BUCKET)
    snapshot = SNAPSHOT_DATE or latest_snapshot(bucket)
    print(f"  snapshot date: {snapshot}", flush=True)

    body = download_csv(bucket, snapshot)
    df = parse_csv(body, snapshot)
    n_orgnr = df["orgnr"].notna().sum()
    n_distinct = df["orgnr"].nunique()
    print(
        f"  parsed: {len(df):,} rows; {n_orgnr:,} with orgnr; "
        f"{n_distinct:,} distinct orgnrs",
        flush=True,
    )

    today = date.today().isoformat()
    write_parquet(df, bucket, f"{GCS_PREFIX_STATE}/{today}.parquet")


if __name__ == "__main__":
    main()
