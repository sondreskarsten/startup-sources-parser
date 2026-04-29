"""Parser for SkatteFUNN innsendte søknader XLSX → unified parquet.

Reads the two XLSX files written by ``startup-sources-collector``
(``sources/skattefunn``) and emits a unified parquet with normalized
orgnr (9-digit zero-padded string), godkjent/avslått split, and
canonical column names regardless of which source file the row came
from.

Input
-----
``gs://{GCS_BUCKET}/skattefunn/raw/label={label}/{date}.xlsx``

Two label partitions:

* ``label=2002_2024`` — historical archive, ~70K søknader 2002–2024,
  godkjent + avslått, columns ``Innsendt dato | Prosjektnummer |
  Bedriftsnavn | Prosjekttittel | Organisasjonsnummer | Fylke |
  Kommunenavn | Søknad godkjent | Søknad avslått |
  Prosjektets fra-år | Prosjektets til-år | Vedtaksdato |
  Populærvitenskapelig sammendrag``.
* ``label=per_januar_2026`` — rolling current cut, ~5K søknader from
  the new søknadssystem (May 2024 onwards), columns ``Søknadsdato |
  Søknadsnummer | Prosjektnummer | Prosjektansvarlig | Prosjekttittel |
  Org.nr | Fylke | Kommune | Poststed | GODKJENT? | Fra-dato |
  Til-dato | Vedtaksdato | Populærvitenskapelig sammendrag``.

Output
------
``gs://{GCS_BUCKET}/skattefunn/state/{date}.parquet``

Columns::

    orgnr (str, 9-digit zero-padded)
    soknad_dato (date)
    prosjekt_nummer (int, nullable)
    soknad_nummer (int, nullable)               # only populated for current cut
    bedriftsnavn (str)
    prosjekt_tittel (str)
    fylke (str)
    kommune (str)
    poststed (str, nullable)                    # only on current cut
    godkjent (bool, nullable)                   # null if undecided
    fra_aar (int, nullable)
    til_aar (int, nullable)
    vedtaksdato (date, nullable)
    populaervitenskapelig_sammendrag (str, nullable)
    source_label (str)                          # "2002_2024" or "per_januar_2026"
    snapshot_date (date)                        # date the source XLSX was downloaded

Deduplication: the two label partitions overlap in 2024 (historical
ends end-2024; current starts May 2024). Priority is current ⇒
historical when both have the same Prosjektnummer; the parser keeps
the row with ``source_label = "per_januar_2026"`` if present.

Environment variables
---------------------
GCS_BUCKET : str
    Source/destination GCS bucket. Default ``sondre_brreg_data``.
SNAPSHOT_DATE : str or None
    Override snapshot date (ISO format). Default = pick the latest
    snapshot present in GCS for each label partition.
"""

import os
import re
import sys
import tempfile
from datetime import date, datetime

import pandas as pd
from google.cloud import storage


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX_RAW = "skattefunn/raw"
GCS_PREFIX_STATE = "skattefunn/state"
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")


def normalize_orgnr(value):
    """Convert any orgnr-shaped value to a 9-digit zero-padded string.

    Parameters
    ----------
    value : Any
        String, int, or float candidate for an orgnr.

    Returns
    -------
    str or None
        ``None`` if the value cannot be coerced to exactly 9 digits.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"\D", "", s)
    if not s:
        return None
    if len(s) > 9:
        return None
    s = s.zfill(9)
    if len(s) != 9 or not s.isdigit():
        return None
    return s


def latest_snapshot(bucket, label):
    """Find the most recent snapshot date present at the given label.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
        The bucket to scan.
    label : str
        Label partition (e.g. ``"2002_2024"`` or ``"per_januar_2026"``).

    Returns
    -------
    str
        ISO date string of the latest ``YYYY-MM-DD.xlsx`` found.

    Raises
    ------
    FileNotFoundError
        If no XLSX is present under the given label.
    """
    prefix = f"{GCS_PREFIX_RAW}/label={label}/"
    blobs = sorted(
        bucket.list_blobs(prefix=prefix), key=lambda b: b.name, reverse=True,
    )
    for b in blobs:
        if b.name.endswith(".xlsx"):
            stem = b.name.rsplit("/", 1)[-1].replace(".xlsx", "")
            return stem
    raise FileNotFoundError(f"No XLSX found under gs://{bucket.name}/{prefix}")


def download_xlsx(bucket, label, snapshot_date):
    """Download one XLSX from GCS into a temp file path.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    label : str
    snapshot_date : str
        ISO date of the snapshot to fetch.

    Returns
    -------
    str
        Local path to the downloaded XLSX.
    """
    blob_name = f"{GCS_PREFIX_RAW}/label={label}/{snapshot_date}.xlsx"
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False).name
    bucket.blob(blob_name).download_to_filename(tmp)
    print(
        f"  fetched gs://{bucket.name}/{blob_name} → {tmp} "
        f"({os.path.getsize(tmp):,} bytes)",
        flush=True,
    )
    return tmp


def parse_historical(path, snapshot_date):
    """Parse the 2002–2024 historical XLSX.

    Parameters
    ----------
    path : str
        Local XLSX path.
    snapshot_date : str
        ISO snapshot date for provenance.

    Returns
    -------
    pandas.DataFrame
        Normalized rows with the unified column set.
    """
    df = pd.read_excel(path, sheet_name="Rapport 1", dtype={"Organisasjonsnummer": str})
    out = pd.DataFrame(
        {
            "orgnr": df["Organisasjonsnummer"].apply(normalize_orgnr),
            "soknad_dato": pd.to_datetime(df["Innsendt dato"], errors="coerce").dt.date,
            "prosjekt_nummer": pd.to_numeric(df["Prosjektnummer"], errors="coerce").astype("Int64"),
            "soknad_nummer": pd.Series([pd.NA] * len(df), dtype="Int64"),
            "bedriftsnavn": df["Bedriftsnavn"].astype(str),
            "prosjekt_tittel": df["Prosjekttittel"].astype(str),
            "fylke": df["Fylke"].astype(str),
            "kommune": df["Kommunenavn"].astype(str),
            "poststed": pd.Series([None] * len(df), dtype="object"),
            "godkjent": _historical_godkjent(df),
            "fra_aar": pd.to_numeric(df["Prosjektets fra-år"], errors="coerce").astype("Int64"),
            "til_aar": pd.to_numeric(df["Prosjektets til-år"], errors="coerce").astype("Int64"),
            "vedtaksdato": pd.to_datetime(df["Vedtaksdato"], errors="coerce").dt.date,
            "populaervitenskapelig_sammendrag": df["Populærvitenskapelig sammendrag"].astype(str),
            "source_label": "2002_2024",
            "snapshot_date": pd.Timestamp(snapshot_date).date(),
        }
    )
    return out


def _historical_godkjent(df):
    """Resolve godkjent boolean from the historical file's two columns.

    The historical file uses ``Søknad godkjent`` and ``Søknad avslått``
    as separate boolean columns. Some rows have both null (undecided
    or returned for revision) — those return ``pd.NA``.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw historical frame.

    Returns
    -------
    pandas.Series
        Nullable boolean series.
    """
    g = df["Søknad godkjent"]
    a = df["Søknad avslått"]
    out = []
    for gv, av in zip(g, a):
        gtrue = bool(gv) if pd.notna(gv) else False
        atrue = bool(av) if pd.notna(av) else False
        if gtrue and not atrue:
            out.append(True)
        elif atrue and not gtrue:
            out.append(False)
        else:
            out.append(pd.NA)
    return pd.array(out, dtype="boolean")


def parse_current(path, snapshot_date):
    """Parse the per-januar-2026 (or successor) rolling XLSX.

    Parameters
    ----------
    path : str
        Local XLSX path.
    snapshot_date : str
        ISO snapshot date for provenance.

    Returns
    -------
    pandas.DataFrame
        Normalized rows with the unified column set.
    """
    df = pd.read_excel(path, sheet_name="Til nettsider")
    godkjent_str = df["GODKJENT?"].astype(str).str.strip().str.upper()
    godkjent = godkjent_str.map({"JA": True, "NEI": False}).astype("boolean")

    out = pd.DataFrame(
        {
            "orgnr": df["Org.nr"].apply(normalize_orgnr),
            "soknad_dato": pd.to_datetime(df["Søknadsdato"], errors="coerce").dt.date,
            "prosjekt_nummer": pd.to_numeric(df["Prosjektnummer"], errors="coerce").astype("Int64"),
            "soknad_nummer": pd.to_numeric(df["Søknadsnummer"], errors="coerce").astype("Int64"),
            "bedriftsnavn": df["Prosjektansvarlig"].astype(str),
            "prosjekt_tittel": df["Prosjekttittel"].astype(str),
            "fylke": df["Fylke"].astype(str),
            "kommune": df["Kommune"].astype(str),
            "poststed": df["Poststed"].astype(str),
            "godkjent": godkjent,
            "fra_aar": pd.to_datetime(df["Fra-dato"], errors="coerce").dt.year.astype("Int64"),
            "til_aar": pd.to_datetime(df["Til-dato"], errors="coerce").dt.year.astype("Int64"),
            "vedtaksdato": pd.to_datetime(df["Vedtaksdato"], errors="coerce").dt.date,
            "populaervitenskapelig_sammendrag": df["Populærvitenskapelig sammendrag"].astype(str),
            "source_label": "per_januar_2026",
            "snapshot_date": pd.Timestamp(snapshot_date).date(),
        }
    )
    return out


def deduplicate(historical, current):
    """Combine the two frames, preferring current rows on overlap.

    Overlap is detected on ``prosjekt_nummer``. Rows with a
    ``prosjekt_nummer`` present in both are kept only from the current
    frame. Historical rows with a null ``prosjekt_nummer`` are kept
    as-is (they cannot collide).

    Parameters
    ----------
    historical : pandas.DataFrame
    current : pandas.DataFrame

    Returns
    -------
    pandas.DataFrame
        Concatenated, deduplicated frame.
    """
    cur_nums = set(
        n for n in current["prosjekt_nummer"].dropna().astype("Int64").tolist()
    )
    keep_hist = historical[
        historical["prosjekt_nummer"].isna()
        | ~historical["prosjekt_nummer"].isin(cur_nums)
    ]
    return pd.concat([keep_hist, current], ignore_index=True)


def write_parquet(df, bucket, snapshot_date):
    """Write the unified frame to GCS as parquet.

    Parameters
    ----------
    df : pandas.DataFrame
    bucket : google.cloud.storage.Bucket
    snapshot_date : str
        ISO date used as filename.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    df.to_parquet(tmp, index=False)
    blob_name = f"{GCS_PREFIX_STATE}/{snapshot_date}.parquet"
    bucket.blob(blob_name).upload_from_filename(tmp)
    size = os.path.getsize(tmp)
    os.unlink(tmp)
    print(
        f"  wrote gs://{bucket.name}/{blob_name} ({size:,} bytes, {len(df):,} rows)",
        flush=True,
    )


def main():
    """Run the SkatteFUNN parser end to end."""
    print(f"{'=' * 60}", flush=True)
    print(f"  skattefunn-parser", flush=True)
    print(f"  bucket: {GCS_BUCKET}", flush=True)
    print(f"  {date.today().isoformat()}", flush=True)
    print(f"{'=' * 60}", flush=True)

    bucket = storage.Client().bucket(GCS_BUCKET)

    if SNAPSHOT_DATE:
        date_hist = SNAPSHOT_DATE
        date_curr = SNAPSHOT_DATE
    else:
        date_hist = latest_snapshot(bucket, "2002_2024")
        date_curr = latest_snapshot(bucket, "per_januar_2026")

    print(f"  snapshot dates: historical={date_hist}, current={date_curr}", flush=True)

    p_hist = download_xlsx(bucket, "2002_2024", date_hist)
    p_curr = download_xlsx(bucket, "per_januar_2026", date_curr)

    df_hist = parse_historical(p_hist, date_hist)
    df_curr = parse_current(p_curr, date_curr)
    print(f"  historical rows: {len(df_hist):,}", flush=True)
    print(f"  current rows:    {len(df_curr):,}", flush=True)

    df = deduplicate(df_hist, df_curr)
    print(f"  combined rows:   {len(df):,}", flush=True)

    n_orgnr = df["orgnr"].notna().sum()
    n_distinct = df["orgnr"].nunique()
    n_god = (df["godkjent"] == True).sum()
    n_avs = (df["godkjent"] == False).sum()
    n_undecided = df["godkjent"].isna().sum()
    print(
        f"  orgnr-resolved: {n_orgnr:,}  distinct orgnrs: {n_distinct:,}",
        flush=True,
    )
    print(
        f"  godkjent: {n_god:,}  avslått: {n_avs:,}  undecided/null: {n_undecided:,}",
        flush=True,
    )

    write_parquet(df, bucket, date.today().isoformat())

    os.unlink(p_hist)
    os.unlink(p_curr)


if __name__ == "__main__":
    main()
