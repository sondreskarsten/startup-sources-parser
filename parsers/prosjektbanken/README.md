# parsers/prosjektbanken

Reads Prosjektbanken JSONL.gz from GCS and expands to one row per (project, organisation) participation.

## Architecture note: orgnr is always null

The Prosjektbanken JSON does not carry organisasjonsnummer for any record — only organisation names (with FORISS/EU using a sectoral list-of-lists, and SKATTEFUNN using a flat `org_name` field). This parser preserves the source LUAS faithfully and emits `orgnr = null` on every row, with the original organisation name in `org_name_raw`.

Name → orgnr resolution is a separate downstream concern. Either run it as a query-time view against `enheter` ∪ `foretakshendelser pool`, or build a dedicated resolver pipeline whose state is rebuilt independently of this parser. The shared `parsers/_common/resolver.py` module is a tool you can reuse for either approach, but it is **not** called from any parser in this repo.

## Input

```
gs://sondre_brreg_data/prosjektbanken/raw/source={kilde}/{date}.jsonl.gz
```

with `kilde ∈ {foriss, eu, skattefunn}`.

## Output

```
gs://sondre_brreg_data/prosjektbanken/state/
  source=foriss/{date}.parquet
  source=eu/{date}.parquet
  source=skattefunn/{date}.parquet
  all/{date}.parquet
```

Columns: `project_id`, `project_title`, `org_name_raw`, `org_role`, `orgnr` (always null), `years_active`, `current_activity`, `total_funding_nok` (NaN if redacted), `geographies`, `disciplines`, `kilde`, `snapshot_date`.

## Note on SKATTEFUNN sub-source

The `parsers/skattefunn/` parser supersedes the Prosjektbanken `kilde=SKATTEFUNN` for SkatteFUNN-specific work because the XLSX publications carry direct `Organisasjonsnummer` and include both godkjent and avslått søknader. The Prosjektbanken SKATTEFUNN scrape is kept here for cross-validation and because it carries the populærvitenskapelig sammendrag for godkjente prosjekter.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `KILDER` | `FORISS,EU,SKATTEFUNN` | Comma-separated kilde codes |
| `SNAPSHOT_DATE` | (auto) | Override; else latest snapshot per kilde |
