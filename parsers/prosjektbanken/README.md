# parsers/prosjektbanken

Reads Prosjektbanken JSONL.gz from GCS, expands to one row per (project, organisation) participation, and applies the shared name → orgnr resolver.

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

Columns: `project_id`, `project_title`, `org_name_raw`, `org_role`, `orgnr` (str, 9-digit), `orgnr_source`, `years_active`, `current_activity`, `total_funding_nok` (NaN if redacted), `geographies`, `disciplines`, `kilde`, `snapshot_date`.

## Note on SKATTEFUNN sub-source

The `parsers/skattefunn/` parser supersedes the Prosjektbanken `kilde=SKATTEFUNN` for SkatteFUNN-specific work because the XLSX publications carry direct `Organisasjonsnummer` and include both godkjent and avslått søknader. The Prosjektbanken SKATTEFUNN scrape is kept here for cross-validation and because it carries the populærvitenskapelig sammendrag for godkjente prosjekter.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `KILDER` | `FORISS,EU,SKATTEFUNN` | Comma-separated kilde codes |
| `SNAPSHOT_DATE` | (auto) | Override; else latest snapshot per kilde |
| `RESOLVE_NAMES` | `1` | Run name resolver against Enhetsregisteret + foretakshendelser pool |

## Resolution behaviour

Each project's `organisations` array is expanded to one row per organisation. The resolver runs on `org_name_raw` and writes to `orgnr` + `orgnr_source`. Disambiguation: alive AS > alive any > foretakshendelser pool match.

For projects where the source already provides an orgnr (some EU records do), it would be set as `orgnr_source = "source"` — but the Prosjektbanken JSON does not currently expose orgnr directly, so all rows go through name resolution.
