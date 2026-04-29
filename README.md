# startup-sources-parser

Parser-stage repo for startup-domain external data sources. Reads raw archives written by [`startup-sources-collector`](https://github.com/sondreskarsten/startup-sources-collector) from GCS, normalizes orgnr to 9-digit zero-padded strings, unifies schemas across source-file variants, and writes parquet outputs back to GCS.

This is the airgapped counterpart to the collector — no external API access, only GCS reads from canonical raw paths and parquet writes to canonical state paths.

## Layout

```
startup-sources-parser/
  parse.py                     # top-level dispatcher (selects by SOURCE env var)
  Dockerfile
  requirements.txt
  README.md
  parsers/
    skattefunn/
      parse.py                 # 2 XLSX → unified parquet, godkjent/avslått split
      README.md
    cordis/
      parse.py                 # 3 ZIPs → NO-only parquets per programme + combined
      README.md
    innovasjon_norge/
      parse.py                 # latin-1 CSV → unified parquet
      README.md
```

## Parsers

| Parser | Status | Reads | Writes | orgnr behaviour |
|---|---|---|---|---|
| `skattefunn` | live | `skattefunn/raw/label=*/{date}.xlsx` (2 files) | `skattefunn/state/{date}.parquet` | direct from `Organisasjonsnummer` / `Org.nr` |
| `cordis` | live | `cordis/raw/programme={fp7,h2020,horizon}/{date}.zip` | `cordis/state/programme={p}/{date}.parquet` + `cordis/state/all/{date}.parquet` | extracted from `vatNumber` (in-source field); null otherwise |
| `innovasjon_norge` | live | `innovasjon_norge/raw/{date}.csv` | `innovasjon_norge/state/{date}.parquet` | direct from `Org-nr` (last 9 of 11-digit format); null for ENK |
| `prosjektbanken` | live | `prosjektbanken/raw/source={kilde}/{date}.jsonl.gz` | `prosjektbanken/state/source={k}/{date}.parquet` + `state/all/{date}.parquet` | always null (source has no orgnr field) |

## Architecture: parsers preserve LUAS as-observed

Every parser in this repo follows the same rule: **orgnr is populated only when the source row itself supplies one.** The parser will normalise an in-source field (parse `vatNumber` to extract the digit segment, zero-pad an integer column, take the last 9 of an 11-digit storage format) — that is field-level normalisation within a single source row, not a cross-source join. The parser will **not** name-match against `enheter` or `foretakshendelser pool` to recover orgnr from a name field. That's a query-time concern.

This keeps each parser's output a faithful materialisation of one source's observation at one moment in time. The shared `parsers/_common/resolver.py` module exists as a tool for downstream consumers who want to attach orgnrs at query time, but it is not called by any parser here.

## Usage

```bash
# Run the SkatteFUNN parser
SOURCE=skattefunn python3 parse.py

# Override snapshot date
SOURCE=skattefunn SNAPSHOT_DATE=2026-04-29 python3 parse.py

# Run only Horizon Europe in CORDIS
SOURCE=cordis PROGRAMMES=horizon python3 parse.py

# All parsers in sequence
for s in skattefunn cordis innovasjon_norge; do SOURCE=$s python3 parse.py; done
```

## Conventions

- **Orgnr is always a 9-digit zero-padded string.** Numeric Excel values lose their leading zeros — every parser zero-pads.
- **Snapshot dating is preserved in every output row** (`snapshot_date` column).
- **Source labels are preserved** when one source has multiple file variants (e.g. SkatteFUNN's `2002_2024` vs `per_januar_2026`).
- **Parquet outputs are written with default sort order** (no per-source sort key); the integration ledger downstream applies its own ordering.
- **No name resolution against Enhetsregisteret in this repo.** That's a downstream join step. The parser keeps `org_name_raw` as observed in the source.

## Common environment variables

| Variable | Default | Description |
|---|---|---|
| `SOURCE` | `skattefunn` | Parser under `parsers/` to run. |
| `GCS_BUCKET` | `sondre_brreg_data` | Source/destination bucket. |
| `SNAPSHOT_DATE` | (auto) | Override snapshot date (ISO format); else picks the latest snapshot present. |

Per-parser flags are documented in each `parsers/{name}/README.md`.

## Cloud Run

| Parser | Job name | Schedule (Europe/Oslo) | CPU / Mem |
|---|---|---|---|
| skattefunn | `skattefunn-parser` | `30 6 * * 1` weekly Mondays | 1 vCPU / 1 GiB |
| cordis | `cordis-parser` | `0 7 1 * *` monthly 1st | 2 vCPU / 4 GiB |
| innovasjon_norge | `innovasjon-norge-parser` | `0 7 * * *` daily | 1 vCPU / 1 GiB |

Image: `europe-north1-docker.pkg.dev/sondreskarsten-d7d14/brreg-pipelines/startup-sources-parser:latest`
