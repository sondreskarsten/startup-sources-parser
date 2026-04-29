# parsers/cordis

Unzips CORDIS programme bundles, filters `organization.csv` to Norwegian rows, extracts orgnr from `vatNumber`, and emits one parquet per programme plus a combined parquet across all programmes.

## Input

```
gs://sondre_brreg_data/cordis/raw/
  programme=fp7/{date}.zip            # ~33 MB
  programme=h2020/{date}.zip          # ~55 MB
  programme=horizon/{date}.zip        # ~30 MB
```

Each ZIP contains `organization.csv`, `project.csv`, `topics.csv`, `legalBasis.csv`, `euroSciVoc.csv`, `webLink.csv`, `policyPriorities.csv`, `webItem.csv`, `information.zip`.

## Output

```
gs://sondre_brreg_data/cordis/state/
  programme=fp7/{date}.parquet
  programme=h2020/{date}.parquet
  programme=horizon/{date}.parquet
  all/{date}.parquet                 # union across all programmes
```

Columns: `orgnr` (str, 9-digit zero-padded; nullable), `org_name_raw`, `short_name`, `sme` (bool, nullable), `activity_type`, `role`, `project_id`, `project_acronym`, `project_title`, `start_date`, `end_date`, `total_cost`, `ec_max_contribution`, `ec_contribution`, `net_ec_contribution`, `status`, `country` (always `NO`), `nuts_code`, `city`, `post_code`, `programme`, `snapshot_date`.

## Orgnr extraction from vatNumber

Norwegian VAT numbers come in many shapes:

| Format | Example | Strategy |
|---|---|---|
| Bare orgnr | `123456789` | use as-is |
| Spaced | `123 456 789` | strip whitespace |
| Norwegian VAT | `NO123456789MVA` | strip non-digits, take exactly 9-digit segment |
| With country code padding | `NO 123 456 789 MVA` | as above |
| Empty / missing | `""` / null | row marked `orgnr = null`; recover via name match downstream |
| Foreign | `DE123456789` | parser ignores (returns null) for NO-only output |

If multiple digit runs are present, the parser uses the last 9-digit segment.

## Filtering

The parser includes only `country == "NO"` rows. Other-country rows are silently dropped — they're available via the raw ZIP if ever needed.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `PROGRAMMES` | `fp7,h2020,horizon` | Comma-separated programme codes |
| `SNAPSHOT_DATE` | (auto) | Override; else latest snapshot per programme |

## Known limitations

- VAT number is missing for many HES (higher-education) and REC (research centre) institutions. The downstream layer must do name-resolution against Enhetsregisteret to recover orgnr for these.
- A handful of FP7 rows use full country names instead of ISO 3166 alpha-2 codes (e.g. `Norway` instead of `NO`). The parser currently uses upper-case alpha-2 only — these older rows are dropped. ~50 rows total across FP7.
- `total_cost` and `ec_max_contribution` are at the **project** level, not the participant. `ec_contribution` is participant-level.
