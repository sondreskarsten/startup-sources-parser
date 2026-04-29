# parsers/skattefunn

Parses Forskningsrådet's two SkatteFUNN innsendte søknader XLSX files into a unified parquet.

## Input

```
gs://sondre_brreg_data/skattefunn/raw/
  label=2002_2024/{date}.xlsx           # historical archive (~70K rows)
  label=per_januar_2026/{date}.xlsx     # rolling current cut (~5K rows)
```

## Output

```
gs://sondre_brreg_data/skattefunn/state/{date}.parquet
```

Columns: `orgnr`, `soknad_dato`, `prosjekt_nummer`, `soknad_nummer`, `bedriftsnavn`, `prosjekt_tittel`, `fylke`, `kommune`, `poststed`, `godkjent` (bool nullable), `fra_aar`, `til_aar`, `vedtaksdato`, `populaervitenskapelig_sammendrag`, `source_label`, `snapshot_date`.

## Schema unification

| Concept | Historical column | Current column | Unified column |
|---|---|---|---|
| Submission date | `Innsendt dato` | `Søknadsdato` | `soknad_dato` |
| Project number | `Prosjektnummer` | `Prosjektnummer` | `prosjekt_nummer` |
| Søknad number | — | `Søknadsnummer` | `soknad_nummer` |
| Applicant | `Bedriftsnavn` | `Prosjektansvarlig` | `bedriftsnavn` |
| Org number | `Organisasjonsnummer` (str) | `Org.nr` (int — needs zero-pad) | `orgnr` (str, 9-digit) |
| Result | `Søknad godkjent` / `Søknad avslått` (separate bools) | `GODKJENT?` (`JA` / `NEI`) | `godkjent` (bool, nullable) |
| Project span | `Prosjektets fra-år` / `Prosjektets til-år` (int) | `Fra-dato` / `Til-dato` (datetime) | `fra_aar` / `til_aar` (int) |

## Deduplication

The two label partitions overlap in 2024 (historical ends end-2024; current starts May 2024). Priority is **current ⇒ historical** when both have the same `Prosjektnummer` — the parser keeps the row from `per_januar_2026`. Historical rows with null `Prosjektnummer` are kept as-is (they cannot collide).

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `SNAPSHOT_DATE` | (auto) | Override snapshot date for both label partitions; else picks the latest each independently. |
