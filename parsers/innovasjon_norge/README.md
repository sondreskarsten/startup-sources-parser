# parsers/innovasjon_norge

Reads the latin-1 / semicolon-separated tildelingsrapport CSV and emits a normalized parquet.

## Input

```
gs://sondre_brreg_data/innovasjon_norge/raw/{date}.csv
```

Encoding latin-1 (Windows-1252-compatible), separator `;`. Columns: `Fylkesnavn`, `Kommunenavn`, `Org-nr`, `Bedriftsnavn`, `Virkemiddelkategori`, `Underkategori`, `Innvilget beløp`, `Innvilget dato`, `Beslutningsenhet`, `Næringshovedområde`, `Næring`, `Type finansiering`.

## Output

```
gs://sondre_brreg_data/innovasjon_norge/state/{date}.parquet
```

Columns: `orgnr` (str, 9-digit zero-padded), `bedriftsnavn`, `fylke`, `kommune`, `virkemiddelkategori`, `underkategori`, `innvilget_belop_nok` (float), `innvilget_dato` (date), `beslutningsenhet`, `naeringshovedomrade`, `naering`, `type_finansiering`, `snapshot_date`.

## Normalisations

- `Org-nr` → `orgnr`: strip non-digits, zero-pad to 9 digits. Foreign or otherwise non-9-digit rows are kept but with `orgnr = null`.
- `Innvilget beløp` → `innvilget_belop_nok`: handles Norwegian-locale decimals (`,` decimal, `.` or space thousands).
- `Innvilget dato` → `innvilget_dato`: `DD.MM.YYYY` format.

## Deduplication

Not done in this parser — Innovasjon Norge has no per-tildeling stable ID, so dedup must use the composite `(orgnr, innvilget_dato, underkategori, innvilget_belop_nok)` and is handled in the integration ledger downstream.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `SNAPSHOT_DATE` | (auto) | Override; else latest snapshot |
