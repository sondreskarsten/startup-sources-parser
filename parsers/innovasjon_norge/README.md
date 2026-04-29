# parsers/innovasjon_norge

Reads the latin-1 / semicolon-separated tildelingsrapport CSV and emits a normalized parquet.

## Architecture note: orgnr only when the source supplies it

The parser performs **in-source field normalisation** on `Org-nr` (parsing the 11-digit `00xxxxxxxxx` zero-prefixed format → 9-digit orgnr) but does **not** join against `enheter` or `foretakshendelser pool` to recover orgnr from `Bedriftsnavn` for ENK rows where the source has no `Org-nr`. Sole-proprietorship rows stay with `orgnr = null` and `bedriftsnavn` populated. Name → orgnr resolution is a separate downstream concern.

## Input

```
gs://sondre_brreg_data/innovasjon_norge/raw/{date}.csv
```

Encoding latin-1 (Windows-1252-compatible), separator `;`. Columns: `Fylkesnavn`, `Kommunenavn`, `Org-nr`, `Bedriftsnavn`, `Virkemiddelkategori`, `Underkategori`, `Innvilget beløp`, `Innvilget dato`, `Beslutningsenhet`, `Næringshovedområde`, `Næring`, `Type finansiering`.

## Output

```
gs://sondre_brreg_data/innovasjon_norge/state/{date}.parquet
```

Columns: `orgnr` (str, 9-digit zero-padded; null for ENK rows), `bedriftsnavn`, `fylke`, `kommune`, `virkemiddelkategori`, `underkategori`, `innvilget_belop_nok` (float), `innvilget_dato` (date), `beslutningsenhet`, `naeringshovedomrade`, `naering`, `type_finansiering`, `snapshot_date`.

## Normalisations

- `Org-nr` → `orgnr`: strip non-digits, zero-pad to 9 digits. Foreign or otherwise non-9-digit rows are kept but with `orgnr = null`.
- `Innvilget beløp` → `innvilget_belop_nok`: handles Norwegian-locale decimals (`,` decimal, `.` or space thousands).
- `Innvilget dato` → `innvilget_dato`: `DD.MM.YYYY` format.

## Known caveats

- Innovasjon Norge uses `00xxxxxxxxx` (11-digit, two leading zeros) as the storage format for valid Norwegian orgnrs. The parser strips and takes the last 9. A small subset of rows (~1,000) carry IN-internal codes like `00005xxxxxxx` (foreign UN agencies, IN regional offices) which produce `005xxxxxxx`-style strings that are not valid BRREG orgnrs. These survive the parser as `orgnr` with the literal value; downstream consumers should validate against the register if invalid orgnrs would be a problem.
- No deduplication. Innovasjon Norge has no per-tildeling stable ID — dedup must use the composite `(orgnr, innvilget_dato, underkategori, innvilget_belop_nok)` and is handled at query time.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `SNAPSHOT_DATE` | (auto) | Override; else latest snapshot |
