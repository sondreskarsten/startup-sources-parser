"""Top-level dispatcher for startup-sources-parser.

Selects which parser to run based on the ``SOURCE`` environment
variable and invokes that parser's ``parse.py``. Each parser lives
under ``parsers/{source_name}/`` and exposes a ``main()`` function.

Available parsers
-----------------
``skattefunn``
    Parses the two Forskningsrådet SkatteFUNN XLSX files into a
    unified parquet with godkjent/avslått split.
``cordis``
    Unzips CORDIS programme bundles, filters organization.csv to
    Norwegian rows, extracts orgnr from vatNumber/name.
``innovasjon_norge``
    Reads the latin-1 tildelingsrapport CSV and emits a normalized
    parquet keyed on Org-nr + Innvilget dato.

Environment variables
---------------------
SOURCE : str
    Parser name to run. Default ``skattefunn``. Must match a
    subdirectory of ``parsers/``.

All other environment variables are forwarded to the parser's
``parse.py``.
"""

import os
import sys
import importlib.util


SOURCE = os.environ.get("SOURCE", "skattefunn")


def main():
    """Dispatch to the selected parser's parse.py main function."""
    here = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(here, "parsers", SOURCE)
    if not os.path.isdir(src_path):
        print(f"Unknown SOURCE: {SOURCE}", flush=True)
        print(
            f"Available parsers: {sorted(os.listdir(os.path.join(here, 'parsers')))}",
            flush=True,
        )
        sys.exit(1)

    sys.path.insert(0, src_path)

    spec = importlib.util.spec_from_file_location(
        f"{SOURCE}_parse", os.path.join(src_path, "parse.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()


if __name__ == "__main__":
    main()
