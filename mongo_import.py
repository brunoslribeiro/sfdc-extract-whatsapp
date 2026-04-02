import os
import sys


def _bootstrap_src_path() -> None:
    here = os.path.dirname(__file__)
    src = os.path.join(here, "src")
    if os.path.isdir(src) and src not in sys.path:
        sys.path.insert(0, src)


def main() -> None:
    _bootstrap_src_path()
    from sfdc_whatsapp_export.import_mongo_cli import main as cli_main

    cli_main()


if __name__ == "__main__":
    main()

