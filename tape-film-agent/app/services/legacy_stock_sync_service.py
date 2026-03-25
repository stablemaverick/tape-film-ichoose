from jobs.common import LEGACY_ROOT, require_file, run_command
from app.services.legacy_file_locator import (
    find_latest_matching_file,
    get_supplier_file_locations,
)


def run_legacy_daily_stock_sync(check_only: bool = False) -> dict:
    root_dir = LEGACY_ROOT

    fetch_supplier_files = require_file(
        root_dir / "fetch_supplier_files.py",
        hint="Expected legacy supplier fetch script.",
    )

    import_moovies_raw = require_file(
        root_dir / "import_moovies_raw.py",
        hint="Expected legacy Moovies import script.",
    )

    import_lasgo_raw = require_file(
        root_dir / "import_lasgo_raw.py",
        hint="Expected legacy Lasgo import script.",
    )

    normalize_supplier_products = require_file(
        root_dir / "normalize_supplier_products.py",
        hint="Expected legacy normalization script.",
    )

    upsert_supplier_offers = require_file(
        root_dir / "upsert_supplier_offers_to_catalog_items_preserve_tmdb.py",
        hint="Expected legacy supplier upsert script.",
    )

    locations = get_supplier_file_locations()

    if check_only:
        return {
            "status": "ok",
            "job": "daily_stock_sync",
            "mode": "check_only",
            "scripts": {
                "fetch_supplier_files": str(fetch_supplier_files),
                "import_moovies_raw": str(import_moovies_raw),
                "import_lasgo_raw": str(import_lasgo_raw),
                "normalize_supplier_products": str(normalize_supplier_products),
                "upsert_supplier_offers": str(upsert_supplier_offers),
            },
            "file_locations": locations,
        }

    print("[stock-sync] Step 1/5: fetch supplier files")
    run_command(["python", str(fetch_supplier_files)], working_dir=LEGACY_ROOT)

    moovies_filepath = find_latest_matching_file(
        locations["moovies_dir"],
        locations["moovies_pattern"],
    )
    lasgo_filepath = find_latest_matching_file(
        locations["lasgo_dir"],
        locations["lasgo_pattern"],
    )

    print(f"[stock-sync] Latest Moovies file: {moovies_filepath}")
    print(f"[stock-sync] Latest Lasgo file: {lasgo_filepath}")

    print("[stock-sync] Step 2/5: import moovies raw (stock_cost, existing only)")
    run_command(
        [
            "python",
            str(import_moovies_raw),
            moovies_filepath,
            "--mode",
            "stock_cost",
            "--existing-only-in-raw",
        ],
        working_dir=LEGACY_ROOT,
    )

    print("[stock-sync] Step 3/5: import lasgo raw (stock_cost, existing only)")
    run_command(
        [
            "python",
            str(import_lasgo_raw),
            lasgo_filepath,
            "--mode",
            "stock_cost",
            "--existing-only-in-raw",
        ],
        working_dir=LEGACY_ROOT,
    )

    print("[stock-sync] Step 4/5: normalize supplier products")
    run_command(
        [
            "python",
            str(normalize_supplier_products),
        ],
        working_dir=LEGACY_ROOT,
    )

    print("[stock-sync] Step 5/5: upsert supplier offers (existing only)")
    run_command(
        [
            "python",
            str(upsert_supplier_offers),
            "--existing-only",
        ],
        working_dir=LEGACY_ROOT,
    )

    return {
        "status": "ok",
        "job": "daily_stock_sync",
        "mode": "live",
        "moovies_filepath": moovies_filepath,
        "lasgo_filepath": lasgo_filepath,
    }
