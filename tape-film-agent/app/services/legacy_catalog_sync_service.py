from jobs.common import LEGACY_ROOT, require_file, run_command


def run_legacy_catalog_sync(check_only: bool = False) -> dict:
    root_dir = LEGACY_ROOT

    normalize_supplier_products = require_file(
        root_dir / "normalize_supplier_products.py",
        hint="Expected legacy normalization script.",
    )

    upsert_supplier_offers = require_file(
        root_dir / "upsert_supplier_offers_to_catalog_items_preserve_tmdb.py",
        hint="Expected legacy supplier upsert script.",
    )

    enrich_catalog = require_file(
        root_dir / "enrich_catalog_with_tmdb_v2.py",
        hint="Expected legacy TMDB enrichment script.",
    )

    build_films = require_file(
        root_dir / "build_films_from_catalog.py",
        hint="Expected legacy films build script.",
    )

    if check_only:
        return {
            "status": "ok",
            "job": "catalog_sync",
            "mode": "check_only",
            "scripts": {
                "normalize_supplier_products": str(normalize_supplier_products),
                "upsert_supplier_offers": str(upsert_supplier_offers),
                "enrich_catalog": str(enrich_catalog),
                "build_films": str(build_films),
            },
        }

    print("[catalog-sync] Step 1/4: normalize supplier products")
    run_command(
        [
            "python",
            str(normalize_supplier_products),
        ],
        working_dir=LEGACY_ROOT,
    )

    print("[catalog-sync] Step 2/4: upsert supplier offers")
    run_command(
        [
            "python",
            str(upsert_supplier_offers),
        ],
        working_dir=LEGACY_ROOT,
    )

    print("[catalog-sync] Step 3/4: enrich catalog with TMDB")
    run_command(
        [
            "python",
            str(enrich_catalog),
        ],
        working_dir=LEGACY_ROOT,
    )

    print("[catalog-sync] Step 4/4: build films from catalog")
    run_command(
        [
            "python",
            str(build_films),
        ],
        working_dir=LEGACY_ROOT,
    )

    return {
        "status": "ok",
        "job": "catalog_sync",
        "mode": "live",
    }
