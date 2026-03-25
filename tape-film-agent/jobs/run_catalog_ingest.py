from jobs.common import run_job
from app.services.legacy_catalog_sync_service import run_legacy_catalog_sync


def main():
    run_job("catalog_sync", run_legacy_catalog_sync)


if __name__ == "__main__":
    main()
