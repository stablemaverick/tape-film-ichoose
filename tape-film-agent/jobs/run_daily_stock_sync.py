from jobs.common import run_job
from app.services.legacy_stock_sync_service import run_legacy_daily_stock_sync


def main():
    run_job("daily_stock_sync", run_legacy_daily_stock_sync)


if __name__ == "__main__":
    main()
