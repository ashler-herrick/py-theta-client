#!/usr/bin/env python
"""
Bulk request stock data from ThetaData.

Usage:
    python scripts/thetadata/bulk_request.py [--universe sp500] [--threads 4]
"""
import argparse

from theta_client.client import ThetaClient, MinIOConfig
from theta_client.requests import DataType, Endpoint, FileGranularity, Interval, StockRequest, OptionRequest


def main():
    parser = argparse.ArgumentParser(
        description="Bulk request stock data from ThetaData"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of parallel threads (default: 4)",
    )
    args = parser.parse_args()

    client = ThetaClient(
        num_threads=args.threads,
        storage_config=MinIOConfig(),
        show_progress=False,
        log_level="DEBUG",
    )

    req = OptionRequest(
        symbol="AAPL",
        start_date=20251231,
        end_date=20251231,
        data_type=DataType.AT_TIME,
        endpoint=Endpoint.QUOTE,
        file_granularity=FileGranularity.MONTHLY,
        force_refresh=False,
        time_of_day="15:45:00.000"
    )
    client.request_data(req)


if __name__ == "__main__":
    main()
