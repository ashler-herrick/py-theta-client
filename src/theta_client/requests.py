from typing import List, Dict
from enum import Enum
from urllib.parse import urlencode, quote
from io import StringIO
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
import csv


import httpx

from theta_client.job import Schema


THETA_BASE_URL = "http://0.0.0.0:25503/v3"


class Interval(Enum):
    TICK = "tick"
    MS10 = "10ms"
    MS100 = "100ms"
    MS500 = "500ms"
    S1 = "1s"
    S5 = "5s"
    S15 = "15s"
    S30 = "30s"
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"


class AssetClass(Enum):
    STOCK = "stock"
    OPTION = "option"


class DataType(Enum):
    HISTORY = "history"
    SNAPSHOT = "snapshot"


class Endpoint(Enum):
    EOD = "eod"
    QUOTE = "quote"
    TRADE = "trade"
    TRADE_QUOTE = "trade_quote"
    GREEKS_FIRST_ORDER = "greeks/first_order"
    GREEKS_EOD = "greeks/eod"


class FileGranularity(Enum):
    DAILY = "daily"
    MONTHLY = "monthly"


class ThetaRequest:
    minio_folder = ""

    def __init__(
        self,
        *,
        symbol: str,
        start_date: int,
        end_date: int,
        asset_class: AssetClass,
        data_type: DataType,
        endpoint: Endpoint,
        interval: Interval = Interval.H1,
        force_refresh: bool = False,
        file_granularity: FileGranularity = FileGranularity.MONTHLY,
    ) -> None:
        """
        Args:
            symbol(str): Underlying symbol.
            start_date(int): Start data in integer format YYYYMMDD
            end_date(int): End date in integer format YYYYMMDD
            asset_class(AssetClass): Asset class (STOCK or OPTION).
            data_type(DataType): Data type (HISTORY or SNAPSHOT).
            endpoint(Endpoint): API endpoint (EOD, QUOTE, or GREEKS_EOD).
            interval(int): Response interval in ms. Default is 3,600,000 corresponding to 1 hour.
            file_format(Formats): Format to use when writing to the lake. Default is 'parquet'
            file_granularity(FileGranularity): Granularity to concatenate response to.
        """
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.asset_class = asset_class
        self.data_type = data_type
        self.endpoint = endpoint
        self.interval = interval
        self.force_refresh = force_refresh
        self.file_granularity = file_granularity
        self._validate_params()

    def __repr__(self):
        params = ", ".join(f"{k}={v!r}" for k, v in vars(self).items())
        return f"{self.__class__.__name__}({params})"

    def _validate_params(self) -> None:
        pass

    def _build_base_url(self) -> str:
        """Construct v3 URL: /v3/{asset_class}/{data_type}/{endpoint}"""
        return f"{THETA_BASE_URL}/{self.asset_class.value}/{self.data_type.value}/{self.endpoint.value}"

    def _create_urls_per_day(self, days: List[str]) -> List[str]:
        urls = []
        base_url = self._build_base_url()

        base_params = {
            "symbol": self.symbol,
        }

        # Add interval if not EOD endpoint
        if self.endpoint != Endpoint.EOD and self.endpoint != Endpoint.GREEKS_EOD:
            base_params["interval"] = self.interval.value

        # Add wildcard parameters for options
        if self.asset_class == AssetClass.OPTION:
            base_params["expiration"] = "*"
            base_params["strike"] = "*"

        for d in days:
            # Create a string like YYYYMMDD
            if self.endpoint != Endpoint.EOD and self.endpoint != Endpoint.GREEKS_EOD:
                params = base_params | {"date": d}
            else:
                params = base_params | {"start_date": d, "end_date": d}

            def quote_keep_star(s, safe, encoding, errors):
                return quote(s, safe="*")

            urls.append(f"{base_url}?{urlencode(params, quote_via=quote_keep_star)}")  # type: ignore

        return urls

    def _get_valid_dates(self) -> List[str]:
        url = f"{THETA_BASE_URL}/stock/list/dates/quote"
        params = {"symbol": self.symbol}

        response = httpx.get(url, params=params)
        response.raise_for_status()

        reader = csv.DictReader(StringIO(response.text))
        dates = []
        for row in reader:
            date_str = row["date"].replace("-", "")
            # Parse date and check if it's a weekday (Monday=0 through Friday=4)
            dt = datetime.strptime(date_str, "%Y%m%d")
            if dt.weekday() < 5:  # 0-4 are weekdays, 5-6 are weekend
                dates.append(date_str)
        return dates

    def _generate_date_range(self) -> list[str]:
        # Convert integers to datetime objects
        start = datetime.strptime(str(self.start_date), "%Y%m%d")
        end = datetime.strptime(str(self.end_date), "%Y%m%d")

        # Generate all dates in range
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)

        return dates

    def _map_dates_to_yearmo(
        self, dates: list[str]
    ) -> Dict[Tuple[str, str], List[str]]:
        yearmo_dict = defaultdict(list)

        for date in dates:
            year = date[:4]
            month = date[4:6]
            day = date[6:]
            yearmo_key = (year, month)
            yearmo_dict[yearmo_key].append(date)

        return dict(yearmo_dict)

    def get_key_map(self) -> Dict[str, List[str]]:
        int_str = (
            "1d"
            if self.endpoint in [Endpoint.EOD, Endpoint.GREEKS_EOD]
            else self.interval
        )
        base_key = f"{self.minio_folder}/{self.endpoint.value}/{self.file_granularity.value}/{int_str}/{self.symbol}"
        given_dates = self._generate_date_range()
        valid_dates = self._get_valid_dates()
        final_dates = list(set(valid_dates).intersection(set(given_dates)))
        day_map = self._map_dates_to_yearmo(final_dates)
        key_map: Dict[str, List[str]] = {}
        if self.file_granularity == FileGranularity.MONTHLY:
            key_map = {
                f"{base_key}/{k[0]}/{k[1]}/data.parquet": self._create_urls_per_day(v)
                for k, v in day_map.items()
            }
        elif self.file_granularity == FileGranularity.DAILY:
            key_map = {
                f"{base_key}/{d[0]}/{d[1]}/{d[2]}/data.parquet": self._create_urls_per_day(
                    d
                )
                for d in day_map.values()
            }

        return key_map

    def get_schema(self) -> Schema:
        """Override in subclasses to return appropriate schema."""
        raise NotImplementedError("Subclasses must implement get_schema()")


class StockRequest(ThetaRequest):
    headers = None

    def __init__(
        self,
        *,
        symbol: str,
        start_date: int,
        end_date: int,
        data_type: DataType,
        endpoint: Endpoint,
        interval: Interval = Interval.H1,
        force_refresh: bool = False,
        file_granularity: FileGranularity = FileGranularity.MONTHLY,
    ) -> None:
        # Set minio_folder based on data_type
        self.minio_folder = f"thetadata/stock/{data_type.value}"

        super().__init__(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            asset_class=AssetClass.STOCK,
            data_type=data_type,
            endpoint=endpoint,
            interval=interval,
            force_refresh=force_refresh,
            file_granularity=file_granularity,
        )

    def get_schema(self) -> Schema:
        if self.endpoint == Endpoint.EOD:
            return Schema.STOCK_EOD
        return Schema.STOCK_QUOTE

    def _validate_params(self) -> None:
        if self.endpoint not in [Endpoint.EOD, Endpoint.QUOTE]:
            raise ValueError(
                f"Invalid endpoint for stock: {self.endpoint}. Valid: EOD, QUOTE"
            )


class OptionRequest(ThetaRequest):
    headers = None

    def __init__(
        self,
        *,
        symbol: str,
        start_date: int,
        end_date: int,
        data_type: DataType,
        endpoint: Endpoint,
        interval: Interval = Interval.H1,
        force_refresh: bool = False,
        file_granularity: FileGranularity = FileGranularity.MONTHLY,
    ) -> None:
        self.minio_folder = f"thetadata/option/{data_type.value}"

        super().__init__(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            asset_class=AssetClass.OPTION,
            data_type=data_type,
            endpoint=endpoint,
            interval=interval,
            force_refresh=force_refresh,
            file_granularity=file_granularity,
        )

    def get_schema(self) -> Schema:
        if self.endpoint == Endpoint.GREEKS_EOD:
            return Schema.GREEK_EOD
        elif self.endpoint == Endpoint.GREEKS_FIRST_ORDER:
            return Schema.GREEK_FIRST_ORDER
        elif self.endpoint == Endpoint.EOD:
            return Schema.OPTION_EOD
        elif self.endpoint == Endpoint.TRADE:
            return Schema.OPTION_TRADE
        elif self.endpoint == Endpoint.TRADE_QUOTE:
            return Schema.OPTION_TRADE_QUOTE
        return Schema.OPTION_QUOTE

    def _validate_params(self) -> None:
        valid_endpoints = [
            Endpoint.EOD,
            Endpoint.QUOTE,
            Endpoint.TRADE,
            Endpoint.TRADE_QUOTE,
            Endpoint.GREEKS_EOD,
            Endpoint.GREEKS_FIRST_ORDER,
        ]
        if self.endpoint not in valid_endpoints:
            raise ValueError(
                f"Invalid endpoint for option: {self.endpoint}. "
                f"Valid: {', '.join(e.value for e in valid_endpoints)}"
            )
