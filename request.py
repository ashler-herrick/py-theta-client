from theta_client.client import ThetaClient, MinIOConfig
from theta_client.requests import StockRequest, DataType, Endpoint, FileGranularity, Interval
from theta_client.utils import get_symbol_universe

#univ = get_symbol_universe(index="sp400")

# Create client with log level configuration
# Options: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
# show_progress: True (default) shows live Rich progress display
#                False disables progress display and enables console logging
client = ThetaClient(
    num_threads=4,
    storage_config=MinIOConfig(),
    show_progress=False,  # Set to False to disable progress display and see logs
    log_level="DEBUG",
)

req = StockRequest(
    symbol="AAPL",
    start_date=20250101,
    end_date=20251231,
    data_type=DataType.HISTORY,
    endpoint=Endpoint.QUOTE,
    file_granularity=FileGranularity.MONTHLY,
    interval=Interval.M15,
    force_refresh=False,
)

client.request_data(req)
