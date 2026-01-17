from theta_client.client import ThetaClient, MinIOConfig
from theta_client.requests import OptionRequest, DataType, Endpoint, FileGranularity
from theta_client.utils import get_symbol_universe

univ = get_symbol_universe()

# Create client with log level configuration
# Options: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
# show_progress: True (default) shows live Rich progress display
#                False disables progress display and enables console logging
client = ThetaClient(
    num_threads=4,
    storage_config=MinIOConfig(),
    log_level="DEBUG",  # Change to "DEBUG" for verbose output, "WARNING" for quiet
    show_progress=True,  # Set to False to disable progress display and see logs
)

# for symbol in univ:
req = OptionRequest(
    symbol="NVDA",
    start_date=20160101,
    end_date=20251231,
    data_type=DataType.HISTORY,
    endpoint=Endpoint.GREEKS_EOD,
    file_granularity=FileGranularity.MONTHLY,
    force_refresh=False
)

client.request_data(req)
