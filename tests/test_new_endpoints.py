"""Test script to verify the 3 new endpoints work correctly."""

from theta_client.requests import OptionRequest, Endpoint, DataType, Interval
from theta_client.job import Schema


def test_trade_endpoint():
    """Test the TRADE endpoint."""
    req = OptionRequest(
        symbol="SPY",
        start_date=20240101,
        end_date=20240101,
        data_type=DataType.HISTORY,
        endpoint=Endpoint.TRADE,
        interval=Interval.S1,
    )

    # Verify schema mapping
    assert req.get_schema() == Schema.OPTION_TRADE

    # Verify URL construction
    base_url = req._build_base_url()
    assert base_url == "http://0.0.0.0:25503/v3/option/history/trade"

    # Verify URL parameters
    urls = req._create_urls_per_day(["20240101"])
    assert len(urls) == 1
    url = urls[0]

    # Check that URL contains the correct parameters
    assert "symbol=SPY" in url
    assert "interval=1s" in url  # interval should be included
    assert "date=20240101" in url  # should use date, not start_date/end_date
    assert "expiration=*" in url  # wildcard should NOT be encoded
    assert "strike=*" in url  # wildcard should NOT be encoded
    assert "%2A" not in url  # verify * is not encoded

    print(f"✓ TRADE endpoint URL: {url}")


def test_trade_quote_endpoint():
    """Test the TRADE_QUOTE endpoint."""
    req = OptionRequest(
        symbol="SPY",
        start_date=20240101,
        end_date=20240101,
        data_type=DataType.HISTORY,
        endpoint=Endpoint.TRADE_QUOTE,
        interval=Interval.TICK,
    )

    # Verify schema mapping
    assert req.get_schema() == Schema.OPTION_TRADE_QUOTE

    # Verify URL construction
    base_url = req._build_base_url()
    assert base_url == "http://0.0.0.0:25503/v3/option/history/trade_quote"

    # Verify URL parameters
    urls = req._create_urls_per_day(["20240101"])
    assert len(urls) == 1
    url = urls[0]

    # Check that URL contains the correct parameters
    assert "symbol=SPY" in url
    assert "interval=tick" in url  # interval should be included
    assert "date=20240101" in url  # should use date, not start_date/end_date
    assert "expiration=*" in url
    assert "strike=*" in url
    assert "%2A" not in url

    print(f"✓ TRADE_QUOTE endpoint URL: {url}")


def test_greeks_first_order_endpoint():
    """Test the GREEKS_FIRST_ORDER endpoint."""
    req = OptionRequest(
        symbol="SPY",
        start_date=20240101,
        end_date=20240101,
        data_type=DataType.HISTORY,
        endpoint=Endpoint.GREEKS_FIRST_ORDER,
        interval=Interval.M5,
    )

    # Verify schema mapping
    assert req.get_schema() == Schema.GREEK_FIRST_ORDER

    # Verify URL construction
    base_url = req._build_base_url()
    assert base_url == "http://0.0.0.0:25503/v3/option/history/greeks/first_order"

    # Verify URL parameters
    urls = req._create_urls_per_day(["20240101"])
    assert len(urls) == 1
    url = urls[0]

    # Check that URL contains the correct parameters
    assert "symbol=SPY" in url
    assert "interval=5m" in url  # interval should be included
    assert "date=20240101" in url  # should use date, not start_date/end_date
    assert "expiration=*" in url
    assert "strike=*" in url
    assert "%2A" not in url

    print(f"✓ GREEKS_FIRST_ORDER endpoint URL: {url}")


def test_existing_greeks_eod_still_works():
    """Verify that the existing GREEKS_EOD endpoint still works correctly."""
    req = OptionRequest(
        symbol="SPY",
        start_date=20240101,
        end_date=20240101,
        data_type=DataType.HISTORY,
        endpoint=Endpoint.GREEKS_EOD,
    )

    # Verify schema mapping
    assert req.get_schema() == Schema.GREEK_EOD

    # Verify URL construction
    base_url = req._build_base_url()
    assert base_url == "http://0.0.0.0:25503/v3/option/history/greeks/eod"

    # Verify URL parameters - EOD endpoints should NOT have interval
    urls = req._create_urls_per_day(["20240101"])
    assert len(urls) == 1
    url = urls[0]

    # Check parameters
    assert "symbol=SPY" in url
    assert "interval" not in url  # EOD should NOT have interval
    assert "start_date=20240101" in url  # EOD uses start_date
    assert "end_date=20240101" in url  # EOD uses end_date
    assert "expiration=*" in url
    assert "strike=*" in url

    print(f"✓ GREEKS_EOD (existing) endpoint URL: {url}")


def test_schema_enum_values():
    """Verify all Schema enum values exist."""
    # Check that all 8 schema values exist
    assert hasattr(Schema, "STOCK_QUOTE")
    assert hasattr(Schema, "STOCK_EOD")
    assert hasattr(Schema, "OPTION_QUOTE")
    assert hasattr(Schema, "OPTION_EOD")
    assert hasattr(Schema, "OPTION_TRADE")
    assert hasattr(Schema, "OPTION_TRADE_QUOTE")
    assert hasattr(Schema, "GREEK_FIRST_ORDER")
    assert hasattr(Schema, "GREEK_EOD")

    # Verify string values
    assert Schema.OPTION_TRADE.value == "option_trade"
    assert Schema.OPTION_TRADE_QUOTE.value == "option_trade_quote"
    assert Schema.GREEK_FIRST_ORDER.value == "greek_first_order"

    print("✓ All Schema enum values exist and have correct string values")


def test_endpoint_enum_values():
    """Verify all Endpoint enum values exist."""
    # Check that all 6 endpoint values exist
    assert hasattr(Endpoint, "EOD")
    assert hasattr(Endpoint, "QUOTE")
    assert hasattr(Endpoint, "TRADE")
    assert hasattr(Endpoint, "TRADE_QUOTE")
    assert hasattr(Endpoint, "GREEKS_FIRST_ORDER")
    assert hasattr(Endpoint, "GREEKS_EOD")

    # Verify string values
    assert Endpoint.TRADE.value == "trade"
    assert Endpoint.TRADE_QUOTE.value == "trade_quote"
    assert Endpoint.GREEKS_FIRST_ORDER.value == "greeks/first_order"

    print("✓ All Endpoint enum values exist and have correct string values")


def test_schemas_dictionary():
    """Verify the SCHEMAS dictionary has all 8 schemas."""
    from theta_client.schemas import SCHEMAS

    # Check that all 8 schemas are in the dictionary
    assert "stock_quote" in SCHEMAS
    assert "stock_eod" in SCHEMAS
    assert "option_quote" in SCHEMAS
    assert "option_eod" in SCHEMAS
    assert "option_trade" in SCHEMAS
    assert "option_trade_quote" in SCHEMAS
    assert "greek_first_order" in SCHEMAS
    assert "greek_eod" in SCHEMAS

    assert len(SCHEMAS) == 8

    print("✓ SCHEMAS dictionary has all 8 schemas")


if __name__ == "__main__":
    print("\nTesting new endpoints implementation...\n")

    # Test enum values
    test_schema_enum_values()
    test_endpoint_enum_values()
    test_schemas_dictionary()

    # Test URL generation for new endpoints
    test_trade_endpoint()
    test_trade_quote_endpoint()
    test_greeks_first_order_endpoint()

    # Verify existing endpoint still works
    test_existing_greeks_eod_still_works()

    print("\n✅ All tests passed! The 3 new endpoints are ready to use.")
