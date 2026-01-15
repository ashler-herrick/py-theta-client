"""PyArrow schemas for Theta Data API responses.

These schemas match the actual CSV responses from the Theta Data API v3 endpoints.
See https://docs.thetadata.us/ for complete API documentation.
"""

import pyarrow as pa


# Stock Quote Schema
# Endpoint: /v3/stock/history/quote
stock_quote = [
    pa.field("timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
]


# Stock EOD Schema
# Endpoint: /v3/stock/history/eod
stock_eod = [
    pa.field("created", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("last_trade", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("count", pa.int64()),
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
]


# Option Quote Schema
# Endpoint: /v3/option/history/quote
option_quote = [
    pa.field("symbol", pa.string()),  # Contract symbol
    pa.field("expiration", pa.date32()),  # Date type for YYYY-MM-DD
    pa.field("strike", pa.float64()),  # Strike price in dollars
    pa.field("right", pa.string()),  # "call" or "put"
    pa.field("timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
]


# Option EOD Schema
# Endpoint: /v3/option/history/eod
option_eod = [
    pa.field("symbol", pa.string()),  # Contract symbol
    pa.field("expiration", pa.date32()),  # Date type for YYYY-MM-DD
    pa.field("strike", pa.float64()),  # Strike price in dollars
    pa.field("right", pa.string()),  # "call" or "put"
    pa.field("created", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("last_trade", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("count", pa.int64()),
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
]


# Option Trade Schema
# Endpoint: /v3/option/history/trade
option_trade = [
    pa.field("symbol", pa.string()),  # Contract symbol
    pa.field("expiration", pa.date32()),  # Date type for YYYY-MM-DD
    pa.field("strike", pa.float64()),  # Strike price in dollars
    pa.field("right", pa.string()),  # "call" or "put"
    pa.field("timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("sequence", pa.int64()),  # Exchange sequence number
    pa.field("ext_condition1", pa.int16()),  # Extended trade condition
    pa.field("ext_condition2", pa.int16()),  # Extended trade condition
    pa.field("ext_condition3", pa.int16()),  # Extended trade condition
    pa.field("ext_condition4", pa.int16()),  # Extended trade condition
    pa.field("condition", pa.int16()),  # Primary trade condition
    pa.field("size", pa.int32()),  # Contracts traded
    pa.field("exchange", pa.int16()),  # Executing exchange
    pa.field("price", pa.float64()),  # Trade price
]


# Option Trade Quote Schema
# Endpoint: /v3/option/history/trade_quote
option_trade_quote = [
    pa.field("symbol", pa.string()),  # Contract symbol
    pa.field("expiration", pa.date32()),  # Date type for YYYY-MM-DD
    pa.field("strike", pa.float64()),  # Strike price in dollars
    pa.field("right", pa.string()),  # "call" or "put"
    pa.field("trade_timestamp", pa.timestamp("ms")),  # Trade timestamp
    pa.field("quote_timestamp", pa.timestamp("ms")),  # Quote timestamp
    pa.field("sequence", pa.int64()),  # Exchange sequence number
    pa.field("ext_condition1", pa.int16()),  # Extended trade condition
    pa.field("ext_condition2", pa.int16()),  # Extended trade condition
    pa.field("ext_condition3", pa.int16()),  # Extended trade condition
    pa.field("ext_condition4", pa.int16()),  # Extended trade condition
    pa.field("condition", pa.int16()),  # Primary trade condition
    pa.field("size", pa.int32()),  # Contracts traded
    pa.field("exchange", pa.int16()),  # Executing exchange
    pa.field("price", pa.float64()),  # Trade price
    pa.field("bid_size", pa.int32()),  # Quote bid size
    pa.field("bid_exchange", pa.int16()),  # Quote bid exchange
    pa.field("bid", pa.float64()),  # Quote bid price
    pa.field("bid_condition", pa.int16()),  # Quote bid condition
    pa.field("ask_size", pa.int32()),  # Quote ask size
    pa.field("ask_exchange", pa.int16()),  # Quote ask exchange
    pa.field("ask", pa.float64()),  # Quote ask price
    pa.field("ask_condition", pa.int16()),  # Quote ask condition
]


# Greek First Order Schema
# Endpoint: /v3/option/history/greeks/first_order
greek_first_order = [
    pa.field("symbol", pa.string()),  # Contract symbol
    pa.field("expiration", pa.date32()),  # Date type for YYYY-MM-DD
    pa.field("strike", pa.float64()),  # Strike price in dollars
    pa.field("right", pa.string()),  # "call" or "put"
    pa.field("timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("bid", pa.float64()),  # Bid price
    pa.field("ask", pa.float64()),  # Ask price
    # First-order Greeks only
    pa.field("delta", pa.float64()),
    pa.field("theta", pa.float64()),
    pa.field("vega", pa.float64()),
    pa.field("rho", pa.float64()),
    pa.field("epsilon", pa.float64()),
    pa.field("lambda", pa.float64()),
    # Implied volatility
    pa.field("implied_vol", pa.float64()),
    pa.field("iv_error", pa.float64()),  # Quote-to-IV variance
    # Underlying asset data
    pa.field("underlying_timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("underlying_price", pa.float64()),  # Underlying midpoint at trade time
]


# Greek EOD Schema
# Endpoint: /v3/option/history/greeks/eod
greek_eod = [
    # Contract identification
    pa.field("symbol", pa.string()),  # Contract symbol
    pa.field("expiration", pa.date32()),  # Date type for YYYY-MM-DD
    pa.field("strike", pa.float64()),  # Strike price in dollars
    pa.field("right", pa.string()),  # "call" or "put"
    # Timestamp
    pa.field("timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    # OHLCV data
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("count", pa.int64()),
    # Quote data
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
    # First-order Greeks
    pa.field("delta", pa.float64()),
    pa.field("gamma", pa.float64()),
    pa.field("vega", pa.float64()),
    pa.field("theta", pa.float64()),
    pa.field("rho", pa.float64()),
    pa.field("epsilon", pa.float64()),
    pa.field("lambda", pa.float64()),
    # Second-order Greeks
    pa.field("vanna", pa.float64()),
    pa.field("charm", pa.float64()),
    pa.field("vomma", pa.float64()),
    pa.field("veta", pa.float64()),
    pa.field("vera", pa.float64()),
    # Third-order Greeks
    pa.field("speed", pa.float64()),
    pa.field("zomma", pa.float64()),
    pa.field("color", pa.float64()),
    pa.field("ultima", pa.float64()),
    # Black-Scholes intermediate values
    pa.field("d1", pa.float64()),
    pa.field("d2", pa.float64()),
    pa.field("dual_delta", pa.string()),  # Delta of underlying
    pa.field("dual_gamma", pa.float64()),  # Gamma of underlying
    # Implied volatility
    pa.field("implied_vol", pa.float64()),
    pa.field("iv_error", pa.float64()),  # Quote-to-IV variance
    # Underlying asset data
    pa.field("underlying_timestamp", pa.timestamp("ms")),  # Millisecond precision timestamp
    pa.field("underlying_price", pa.float64()),  # Underlying midpoint at trade time
]


# Schema dictionary mapping schema names to PyArrow schemas
SCHEMAS = {
    "stock_quote": pa.schema(stock_quote),
    "stock_eod": pa.schema(stock_eod),
    "option_quote": pa.schema(option_quote),
    "option_eod": pa.schema(option_eod),
    "option_trade": pa.schema(option_trade),
    "option_trade_quote": pa.schema(option_trade_quote),
    "greek_first_order": pa.schema(greek_first_order),
    "greek_eod": pa.schema(greek_eod),
}
