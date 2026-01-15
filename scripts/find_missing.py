#!/usr/bin/env python3
"""
Analyze log file for missing options data and generate summary report.

Extracts 'WARNING | No data response from' lines and reports which
symbol/year/month combinations have missing data.

Examples:
  python analyze_missing_data.py app.log
  python analyze_missing_data.py app.log --summary
  python analyze_missing_data.py app.log --by-ticker
  python analyze_missing_data.py app.log --csv missing_data.csv
  python analyze_missing_data.py app.log --out missing_report.txt --csv missing_data.csv
"""

import sys
import re
import argparse
import csv
from pathlib import Path
from collections import defaultdict

MATCH = "WARNING | No data response from "


def iter_lines(source):
    if source == "-" or source.lower() == "stdin":
        yield from sys.stdin
    else:
        with Path(source).open("r", encoding="utf-8", errors="replace") as f:
            yield from f


def parse_theta_url(url):
    """
    Extract ticker, year, month from Theta API URL.
    
    Example URLs:
    http://0.0.0.0:25503/v3/option/history/greeks/eod?symbol=CCL&expiration=*&start_date=20190209
    https://api.thetadata.com/v2/hist/option/greeks_eod?root=AAPL&start_date=20250101
    
    Returns: (ticker, year, month) or None if parse fails
    """
    # Try both 'symbol=' and 'root=' patterns
    ticker_match = re.search(r'(?:symbol|root)=([A-Z]+)', url)
    # Extract start_date to determine year/month
    date_match = re.search(r'start_date=(\d{8})', url)
    
    if not ticker_match or not date_match:
        return None
    
    ticker = ticker_match.group(1)
    date_str = date_match.group(1)
    
    try:
        year = int(date_str[:4])
        month = int(date_str[4:6])
        return (ticker, year, month)
    except (ValueError, IndexError):
        return None


def extract_missing_data(lines):
    """
    Extract missing data entries from log lines.
    
    Returns: 
        - missing: dict[ticker][year][month] = count_of_missing_days
        - missing_entries: list of (ticker, year, month, url) tuples
        - unparseable: list of unparseable URLs
    """
    missing = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    missing_entries = []
    unparseable = []
    
    for line in lines:
        if MATCH in line:
            try:
                url = line.split(MATCH, 1)[1].rsplit(" (", 1)[0].strip()
                parsed = parse_theta_url(url)
                
                if parsed:
                    ticker, year, month = parsed
                    missing[ticker][year][month] += 1
                    missing_entries.append((ticker, year, month, url))
                else:
                    unparseable.append(url)
            except IndexError:
                pass
    
    return missing, missing_entries, unparseable


def write_csv(missing_entries, csv_path):
    """Write missing data entries to CSV file."""
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['symbol', 'year', 'month', 'url'])
        
        # Sort by symbol, year, month
        sorted_entries = sorted(missing_entries, key=lambda x: (x[0], x[1], x[2]))
        
        for ticker, year, month, url in sorted_entries:
            writer.writerow([ticker, year, month, url])


def format_summary_report(missing_data):
    """Generate a summary statistics report."""
    lines = []
    lines.append("=" * 60)
    lines.append("MISSING DATA SUMMARY REPORT")
    lines.append("=" * 60)
    lines.append("")
    
    total_tickers = len(missing_data)
    total_days = sum(
        count
        for ticker_data in missing_data.values()
        for year_data in ticker_data.values()
        for count in year_data.values()
    )
    
    lines.append(f"Total tickers with missing data: {total_tickers}")
    lines.append(f"Total missing days: {total_days}")
    lines.append("")
    
    # Tickers with most missing days
    ticker_counts = [
        (ticker, sum(
            count
            for year_data in years.values()
            for count in year_data.values()
        ))
        for ticker, years in missing_data.items()
    ]
    ticker_counts.sort(key=lambda x: x[1], reverse=True)
    
    lines.append("Top 20 tickers by missing days:")
    lines.append("-" * 40)
    for ticker, count in ticker_counts[:20]:
        lines.append(f"  {ticker:6s} : {count:4d} missing days")
    
    return "\n".join(lines)


def format_by_ticker_report(missing_data):
    """Generate detailed report grouped by ticker."""
    lines = []
    lines.append("=" * 70)
    lines.append("MISSING DATA BY TICKER")
    lines.append("=" * 70)
    lines.append("")
    
    for ticker in sorted(missing_data.keys()):
        years_data = missing_data[ticker]
        total_days = sum(
            count
            for year_data in years_data.values()
            for count in year_data.values()
        )
        
        lines.append(f"{ticker} ({total_days} missing days)")
        lines.append("-" * 50)
        
        for year in sorted(years_data.keys()):
            months_data = years_data[year]
            for month in sorted(months_data.keys()):
                count = months_data[month]
                lines.append(f"  {year}-{month:02d}: {count:3d} missing days")
        
        lines.append("")
    
    return "\n".join(lines)


def format_detailed_report(missing_data):
    """Generate detailed table of all missing entries."""
    lines = []
    lines.append("=" * 70)
    lines.append("DETAILED MISSING DATA REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"{'Ticker':<10} {'Year':<8} {'Month':<8} {'Missing Days':<15}")
    lines.append("-" * 50)
    
    # Collect all entries
    entries = []
    for ticker in missing_data:
        for year in missing_data[ticker]:
            for month in missing_data[ticker][year]:
                count = missing_data[ticker][year][month]
                entries.append((ticker, year, month, count))
    
    # Sort by ticker, then year, then month
    entries.sort()
    
    total_days = 0
    for ticker, year, month, count in entries:
        lines.append(f"{ticker:<10} {year:<8} {month:02d}       {count:<15}")
        total_days += count
    
    lines.append("")
    lines.append(f"Total entries: {len(entries)}")
    lines.append(f"Total missing days: {total_days}")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze missing options data from log files"
    )
    parser.add_argument("source", help="Log file path or '-' for stdin")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show summary statistics only"
    )
    parser.add_argument(
        "--by-ticker",
        action="store_true",
        help="Group by ticker (default: detailed table)"
    )
    parser.add_argument(
        "--out",
        help="Write text report to file instead of stdout"
    )
    parser.add_argument(
        "--csv",
        help="Write CSV file with symbol, year, month, url columns"
    )
    args = parser.parse_args()
    
    missing_data, missing_entries, unparseable = extract_missing_data(
        iter_lines(args.source)
    )
    
    # Write CSV if requested
    if args.csv:
        write_csv(missing_entries, args.csv)
        print(f"CSV written to {args.csv} ({len(missing_entries)} entries)")
    
    # Generate appropriate report
    if args.summary:
        output = format_summary_report(missing_data)
    elif args.by_ticker:
        output = format_by_ticker_report(missing_data)
    else:
        output = format_detailed_report(missing_data)
    
    # Add unparseable URLs if any
    if unparseable:
        output += "\n\n" + "=" * 70 + "\n"
        output += f"WARNING: {len(unparseable)} URLs could not be parsed:\n"
        output += "-" * 70 + "\n"
        for url in unparseable[:10]:  # Show first 10
            output += f"  {url}\n"
        if len(unparseable) > 10:
            output += f"  ... and {len(unparseable) - 10} more\n"
    
    # Output text report
    if args.out:
        Path(args.out).write_text(output, encoding="utf-8")
        print(f"Report written to {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()