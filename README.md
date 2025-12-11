# Web Scraping → Kafka Mini-Pipeline

## Project Overview
This project implements a data pipeline that scrapes cryptocurrency market data and streams it to Apache Kafka.

## Data Source
**URL**: https://api.coingecko.com/api/v3/coins/markets

**Description**: CoinGecko API provides real-time cryptocurrency market data including prices, market capitalization, trading volumes, and 24-hour price changes for the top cryptocurrencies by market cap.

## Data Fields
The scraped dataset contains the following fields:
- `id` - Cryptocurrency identifier (e.g., bitcoin, ethereum)
- `symbol` - Trading symbol (e.g., BTC, ETH)
- `name` - Full name of cryptocurrency
- `price_usd` - Current price in USD
- `market_cap` - Total market capitalization
- `market_cap_rank` - Ranking by market cap
- `total_volume` - 24-hour trading volume
- `price_change_24h` - Absolute price change (24h)
- `change_24h_percent` - Percentage price change (24h)
- `circulating_supply` - Circulating supply of coins
- `last_updated` - Last update timestamp from API
- `scraped_at` - Timestamp when data was processed

## Data Cleaning Steps

The script performs **8 cleaning operations**:

1. **Rename columns** - Changed column names to more readable format (`current_price` → `price_usd`, `price_change_percentage_24h` → `change_24h_percent`)

2. **Uppercase conversion** - Converted all cryptocurrency symbols to uppercase for consistency (e.g., "btc" → "BTC")

3. **Remove invalid rows** - Filtered out rows with zero or negative prices to ensure data quality

4. **Round numeric values** - Rounded price and percentage columns to 2 decimal places for cleaner display

5. **Add timestamp** - Added `scraped_at` field with current timestamp to track when data was collected

6. **Format large numbers** - Converted market cap and volume to integers for consistent formatting

7. **Remove duplicates** - Removed duplicate entries based on symbol to avoid redundant data

8. **Sort data** - Sorted dataset by market cap rank to maintain logical ordering

## Installation & Setup

### Prerequisites
```bash
# Install required Python packages
pip install requests beautifulsoup4 pandas kafka-python

# Start Kafka (requires Kafka installation)
# Start Zookeeper first
brew services start zookeeper

# Start Kafka server
brew services start kafka


## Running the Script

```(venv) nursultantolegen@MacBook-Air-Nursultan-3 bonustask % python script.py
```

The script will:
1. Scrape cryptocurrency data from CoinGecko API
2. Clean and transform the data
3. Send each row as JSON message to Kafka topic `bonus_<22B030453>`
4. Save cleaned data to `cleaned_data.csv` and `cleaned_data.json`

## Kafka Topic
**Topic name format**: `bonus_22B030453>`


## Sample Kafka Message

```json
{
  "id": "bitcoin",
  "symbol": "BTC",
  "name": "Bitcoin",
  "price_usd": 43521.45,
  "market_cap": 851234567890,
  "market_cap_rank": 1,
  "total_volume": 28456789012,
  "price_change_24h": 1234.56,
  "change_24h_percent": 2.92,
  "circulating_supply": 19567890.0,
  "last_updated": "2024-12-13T10:30:45.123Z",
  "scraped_at": "2024-12-13 11:35:22"
}
```

## Output Files

1. **cleaned_data.csv** - Cleaned dataset in CSV format
2. **cleaned_data.json** - Cleaned dataset in JSON format

## Verifying Kafka Messages

To verify messages are in Kafka topic:


# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages from topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic bonus_22B030453 --from-beginning
```

## Project Structure
```
.
├── script.py              # Main pipeline script
├── cleaned_data.csv       # Output: cleaned dataset (CSV)
├── cleaned_data.json      # Output: cleaned dataset (JSON)
└── README.md             # This file
```

## Technical Notes

- **No Selenium**: Uses `requests` library for HTTP requests and API calls
- **Static scraping**: Data comes from public API endpoint, no dynamic rendering needed
- **JSON serialization**: All Kafka messages are sent as JSON format
- **Error handling**: Includes try-catch blocks for network and Kafka errors
- **Minimum rows**: Script extracts 30 rows (exceeds 20 row requirement)
