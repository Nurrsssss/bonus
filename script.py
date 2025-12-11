
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from kafka import KafkaProducer
from datetime import datetime
import time


STUDENT_ID = "22B030453"  
KAFKA_TOPIC = f"bonus_{STUDENT_ID}"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
URL = "https://www.coingecko.com/en/coins"  

def scrape_crypto_data():
    """
    Scrape cryptocurrency data from CoinGecko
    """
    print("Starting web scraping...")
    
    
    api_url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 50,
        'page': 1,
        'sparkline': False
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(api_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Extract relevant fields
        crypto_list = []
        for coin in data[:30]:  # Get at least 20 coins
            crypto_list.append({
                'id': coin.get('id', ''),
                'symbol': coin.get('symbol', ''),
                'name': coin.get('name', ''),
                'current_price': coin.get('current_price', 0),
                'market_cap': coin.get('market_cap', 0),
                'market_cap_rank': coin.get('market_cap_rank', 0),
                'total_volume': coin.get('total_volume', 0),
                'price_change_24h': coin.get('price_change_24h', 0),
                'price_change_percentage_24h': coin.get('price_change_percentage_24h', 0),
                'circulating_supply': coin.get('circulating_supply', 0),
                'last_updated': coin.get('last_updated', '')
            })
        
        df = pd.DataFrame(crypto_list)
        print(f"Successfully scraped {len(df)} rows of data")
        return df
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        return None

def clean_data(df):
    """
    Clean the scraped data with at least 5 operations
    """
    print("\nCleaning data...")
    
    # Cleaning operation 1: Rename columns to more readable names
    df = df.rename(columns={
        'current_price': 'price_usd',
        'price_change_percentage_24h': 'change_24h_percent'
    })
    print("✓ Renamed columns")
    
    # Cleaning operation 2: Convert symbol to uppercase
    df['symbol'] = df['symbol'].str.upper()
    print("✓ Converted symbols to uppercase")
    
    # Cleaning operation 3: Remove rows with missing or zero prices
    df = df[df['price_usd'] > 0]
    print("✓ Removed rows with invalid prices")
    
    # Cleaning operation 4: Round numeric columns to 2 decimal places
    numeric_cols = ['price_usd', 'change_24h_percent', 'price_change_24h']
    for col in numeric_cols:
        df[col] = df[col].round(2)
    print("✓ Rounded numeric values")
    
    # Cleaning operation 5: Add timestamp for when data was processed
    df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("✓ Added timestamp")
    
    # Cleaning operation 6: Format market cap and volume as integers
    df['market_cap'] = df['market_cap'].astype(int)
    df['total_volume'] = df['total_volume'].astype(int)
    print("✓ Formatted large numbers as integers")
    
    # Cleaning operation 7: Remove duplicate entries by symbol
    df = df.drop_duplicates(subset=['symbol'], keep='first')
    print("✓ Removed duplicates")
    
    # Cleaning operation 8: Sort by market cap rank
    df = df.sort_values('market_cap_rank').reset_index(drop=True)
    print("✓ Sorted by market cap rank")
    
    print(f"\nCleaned dataset contains {len(df)} rows")
    return df

def produce_to_kafka(df):
    """
    Send each row to Kafka as JSON message
    """
    print(f"\nConnecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        print(f"Producing messages to topic: {KAFKA_TOPIC}")
        
        success_count = 0
        for index, row in df.iterrows():
            message = row.to_dict()
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, value=message)
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            success_count += 1
            if success_count <= 3:  # Print first 3 messages
                print(f"✓ Sent message {success_count}: {message['symbol']} - ${message['price_usd']}")
        
        producer.flush()
        producer.close()
        
        print(f"\n✓ Successfully sent {success_count} messages to Kafka topic: {KAFKA_TOPIC}")
        return True
        
    except Exception as e:
        print(f"Error producing to Kafka: {e}")
        print("\nNote: Make sure Kafka is running on localhost:9092")
        print("Start Kafka with: bin/kafka-server-start.sh config/server.properties")
        return False

def save_dataset(df, format='csv'):
    """
    Save the cleaned dataset to file
    """
    if format == 'csv':
        filename = 'cleaned_data.csv'
        df.to_csv(filename, index=False)
    else:
        filename = 'cleaned_data.json'
        df.to_json(filename, orient='records', indent=2)
    
    print(f"\n✓ Saved cleaned dataset to {filename}")

def main():
    """
    Main pipeline execution
    """
    print("="*60)
    print("Web Scraping → Kafka Mini-Pipeline")
    print("="*60)
    
    # Step 1: Scrape data
    df = scrape_crypto_data()
    if df is None or len(df) == 0:
        print("Failed to scrape data. Exiting.")
        return
    
    print(f"\nInitial data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Step 2: Clean data
    df_cleaned = clean_data(df)
    

    
    # Step 4: Produce to Kafka
    print("\n" + "="*60)
    kafka_success = produce_to_kafka(df_cleaned)
    
    # Step 5: Save dataset
    save_dataset(df_cleaned, format='csv')
    save_dataset(df_cleaned, format='json')
    
    print("\n" + "="*60)
    print("Pipeline completed successfully!" if kafka_success else "Pipeline completed with warnings")
    print("="*60)
    print(f"\nTotal records processed: {len(df_cleaned)}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print("Output files: cleaned_data.csv, cleaned_data.json")

if __name__ == "__main__":
    main()