import json
import time
import requests
from kafka import KafkaProducer

# 1. Connect to Kafka (Running on your laptop)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Crypto Producer Started... Press Ctrl+C to stop.")

while True:
    try:
        # 2. Fetch Real Data from CoinGecko
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
        response = requests.get(url)
        data = response.json()
        
        # 3. Format the data for Kafka
        # We create TWO messages: one for BTC, one for ETH
        btc_event = {
            "currency": "bitcoin", 
            "price": data['bitcoin']['usd'], 
            "timestamp": time.time()
        }
        eth_event = {
            "currency": "ethereum", 
            "price": data['ethereum']['usd'], 
            "timestamp": time.time()
        }
        
        # 4. Send to Kafka Topic 'crypto-stream'
        producer.send('crypto-stream', value=btc_event)
        producer.send('crypto-stream', value=eth_event)
        
        print(f"Sent: BTC=${btc_event['price']} | ETH=${eth_event['price']}")
        
        # 5. Sleep (Respect the API rate limit!)
        time.sleep(30)
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)