import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
import time

# 1. Page Config
st.set_page_config(page_title="Real-Time Crypto Tracker", layout="wide")
st.title("🚀 Live Crypto Price Feed (Kafka Stream)")

# 2. Setup Kafka Consumer
# We use a unique group_id so this dashboard doesn't steal messages from Spark
consumer = KafkaConsumer(
    'crypto-stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 3. Initialize Data Storage in Session State
if 'btc_data' not in st.session_state:
    st.session_state['btc_data'] = []
if 'eth_data' not in st.session_state:
    st.session_state['eth_data'] = []

# 4. Create Placeholders for the Chart and Metrics
col1, col2 = st.columns(2)
with col1:
    btc_metric = st.empty()
with col2:
    eth_metric = st.empty()

chart_holder = st.empty()

# 5. The Loop (Consuming Data)
st.write("Listening for events...")
for message in consumer:
    event = message.value
    current_time = pd.to_datetime(event['timestamp'], unit='s')
    
    # Update State
    if event['currency'] == 'bitcoin':
        st.session_state['btc_data'].append({'time': current_time, 'price': event['price']})
        btc_metric.metric(label="Bitcoin (BTC)", value=f"${event['price']:,.2f}")
        
    elif event['currency'] == 'ethereum':
        st.session_state['eth_data'].append({'time': current_time, 'price': event['price']})
        eth_metric.metric(label="Ethereum (ETH)", value=f"${event['price']:,.2f}")

    # Convert to DataFrame for plotting
    df_btc = pd.DataFrame(st.session_state['btc_data'])
    
    # Combine for the Chart
    if not df_btc.empty:
        # Keep only the last 50 points to keep the chart clean
        df_chart = df_btc.tail(50).set_index('time')
        chart_holder.line_chart(df_chart['price'])