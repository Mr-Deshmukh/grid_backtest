import streamlit as st
import pandas as pd
import boto3
import io
import yaml
from datetime import datetime
import altair as alt

# AWS S3 Config from Streamlit secrets
aws_access_key_id = st.secrets["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["AWS_SECRET_ACCESS_KEY"]
region_name = st.secrets["region_name"]
bucket_name = st.secrets["bucket_name"]
combine_results_prefix = st.secrets["combine_results_prefix"]
yaml_config_key = st.secrets["yaml_config_key"]
# Hardcoded plot prefix for stored plots (PNG images)
plot_prefix = "combine_results_graph/"


# --- S3 Helper Functions ---
def get_s3_client():
    return boto3.client(
        's3',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def list_combined_csvs():
    client = get_s3_client()
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=combine_results_prefix)
    all_keys = []
    for page in pages:
        contents = page.get('Contents', [])
        all_keys.extend([
            item['Key'] for item in contents
            if item['Key'].endswith('.csv') and 'aggregated_results' in item['Key']
        ])
    return sorted(all_keys)


def read_csv_from_s3(key):
    client = get_s3_client()
    response = client.get_object(Bucket=bucket_name, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()))


def read_yaml_from_s3(key):
    client = get_s3_client()
    response = client.get_object(Bucket=bucket_name, Key=key)
    return yaml.safe_load(response['Body'].read().decode('utf-8'))


def list_plot_keys():
    client = get_s3_client()
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=plot_prefix)
    keys = []
    for page in pages:
        contents = page.get('Contents', [])
        keys.extend([item['Key'] for item in contents if item['Key'].endswith('.png')])
    return sorted(keys)


# --- Streamlit App ---
st.set_page_config(layout="wide")
st.title("📈 Market Making Results Dashboard")

# Load YAML config
yaml_config = read_yaml_from_s3(yaml_config_key)
config_stocks = yaml_config.get("stocks", {})
valid_symbols = set(config_stocks.keys())

# List pre-computed backtest result CSV files
result_keys = list_combined_csvs()
if not result_keys:
    st.error("No combined result CSV files found in S3 bucket.")
    st.stop()

# Read all daily result CSVs and concatenate them
daily_results = []
progress_bar = st.progress(0, text="Loading precomputed results...")

for i, key in enumerate(result_keys):
    df = read_csv_from_s3(key)
    # Extract date from filename: aggregated_results_YYYYMMDD.csv
    file_date = key.split('/')[-1].replace('aggregated_results_', '').replace('.csv', '')
    df['Date'] = file_date
    daily_results.append(df)
    progress_bar.progress((i + 1) / len(result_keys), text=f"Loaded: {key.split('/')[-1]}")

result_df = pd.concat(daily_results, ignore_index=True)

col1, col2 = st.columns(2)
with col1:
    st.subheader("📊 Backtest Results (All Days)")
    st.dataframe(result_df)

summary_df = result_df.groupby('Symbol').agg({
    'Net_PnL': 'sum',
    'Max_PnL': 'max',
    'Drawdown': 'max'
}).rename(columns={
    'Net_PnL': 'Total Net PnL',
    'Max_PnL': 'Max Net PnL',
    'Drawdown': 'Max Drawdown'
}).reset_index()

with col2:
    st.subheader("📋 Combined Summary")
    st.dataframe(summary_df)

col3, col4 = st.columns(2)
with col3:
    st.subheader("📈 Net PnL Over Time for Selected Symbol")
    symbol_options = summary_df['Symbol'].unique().tolist()
    selected_symbol = st.selectbox("Select Symbol", symbol_options)

    symbol_df = result_df[result_df['Symbol'] == selected_symbol].copy()
    symbol_df['Date'] = pd.to_datetime(symbol_df['Date'], format='%Y%m%d').dt.date
    symbol_df = symbol_df.sort_values('Date')

    line_chart = alt.Chart(symbol_df).mark_line(point=True).encode(
        x=alt.X('Date:T', title='Date'),
        y=alt.Y('Net_PnL:Q', title='Net PnL'),
        tooltip=['Date:T', 'Net_PnL']
    ).properties(
        width=800,
        height=400,
        title=f"Net PnL Over Time: {selected_symbol}"
    )

    st.altair_chart(line_chart, use_container_width=True)

with col4:
    st.markdown("### 📥 Download Results")
    csv_bytes = result_df.to_csv(index=False).encode('utf-8')
    st.download_button("Download Full Results CSV", data=csv_bytes,
                       file_name=f"all_results_{datetime.now().strftime('%Y%m%d')}.csv")

# --- Display Stored Plot Images ---
st.markdown("## 📷 View Stored Plots")

plot_keys = list_plot_keys()
if not plot_keys:
    st.write("No plot images found in S3 bucket.")
else:
    # Parse each plot filename (assumed format: <stock>_<date>_plot.png)
    data = []
    for key in plot_keys:
        basename = key.split('/')[-1]  # e.g., "BEL_20250407_plot.png"
        parts = basename.split('_')
        if len(parts) >= 3:
            symbol = parts[0]
            date_str = parts[1]
            data.append({'key': key, 'symbol': symbol, 'date': date_str})
    plot_df = pd.DataFrame(data)

    # Create dropdown selectors for stock and date
    stock_options = sorted(plot_df['symbol'].unique())
    date_options = sorted(plot_df['date'].unique())
    selected_stock_plot = st.selectbox("Select Stock for Plot", stock_options)
    selected_date_plot = st.selectbox("Select Date for Plot", date_options)

    # Filter for the selected plot
    filtered_plot = plot_df[(plot_df['symbol'] == selected_stock_plot) &
                            (plot_df['date'] == selected_date_plot)]
    if not filtered_plot.empty:
        selected_key = filtered_plot.iloc[0]['key']
        client = get_s3_client()
        response = client.get_object(Bucket=bucket_name, Key=selected_key)
        img_bytes = response['Body'].read()
        st.image(img_bytes, caption=f"{selected_stock_plot} Plot for {selected_date_plot}")
    else:
        st.write("No plot found for selected stock and date.")
