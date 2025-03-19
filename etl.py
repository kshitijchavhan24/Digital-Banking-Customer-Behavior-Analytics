import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient

engine = create_engine("sqlite:///digital_banking.db")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS Transactions (
            TransactionID INTEGER PRIMARY KEY,
            CustomerID INTEGER,
            TransactionDate TEXT,
            TransactionAmount REAL,
            DigitalChannel INTEGER  -- 1 for digital, 0 for non-digital
        );
    """))
    
    # Check if the table is empty before inserting sample data
    result = conn.execute(text("SELECT COUNT(*) FROM Transactions"))
    count = result.fetchone()[0]
    if count == 0:
        conn.execute(text("""
            INSERT INTO Transactions (CustomerID, TransactionDate, TransactionAmount, DigitalChannel)
            VALUES 
                (1, '2025-01-05', 100.0, 1),
                (2, '2025-01-06', 200.0, 0),
                (3, '2025-01-07', 150.0, 1);
        """))

# Fetch data from the Transactions table
try:
    query = "SELECT * FROM Transactions WHERE TransactionDate >= '2025-01-01'"
    df_sql = pd.read_sql(query, engine)
    print("SQL Data Extracted:")
    print(df_sql.head())
except Exception as e:
    print("Error during SQL extraction:", e)

# MongoDB Data Extraction
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "customer_logs"
COLLECTION_NAME = "events"

try:
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Insert sample log data if the collection is empty
    if collection.count_documents({}) == 0:
        sample_logs = [
            {"customer_id": 1, "event_date": "2025-01-05", "event_type": "login", "session_duration": 300},
            {"customer_id": 2, "event_date": "2025-01-06", "event_type": "login", "session_duration": 250},
            {"customer_id": 3, "event_date": "2025-01-07", "event_type": "logout", "session_duration": 200}
        ]
        collection.insert_many(sample_logs)
        print("Inserted sample log data into MongoDB.")

    # Query the collection to fetch logs with event_date >= '2025-01-01'
    logs_cursor = collection.find({"event_date": {"$gte": "2025-01-01"}})
    df_logs = pd.DataFrame(list(logs_cursor))
    print("MongoDB Data Extracted:")
    print(df_logs.head())
except Exception as e:
    print("Error during MongoDB extraction:", e)

# Data Cleaning & Preprocessing
df_sql.rename(columns={
    "CustomerID": "customer_id",
    "TransactionDate": "transaction_date",
    "TransactionAmount": "transaction_amount",
    "DigitalChannel": "digital_channel"
}, inplace=True)

# Convert the transaction_date column to datetime
df_sql['transaction_date'] = pd.to_datetime(df_sql['transaction_date'])

# For MongoDB data, convert event_date to datetime if it exists
if 'event_date' in df_logs.columns:
    df_logs['event_date'] = pd.to_datetime(df_logs['event_date'])

print("\nAfter cleaning - SQL Data:")
print(df_sql.head())
print("\nAfter cleaning - MongoDB Data:")
print(df_logs.head())

# Merge the data on customer_id using a left join to keep all SQL transactions.
df_merged = pd.merge(df_sql, df_logs, on='customer_id', how='left')
print("\nMerged Data:")
print(df_merged.head())

# Check for missing values in the merged data
print("\nMissing Values in Merged Data:")
print(df_merged.isnull().sum())

# Example: Fill missing session_duration with 0 if applicable
if 'session_duration' in df_merged.columns:
    df_merged['session_duration'] = df_merged['session_duration'].fillna(0)

print("\nMerged Data after handling missing values:")
print(df_merged.head())


# Data Transformation & Aggregation

if 'digital_channel' in df_merged.columns:
    digital_adoption = df_merged['digital_channel'].mean() * 100  # percentage
else:
    digital_adoption = None

# Active Users: Count unique customer IDs
active_users = df_merged['customer_id'].nunique()

# Transaction Volume: Sum of transaction_amount column
transaction_volume = df_merged['transaction_amount'].sum()

# Average Session Duration: Average of session_duration column (if it exists)
if 'session_duration' in df_merged.columns:
    avg_session_duration = df_merged['session_duration'].mean()
else:
    avg_session_duration = None

# For demonstration
if 'event_type' in df_merged.columns:
    # Count rows where event_type is 'login'
    conversion_events = df_merged[df_merged['event_type'] == 'login'].shape[0]
    total_events = df_merged.shape[0]
    conversion_rate = (conversion_events / total_events) * 100 if total_events > 0 else 0
else:
    conversion_rate = None

# Display the calculated KPIs
print("\nCalculated KPIs:")
print(f"Digital Adoption: {digital_adoption:.2f}%")
print(f"Active Users: {active_users}")
print(f"Transaction Volume: ${transaction_volume:.2f}")
if avg_session_duration is not None:
    print(f"Average Session Duration: {avg_session_duration:.2f} seconds")
else:
    print("Average Session Duration: Not calculated")
if conversion_rate is not None:
    print(f"Conversion Rate: {conversion_rate:.2f}%")
else:
    print("Conversion Rate: Not calculated (no event_type data available)")

# Export KPIs to Excel (Optional)

# Prepare a DataFrame for KPIs
df_kpis = pd.DataFrame({
    "Digital Adoption (%)": [digital_adoption],
    "Active Users": [active_users],
    "Transaction Volume": [transaction_volume],
    "Avg Session Duration (sec)": [avg_session_duration],
    "Conversion Rate (%)": [conversion_rate]
})

# Export to Excel
df_kpis.to_excel("aggregated_kpis.xlsx", index=False)
print("\nKPIs exported to 'aggregated_kpis.xlsx'.")
