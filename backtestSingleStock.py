from HelperFunctions import *
from CoreFuntions import *
import os
from datetime import datetime

file_path = os.path.join(DATA_FOLDER, "RELIANCE_minute.csv")
df = load_stock_data(file_path, start_date="2021-01-01", end_date="2023-12-31")

# Expiries from 2021-01-01 to 2023-12-31
expiries = get_monthly_expiries(datetime(2021, 1, 1), datetime(2023, 12, 31), df)

# Generate signals
signals = generate_signals(df, expiries, "1D", strike_diff=10)

# Get expiry closes
expiry_closes = get_expiry_closes(df, expiries)

# Evaluate success/failure
results = evaluate_signals(signals, expiry_closes)
print(results.head())

summary = summarize_performance(results)
print(summary)