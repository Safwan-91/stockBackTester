from HelperFunctions import *
from CoreFuntions import *
import os
from datetime import datetime


def run_backtest_all(data_folder, start="2021-01-01", end="2023-12-31"):
    all_results = []

    for file in os.listdir(data_folder):
        if not file.endswith(".csv"):
            continue
        stock_name = file.replace("_minute.csv", "").upper()
        file_path = os.path.join(data_folder, file)

        print("backtest started for", stock_name)

        try:
            # Load stock data
            df = load_stock_data(file_path, start, end)
            if df.empty:
                continue

            expiries = get_monthly_expiries(datetime.fromisoformat(start),
                                            datetime.fromisoformat(end), df)

            # Expiry closes
            expiry_closes = get_expiry_closes(df, expiries)

            # Generate signals
            signals = generate_signals(df, expiries, "1D")

            if signals.empty:
                continue

            # Evaluate outcomes
            results = evaluate_signals(signals, expiry_closes)
            print(f"Total trades for {stock_name}: {len(results)}")
            summary = summarize_performance(results)
            print(summary)
            results["stock"] = stock_name
            all_results.append(results)

        except Exception as e:
            print(f"Error processing {file}: {e}")

    if not all_results:
        return pd.DataFrame()

    return pd.concat(all_results, ignore_index=True)


# ---------- Run across all Nifty 50 ----------
all_results = run_backtest_all(DATA_FOLDER)

print("Total trades across all stocks:", len(all_results))
print(all_results.head())

# Overall summary
overall_summary = summarize_performance(all_results)
print("Overall:", overall_summary)

# Per stock summary
per_stock = all_results.groupby("stock").apply(summarize_performance).to_dict()
print("\nPer Stock Summary (sample):")
for stock, summary in list(per_stock.items())[:5]:
    print(stock, ":", summary)
