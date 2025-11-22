import pandas as pd
import numpy as np
import duckdb

def apply_feature_pattern(csv_path: str, output_path: str):
    df = pd.read_csv(csv_path)

    # Feature transformations
    df['amount_log'] = np.log1p(df['Amount'])
    df['hour'] = (df['Time'] % (24 * 3600)) // 3600

    # Feature normalization (zero mean, unit variance)
    df['amount_z'] = (df['amount_log'] - df['amount_log'].mean()) / df['amount_log'].std()

    # Register and query in DuckDB
    con = duckdb.connect()
    con.register('df', df)
    stats = con.execute("""
      SELECT AVG(amount_log) AS avg_amount, MIN(hour) AS min_hr, MAX(hour) AS max_hr FROM df                  
""").df()
    print(stats)

    df[['amount_z','hour', 'Class']].to_parquet(output_path, index=False)
    con.close()

if __name__ == "__main__":
    apply_feature_pattern("data/raw/creditcard.csv", "data/processed/features.parquet")