import hashlib
import pandas as pd
import duckdb

def hash_feature(value: str, num_buckets: int = 1000) -> int:
    """ Hash a string into a fixed integer bucket."""
    return int (hashlib.md5(str(value).encode()).hexdigest(), 16) % num_buckets

def apply_hash_pattern(csv_path: str, output_path: str):
    df = pd.read_csv(csv_path, encoding="ISO-8859-1")
    df = df.dropna(subset=['CustomerID'])

    df['customer_hash'] = df['CustomerID'].apply(lambda x: hash_feature(x, 10000))  

    # DuckDB for SQL like exploration
    con = duckdb.connect()
    con.register('df', df)
    result = con.execute("""
        SELECT customer_hash,COUNT(*) AS transactions
        FROM df
        GROUP BY customer_hash
        ORDER BY transactions DESC
        LIMIT 10
""").df()
    
    print(result)
    df.to_parquet(output_path, index=False)
    con.close()


if __name__ == "__main__":
    apply_hash_pattern("data/raw/data.csv", "data/processed/hash_features.parquet")