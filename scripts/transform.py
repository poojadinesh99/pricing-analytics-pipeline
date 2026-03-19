import duckdb as ddb

con = ddb.connect()

# Load CSVs into DuckDB tables
con.execute("CREATE TABLE transactions AS SELECT * FROM 'data/processed/transactions.csv'");
con.execute("CREATE TABLE products AS SELECT * FROM 'data/processed/products.csv'");
con.execute("CREATE TABLE competitors AS SELECT * FROM 'data/processed/competitors.csv'");

# Create analytics table (JOIN)
with open("sql/analytics.sql", "r") as f:
    query = f.read()

result = con.execute(query).df()

# Save output
result.to_csv("data/processed/analytics_output.csv", index=False)

print(result)