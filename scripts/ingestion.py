import pandas as pd

# Load  raw data
transactions = pd.read_csv("data/raw/transactions.csv")
products = pd.read_csv("data/raw/products.csv")
competitors = pd.read_csv("data/raw/competitors.csv")

# Basic checks
print(transactions.head())
print(products.head())
print(competitors.head())

# Save processed data no transformations applied yet
transactions.to_csv("data/processed/transactions.csv", index=False)
products.to_csv("data/processed/products.csv", index=False)
competitors.to_csv("data/processed/competitors.csv", index=False)

print("Data ingestion completed successfully.")