import polars as pl
import numpy as np
from datetime import datetime, timedelta

# Generate 500,000 rows of data
n_rows = 500000
start_date = datetime(2024, 1, 1)

data = {
    "S.No.": np.arange(1, n_rows + 1),
    "Product_ID": [f"PROD_{i:06d}" for i in range(n_rows)],
    "Category": np.random.choice(["Electronics", "Clothing", "Home", "Books", "Toys"], n_rows),
    "Price": np.random.uniform(10.0, 1000.0, n_rows).round(2),
    "Quantity": np.random.randint(1, 100, n_rows),
    "Date": [(start_date + timedelta(days=int(i % 365))).strftime("%Y-%m-%d") for i in range(n_rows)],
    "In_Stock": np.random.choice([True, False], n_rows)
}

df = pl.DataFrame(data)

# Calculate Total (numeric)
df = df.with_columns((pl.col("Price") * pl.col("Quantity")).alias("Total_Value"))

# Save as XLSX
df.write_excel("large_sample_test.xlsx")
print(f"Generated large_sample_test.xlsx with {n_rows} rows.")
