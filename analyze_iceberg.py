import pyarrow.parquet as pq
import pandas as pd
from pyiceberg.schema import Schema
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table
from pyiceberg.catalog import Catalog, LocalCatalog

# Read local Parquet file
file_path = "path/to/your/file.parquet"
parquet_table = pq.read_table(file_path)
df = parquet_table.to_pandas()

# Define the schema for the Iceberg table (based on your Parquet file schema)
schema = Schema.from_pandas(df)

# Create a local Iceberg catalog
catalog = LocalCatalog("/path/to/your/catalog/directory")

# Create an Iceberg table
table_name = "example_table"
table = catalog.create_table(table_name, schema)

# Write data to Iceberg table
file_io = PyArrowFileIO()
table.new_append().append_file(file_path, file_io).commit()

# You can now use the Iceberg table for further operations
