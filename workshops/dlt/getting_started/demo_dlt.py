"""
Demo to check how to use the DLT library
"""
import dlt

data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}, {"name": "Tom", "age": 35}]

pipeline = dlt.pipeline(
    pipeline_name="demo_pipeline",
    destination="duckdb",
    dataset_name="demo_dataset",
)

load_task = pipeline.run(data, table_name="users")

print(load_task)
