"""
Tutorial to deep dive into the dlt library

Fetch the issues from the dlt library on GitHub and load them into duckdb
"""
import dlt
from dlt.sources.helpers import requests

# Specify the URL of the GitHub API
URL = "https://api.github.com/repos/dlt-hub/dlt/issues"
# Make a request and check if it was successful
response = requests.get(URL)
response.raise_for_status()

pipeline = dlt.pipeline(
    pipeline_name="fetch_github_issues",
    destination="duckdb",
    dataset_name="github_issues",
)

load_task = pipeline.run(
    response.json(),
    table_name="issues",
    # write_disposition="replace"
)
print(load_task)
