"""
Tutorial to load only new data (incremental loading) using dlt library
"""
import dlt
from dlt.sources.helpers import requests


@dlt.resource(table_name="issues", write_disposition="append")
def get_issues(
        created_at=dlt.sources.incremental("created_at", initial_value="2000-02-12T00:00:00Z")
):
    """
    Generator that fetches issues from GitHub
    :param created_at:
    :return:
    """
    # Specify the URL of the GitHub API
    url = (
        "https://api.github.com/repos/dlt-hub/dlt/issues"
        "?per_page=5&sort=created&directions=desc&state=open"
    )

    while True:
        # Make a request and check if it was successful
        response = requests.get(url)
        response.raise_for_status()
        # print(response.json())
        yield response.json()

        print(response.links)
        print(created_at)
        print(created_at.start_out_of_range)
        # Stop requesting pages if the last element was already
        # older than initial value
        # Note: incremental will skip those items anyway, we just
        # do not want to use the api limits
        if created_at.start_out_of_range:
            break

        # get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]
        print(response.links)


pipeline = dlt.pipeline(
    pipeline_name="github_issues_incremental",
    destination="duckdb",
    dataset_name="github_data_append",
)

load_info = pipeline.run(get_issues)
row_counts = pipeline.last_trace.last_normalize_info

print(row_counts)
print("------")
print(load_info)
