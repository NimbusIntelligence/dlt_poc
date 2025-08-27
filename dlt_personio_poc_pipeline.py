import dlt

from sources.personio import get_personio_source


def ingestion_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="load_personio_snowflake",
        dataset_name="dlt_personio_raw",
        destination="snowflake",
    )

    personio_source = get_personio_source()

    pipeline.run(personio_source)


if __name__ == "__main__":
    ingestion_pipeline()
