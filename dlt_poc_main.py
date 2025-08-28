from contextlib import redirect_stderr
import dlt

from sources.personio import get_personio_source
from utils.logger_notifier import LoggerNotifier


def ingestion_pipeline():
    logger_notifier = LoggerNotifier(dlt.secrets["slack"]["hook"])

    pipeline = dlt.pipeline(
        pipeline_name="load_personio_snowflake",
        dataset_name="dlt_personio_raw",
        destination="snowflake",
    )

    personio_source = get_personio_source()

    logger_notifier.notify_pipeline_start(pipeline, [personio_source])

    try:
        load_info = pipeline.run(personio_source)
        logger_notifier.handle_pipeline_results(load_info)
    except Exception as e:
        logger_notifier.handle_pipeline_results(exception=e)


if __name__ == "__main__":
    ingestion_pipeline()
