from contextlib import redirect_stderr
import sys
import json
from datetime import datetime
from typing import Iterable
from dlt import Pipeline, pipeline
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.slack import send_slack_message
from dlt.extract import DltSource


class LoggerNotifier:

    def __init__(
        self, slack_hook: str | None = None, timestamp_fmt: str = "%Y-%m-%d_%H:%M:%S"
    ) -> None:
        self.slack_hook = slack_hook
        self.created_at = datetime.now()
        self.timestamp_fmt = timestamp_fmt
        self.redirect_stderr = redirect_stderr(
            open(self.get_filename("stderr", "log"), "w")
        )
        # sys.stderr = open(self.get_filename("stderr", "log"), "w")

    def get_filename(self, prefix: str, ext: str) -> str:
        return f"logs/{prefix}_{self.created_at.strftime(self.timestamp_fmt)}.{ext}"

    def notify_pipeline_start(self, pipeline: Pipeline, sources: Iterable[DltSource]):

        if self.slack_hook:
            send_slack_message(
                self.slack_hook,
                (
                    f"*Pipeline {pipeline.pipeline_name} started*\n\n"
                    f"*Sources*: {' - '.join(source.name for source in sources)}\n\n"
                    f"*Started at*: {self.created_at.strftime(self.timestamp_fmt)}"
                ),
            )
        else:
            raise ValueError("Slack hook has not been set.")

    def handle_pipeline_results(
        self, *pipeline_loads: LoadInfo, exception: Exception | None = None
    ):
        message_str = ""

        if not exception:
            message_str += (
                f"*INGESTION INFORMATION ({len(pipeline_loads)} PIPELINES)*\n\n"
            )

        if pipeline_loads:
            pipeline_strs = []
            for load_info in pipeline_loads:
                load_dict = load_info.asdict()

                pipeline_str = (
                    f"*Pipeline -> {load_dict['pipeline']['pipeline_name']}*\n"
                )
                pipeline_str += f"""
    - *Destination*: {load_dict['destination_displayable_credentials']}
    - *Dataset*: {load_dict['dataset_name']}
    - *Started at*: {load_dict['started_at'].astimezone()}
    - *Finished at*: {load_dict['finished_at'].astimezone()}
        """
                job_strs = []
                for package in load_dict["load_packages"]:
                    jobs = package["jobs"]
                    pipeline_str += f"\n*Job information ({len(jobs)} tables)*\n\n"
                    for job_info in jobs:
                        status = (
                            "*Failed*" if job_info["failed_message"] else "*Success*"
                        )
                        job_str = f"- {status} for table `{job_info['table_name']}`"
                        if status.strip("*") == "Failed":
                            job_str += f": {job_info['failed_message']}"
                        job_strs.append(job_str)

                pipeline_str += "\n".join(job_strs)

                pipeline_strs.append(pipeline_str)

            message_str += "\n___\n".join(pipeline_strs)

            with open(self.get_filename("log", "json"), "w") as f:
                f.write(
                    "\n".join(
                        json.dumps(
                            load_info.asdict(), indent=4, sort_keys=True, default=str
                        )
                        for load_info in pipeline_loads
                    )
                )

        if exception:
            message_str += f"\n\n*Exception found*: {exception}"
            with open(self.get_filename("error", "log"), "w") as f:
                f.write(f"{type(exception)}: {exception}")

        with open(self.get_filename("summary", "md"), "w") as f:
            f.write(message_str)

        if self.slack_hook:
            send_slack_message(self.slack_hook, message_str)

    # def redirect_stderr(self):
    #     filename = self.get_filename("stderr", "log")
    #     with open(filename, "w") as f:
    #         f.writelines(sys.stderr.readlines())

    # def stderr_redirect(self):
    #     parent = self
    #
    #     class StderrRedirecter:
    #
    #         def __init__(self) -> None:
    #             self.filename = parent.get_filename("stderr", "log")
    #             self.mode = "w"
    #
    #         def __enter__(self):
    #             self.file = open(self.filename, self.mode)
    #             with
    #             return self.file
