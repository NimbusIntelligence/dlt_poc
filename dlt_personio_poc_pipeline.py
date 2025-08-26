from datetime import date
from typing import Any
import dlt
from dlt.common.configuration import configspec
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.auth import (
    OAuth2ClientCredentials,
)
from dlt.sources.rest_api.typing import RESTAPIConfig


@configspec
class OAuth2ClientCredentialsHTTPBasic(OAuth2ClientCredentials):

    def parse_access_token(self, response_json: Any) -> str:
        token = response_json.get("data", {}).get("token")

        if not token:
            raise RuntimeError(f"Validation failed:\n{response_json}")

        return token

    def parse_expiration_in_seconds(self, response_json: Any) -> int:
        return response_json.get("data", {}).get("expires_in")


# client = RESTClient(base_url="https://api.personio.de/v1", auth=oauth)
# client = RESTClient(
#     base_url="https://api.personio.de/v1",
#     auth=BearerTokenAuth(
#         token="papi-LwlycbAXqCKcC8kOQKMVkxbIsgYZlQ4wK-dX8qfg1dHrC3WfbVC69C7Oc8KZb_GgSSQkd8SFBmyPweobKlLoqw"
#     ),
# )


def load_personio() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="load_personio_snowflake",
        dataset_name="dlt_personio_raw",
        destination="snowflake",
    )

    oauth = OAuth2ClientCredentialsHTTPBasic(
        access_token_url="https://api.personio.de/v1/auth",
        client_id=dlt.secrets["api_client_id"],
        client_secret=dlt.secrets["api_client_secret"],
        default_token_expiration=86400,
    )

    personio_config: RESTAPIConfig = {
        "client": {"base_url": "https://api.personio.de/v1/", "auth": oauth},
        "resource_defaults": {
            "endpoint": {
                "paginator": {
                    "type": "offset",
                    "limit": 200,
                    "offset": 0,
                    "total_path": "metadata.total_elements",
                },
                "data_selector": "data",
            },
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "employees",
                "endpoint": {"path": "company/employees"},
            },
            {
                "name": "time_off_types",
                "endpoint": {
                    "path": "company/time-off-types",
                    "paginator": {"type": "single_page"},
                },
            },
            {
                "name": "attendances",
                "endpoint": {
                    "path": "company/attendances",
                    "params": {
                        "start_date": "2024-01-01",
                        "end_date": date.today().strftime("%Y-%m-%d"),
                    },
                },
            },
            {
                "name": "projects",
                "endpoint": {"path": "company/attendances/projects"},
            },
            {
                "name": "absence_periods",
                "endpoint": {
                    "path": "company/absence-periods",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page": 1,
                        "page_param": "offset",
                        "total_path": "metadata.total_pages",
                    },
                    "params": {"limit": 200, "offset": 1},
                },
            },
        ],
    }

    personio_source = rest_api_source(personio_config)

    pipeline.run(personio_source)


load_personio()
