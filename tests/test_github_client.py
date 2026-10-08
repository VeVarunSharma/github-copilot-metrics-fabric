import hashlib
import json
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path

import pytest

from copilot_metrics_fabric.config import GitHubConfig
from copilot_metrics_fabric.credentials import EnvironmentTokenProvider
from copilot_metrics_fabric.github_client import (
    AuthenticationError,
    GitHubCopilotClient,
    HttpRequest,
    PermissionDeniedError,
    ReportNotFoundError,
    ReportType,
    ResponseValidationError,
    RetryPolicy,
    ServerError,
    bytes_response,
)

FIXTURES = Path(__file__).parent / "fixtures"


class FakeTransport:
    def __init__(self, *responses, expected_token: str | None = None):
        self.responses = list(responses)
        self.requests = []
        self.expected_authorization_hash = (
            hashlib.sha256(f"Bearer {expected_token}".encode()).digest()
            if expected_token is not None
            else None
        )
        self.received_expected_bearer = False

    def send(self, request):
        authorization = request.headers.get("Authorization")
        if self.expected_authorization_hash is not None:
            self.received_expected_bearer = (
                isinstance(authorization, str)
                and hashlib.sha256(authorization.encode()).digest()
                == self.expected_authorization_hash
            )
        recorded_headers = {
            key: "[REDACTED]" if key.lower() == "authorization" else value
            for key, value in request.headers.items()
        }
        self.requests.append(HttpRequest(request.url, recorded_headers))
        return self.responses.pop(0)


def assert_secret_absent(rendered: str, secret: str) -> None:
    if secret in rendered:
        raise AssertionError("sensitive value appeared in diagnostic output")


def metadata_bytes() -> bytes:
    return (FIXTURES / "copilot_report_metadata.json").read_bytes()


def org_client(transport, **kwargs):
    return GitHubCopilotClient(
        GitHubConfig("organization", ("example-org",)),
        lambda: "secret-token",
        transport=transport,
        **kwargs,
    )


@pytest.mark.parametrize(
    ("report_type", "suffix"),
    [
        (ReportType.ENTITY, "organization-1-day"),
        (ReportType.USERS, "users-1-day"),
        (ReportType.USER_TEAMS, "user-teams-1-day"),
        (ReportType.REPOSITORIES, "repos-1-day"),
    ],
)
def test_resolves_org_endpoints_and_required_headers(report_type, suffix):
    transport = FakeTransport(
        bytes_response(200, metadata_bytes()),
        expected_token="secret-token",
    )
    client = org_client(transport)

    report = client.get_daily_report(
        client.scopes[0], report_type, date(2025, 10, 13)
    )

    request = transport.requests[0]
    assert request.url == (
        f"https://api.github.com/orgs/example-org/copilot/metrics/reports/{suffix}"
        "?day=2025-10-13"
    )
    assert request.headers["Accept"] == "application/vnd.github+json"
    assert transport.received_expected_bearer
    assert request.headers["Authorization"] == "[REDACTED]"
    assert_secret_absent(repr(transport.requests), "secret-token")
    assert_secret_absent(repr(vars(transport)), "secret-token")
    assert request.headers["X-GitHub-Api-Version"] == "2026-03-10"
    assert report.report_day == date(2025, 10, 13)


def test_resolves_enterprise_entity_and_configured_scope():
    transport = FakeTransport(bytes_response(200, metadata_bytes()))
    client = GitHubCopilotClient(
        GitHubConfig("enterprise", ("allowlisted-org",), "example-enterprise"),
        lambda: "token",
        transport=transport,
    )

    client.get_daily_report(
        client.scopes[0], ReportType.ENTITY, date(2025, 10, 13)
    )

    assert client.scopes[0].kind == "enterprise"
    assert "/enterprises/example-enterprise/" in transport.requests[0].url
    assert "/enterprise-1-day?" in transport.requests[0].url


def test_github_transport_receives_token_provider_bearer_header():
    token = "github-transport-token"
    transport = FakeTransport(
        bytes_response(200, metadata_bytes()),
        expected_token=token,
    )
    client = GitHubCopilotClient(
        GitHubConfig("organization", ("example-org",)),
        lambda: token,
        transport=transport,
    )

    client.get_daily_report(
        client.scopes[0], ReportType.USERS, date(2025, 10, 13)
    )

    assert transport.received_expected_bearer
    assert transport.requests[0].headers["Authorization"] == "[REDACTED]"
    assert_secret_absent(repr(transport.requests), token)
    assert_secret_absent(repr(vars(transport)), token)


def test_204_means_no_report_data():
    client = org_client(FakeTransport(bytes_response(204)))

    result = client.get_daily_report(
        client.scopes[0], ReportType.USERS, date(2025, 10, 13)
    )

    assert result is None


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (401, AuthenticationError),
        (403, PermissionDeniedError),
        (404, ReportNotFoundError),
    ],
)
def test_explicit_client_error_handling(status, error):
    client = org_client(FakeTransport(bytes_response(status)))

    with pytest.raises(error):
        client.get_daily_report(
            client.scopes[0], ReportType.USERS, date(2025, 10, 13)
        )


def test_retries_5xx_with_bounded_retry_after():
    delays = []
    transport = FakeTransport(
        bytes_response(503, headers={"Retry-After": "120"}),
        bytes_response(500),
        bytes_response(200, metadata_bytes()),
    )
    client = org_client(
        transport,
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_backoff_seconds=2,
            max_backoff_seconds=10,
        ),
        sleep=delays.append,
    )

    client.get_daily_report(
        client.scopes[0], ReportType.USERS, date(2025, 10, 13)
    )

    assert delays == [10, 4]
    assert len(transport.requests) == 3


def test_honors_retry_after_http_date_and_retries_any_5xx():
    delays = []
    transport = FakeTransport(
        bytes_response(
            501,
            headers={"Retry-After": "Mon, 13 Oct 2025 12:00:20 GMT"},
        ),
        bytes_response(200, metadata_bytes()),
    )
    client = org_client(
        transport,
        retry_policy=RetryPolicy(max_attempts=2, max_backoff_seconds=30),
        sleep=delays.append,
        now=lambda: datetime(2025, 10, 13, 12, 0, tzinfo=timezone.utc),
    )

    client.get_daily_report(
        client.scopes[0], ReportType.USERS, date(2025, 10, 13)
    )

    assert delays == [20]


def test_raises_after_bounded_server_retries():
    client = org_client(
        FakeTransport(bytes_response(502), bytes_response(503)),
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0),
        sleep=lambda _: None,
    )

    with pytest.raises(ServerError):
        client.get_daily_report(
            client.scopes[0], ReportType.USERS, date(2025, 10, 13)
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"report_day": "2025-10-13"},
        {"download_links": [], "report_day": "2025-10-13"},
        {
            "download_links": ["http://example.test/report?sig=x"],
            "report_day": "2025-10-13",
        },
        {
            "download_links": ["https://example.test/report"],
            "report_day": "2025-10-13",
        },
        {
            "download_links": ["https://example.test/report?sig=x"],
            "report_day": "2025-10-12",
        },
    ],
)
def test_rejects_invalid_report_metadata(payload):
    client = org_client(
        FakeTransport(bytes_response(200, json.dumps(payload).encode()))
    )

    with pytest.raises(ResponseValidationError):
        client.get_daily_report(
            client.scopes[0], ReportType.USERS, date(2025, 10, 13)
        )


def test_streams_ndjson_and_returns_content_hash_without_auth_header():
    content = (FIXTURES / "copilot_users_1_day.ndjson").read_bytes()
    transport = FakeTransport(bytes_response(200, content))
    destination = BytesIO()
    client = org_client(transport)
    signed_url = "https://reports.example.test/report?signature=sanitized"

    result = client.download_ndjson(signed_url, destination)

    assert destination.getvalue() == content
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert result.byte_count == len(content)
    assert result.record_count == 1
    assert "Authorization" not in transport.requests[0].headers


@pytest.mark.parametrize(
    ("fixture_name", "records"),
    [
        ("copilot_users_1_day.ndjson", 1),
        ("copilot_user_teams_1_day.ndjson", 2),
        ("copilot_repos_1_day.ndjson", 1),
    ],
)
def test_streams_documented_sanitized_report_shapes(fixture_name, records):
    content = (FIXTURES / fixture_name).read_bytes()
    client = org_client(FakeTransport(bytes_response(200, content)))

    result = client.download_ndjson(
        "https://reports.example.test/report?signature=sanitized",
        BytesIO(),
    )

    assert result.record_count == records


def test_rejects_invalid_ndjson():
    client = org_client(FakeTransport(bytes_response(200, b'{"ok":true}\nnope\n')))

    with pytest.raises(ResponseValidationError):
        client.download_ndjson(
            "https://reports.example.test/report?signature=sanitized",
            BytesIO(),
        )


def test_errors_do_not_expose_tokens_or_signed_urls():
    token = "super-secret-token"
    signed_url = "https://reports.example.test/report?signature=super-secret-signature"
    client = GitHubCopilotClient(
        GitHubConfig("organization", ("example-org",)),
        lambda: token,
        transport=FakeTransport(bytes_response(401)),
    )

    with pytest.raises(AuthenticationError) as error:
        client.get_daily_report(
            client.scopes[0], ReportType.USERS, date(2025, 10, 13)
        )
    assert_secret_absent(str(error.value), token)

    client = org_client(FakeTransport(bytes_response(403)))
    with pytest.raises(PermissionDeniedError) as error:
        client.download_ndjson(signed_url, BytesIO())
    assert_secret_absent(str(error.value), signed_url)
    assert_secret_absent(str(error.value), "super-secret-signature")


def test_request_repr_redacts_credentials_and_signed_query_strings():
    request = HttpRequest(
        "https://reports.example.test/report?sig=super-secret-signature",
        {
            "Authorization": "Bearer super-secret-token",
            "Accept": "application/json",
        },
    )

    rendered = repr(request)

    assert_secret_absent(rendered, "super-secret-token")
    assert_secret_absent(rendered, "super-secret-signature")
    assert "?[REDACTED]" in rendered
    assert "Authorization" in rendered


def test_enterprise_endpoints_cover_all_report_type_name_differences():
    transport = FakeTransport(
        *(bytes_response(200, metadata_bytes()) for _ in ReportType)
    )
    client = GitHubCopilotClient(
        GitHubConfig("enterprise", (), "example-enterprise"),
        lambda: "token",
        transport=transport,
    )

    for report_type in ReportType:
        client.get_daily_report(
            client.scopes[0], report_type, date(2025, 10, 13)
        )

    paths = [request.url.split("?", 1)[0] for request in transport.requests]
    assert paths == [
        "https://api.github.com/enterprises/example-enterprise/"
        "copilot/metrics/reports/enterprise-1-day",
        "https://api.github.com/enterprises/example-enterprise/"
        "copilot/metrics/reports/users-1-day",
        "https://api.github.com/enterprises/example-enterprise/"
        "copilot/metrics/reports/user-teams-1-day",
        "https://api.github.com/enterprises/example-enterprise/"
        "copilot/metrics/reports/repos-1-day",
    ]


def test_environment_token_provider_reads_at_call_time():
    environment = {"GITHUB_TOKEN": "first"}
    provider = EnvironmentTokenProvider(environment)

    assert provider() == "first"
    environment["GITHUB_TOKEN"] = "second"
    assert provider() == "second"
