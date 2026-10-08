"""Secure, testable client for GitHub Copilot usage metric reports."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from io import BytesIO
from typing import BinaryIO, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from copilot_metrics_fabric.config import GitHubConfig

API_VERSION = "2026-03-10"
DEFAULT_API_URL = "https://api.github.com"
class GitHubClientError(RuntimeError):
    """Base exception for safe-to-display GitHub client failures."""


class CredentialError(GitHubClientError):
    """Raised when no usable credential is available."""


class AuthenticationError(GitHubClientError):
    """Raised when GitHub rejects the credential."""


class PermissionDeniedError(GitHubClientError):
    """Raised when the credential cannot access a report."""


class ReportNotFoundError(GitHubClientError):
    """Raised when a requested scope or report is not found."""


class RateLimitError(GitHubClientError):
    """Raised when GitHub continues to rate-limit requests."""


class ServerError(GitHubClientError):
    """Raised when GitHub continues to return server errors."""


class ResponseValidationError(GitHubClientError):
    """Raised when report metadata or NDJSON is malformed."""


class TransportError(GitHubClientError):
    """Raised when an HTTP request cannot be completed."""


class ReportType(str, Enum):
    """Supported one-day Copilot report granularities."""

    ENTITY = "entity"
    USERS = "users"
    USER_TEAMS = "user-teams"
    REPOSITORIES = "repositories"


@dataclass(frozen=True, slots=True)
class ReportScope:
    kind: str
    slug: str


@dataclass(frozen=True, slots=True)
class ReportMetadata:
    scope: ReportScope
    report_type: ReportType
    report_day: date
    download_links: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DownloadResult:
    sha256: str
    byte_count: int
    record_count: int


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 4
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_backoff_seconds < 0 or self.max_backoff_seconds < 0:
            raise ValueError("retry delays must not be negative")


@dataclass(frozen=True, slots=True)
class HttpRequest:
    url: str
    headers: Mapping[str, str] = field(repr=False)

    def __repr__(self) -> str:
        parsed = urlsplit(self.url)
        safe_url = parsed._replace(
            query="[REDACTED]" if parsed.query else ""
        ).geturl()
        return (
            f"HttpRequest(url={safe_url!r}, "
            f"header_names={tuple(self.headers)!r})"
        )


@dataclass(slots=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: BinaryIO

    def close(self) -> None:
        self.body.close()

    def __enter__(self) -> HttpResponse:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class HttpTransport(Protocol):
    def send(self, request: HttpRequest) -> HttpResponse:
        """Send a request without logging its URL or headers."""


class UrllibTransport:
    """Standard-library HTTP transport."""

    def send(self, request: HttpRequest) -> HttpResponse:
        outbound = Request(request.url, headers=dict(request.headers), method="GET")
        try:
            response = urlopen(outbound)  # noqa: S310 - URLs are validated by client
        except HTTPError as error:
            return HttpResponse(error.code, dict(error.headers), error)
        except URLError:
            raise TransportError("GitHub request could not be completed") from None
        if urlsplit(response.geturl()).scheme != "https":
            response.close()
            raise TransportError("GitHub request redirected to an insecure URL")
        return HttpResponse(
            response.status,
            dict(response.headers),
            response,
        )


class GitHubCopilotClient:
    """Retrieve and stream reports for scopes from the application config."""

    def __init__(
        self,
        config: GitHubConfig,
        token_provider: Callable[[], str],
        *,
        transport: HttpTransport | None = None,
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] | None = None,
        api_url: str = DEFAULT_API_URL,
    ) -> None:
        api = urlsplit(api_url)
        if api.scheme != "https" or not api.netloc or api.query or api.fragment:
            raise ValueError("api_url must be an HTTPS origin")
        self._config = config
        self._token_provider = token_provider
        self._transport = transport or UrllibTransport()
        self._retry = retry_policy or RetryPolicy()
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._api_url = api_url.rstrip("/")

    @property
    def scopes(self) -> tuple[ReportScope, ...]:
        if self._config.mode == "enterprise":
            assert self._config.enterprise is not None
            return (ReportScope("enterprise", self._config.enterprise),)
        return tuple(
            ReportScope("organization", organization)
            for organization in self._config.organizations
        )

    def get_daily_report(
        self,
        scope: ReportScope,
        report_type: ReportType,
        report_day: date,
    ) -> ReportMetadata | None:
        """Return validated metadata, or ``None`` when GitHub returns 204."""
        if scope not in self.scopes:
            raise ValueError("scope is not configured")
        endpoint = self._endpoint(scope, report_type)
        url = f"{self._api_url}{endpoint}?{urlencode({'day': report_day.isoformat()})}"
        token = self._token_provider()
        if not isinstance(token, str) or not token.strip():
            raise CredentialError("GitHub token provider returned no credential")
        request = HttpRequest(
            url,
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "github-copilot-metrics-fabric",
            },
        )
        response = self._send_with_retry(request)
        with response:
            if response.status == 204:
                return None
            self._raise_for_status(response.status)
            try:
                payload = json.load(response.body)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ResponseValidationError(
                    "GitHub report metadata is not valid JSON"
                ) from error
        return self._validate_metadata(payload, scope, report_type, report_day)

    def download_ndjson(self, url: str, destination: BinaryIO) -> DownloadResult:
        """Stream one signed report URL to a binary destination and hash it."""
        self._validate_download_url(url)
        request = HttpRequest(
            url,
            {
                "Accept": "application/x-ndjson, application/json",
                "User-Agent": "github-copilot-metrics-fabric",
            },
        )
        response = self._send_with_retry(request)
        with response:
            self._raise_for_status(response.status, download=True)
            return self._copy_and_validate_ndjson(response.body, destination)

    def _send_with_retry(self, request: HttpRequest) -> HttpResponse:
        for attempt in range(1, self._retry.max_attempts + 1):
            response = self._transport.send(request)
            if (
                not (response.status == 429 or 500 <= response.status <= 599)
                or attempt == self._retry.max_attempts
            ):
                return response
            delay = self._retry_delay(response.headers, attempt)
            response.close()
            self._sleep(delay)
        raise AssertionError("retry loop did not return")

    def _retry_delay(self, headers: Mapping[str, str], attempt: int) -> float:
        retry_after = next(
            (
                value
                for key, value in headers.items()
                if key.lower() == "retry-after"
            ),
            None,
        )
        if retry_after is not None:
            try:
                delay = max(0.0, float(retry_after))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    delay = max(
                        0.0,
                        (
                            retry_at - self._now().astimezone(timezone.utc)
                        ).total_seconds(),
                    )
                except (TypeError, ValueError, OverflowError):
                    delay = 0.0
            return min(delay, self._retry.max_backoff_seconds)
        delay = self._retry.initial_backoff_seconds * (2 ** (attempt - 1))
        return min(delay, self._retry.max_backoff_seconds)

    @staticmethod
    def _raise_for_status(status: int, *, download: bool = False) -> None:
        target = "report download" if download else "report request"
        if status == 200:
            return
        if status == 401:
            raise AuthenticationError(f"GitHub rejected credentials for {target}")
        if status == 403:
            raise PermissionDeniedError(f"GitHub denied access to {target}")
        if status == 404:
            raise ReportNotFoundError(f"GitHub could not find {target}")
        if status == 429:
            raise RateLimitError(f"GitHub rate limit persisted for {target}")
        if 500 <= status <= 599:
            raise ServerError(f"GitHub server error persisted for {target}")
        raise GitHubClientError(
            f"GitHub returned unexpected HTTP status {status} for {target}"
        )

    @staticmethod
    def _validate_metadata(
        payload: object,
        scope: ReportScope,
        report_type: ReportType,
        requested_day: date,
    ) -> ReportMetadata:
        if not isinstance(payload, dict):
            raise ResponseValidationError("GitHub report metadata must be an object")
        if not {"download_links", "report_day"} <= set(payload):
            raise ResponseValidationError(
                "GitHub report metadata is missing required fields"
            )
        try:
            actual_day = date.fromisoformat(payload["report_day"])
        except (TypeError, ValueError) as error:
            raise ResponseValidationError(
                "GitHub report metadata has an invalid report_day"
            ) from error
        if actual_day != requested_day:
            raise ResponseValidationError(
                "GitHub report metadata does not match the requested day"
            )
        links = payload["download_links"]
        if not isinstance(links, list) or not links:
            raise ResponseValidationError(
                "GitHub report metadata must contain download links"
            )
        validated = []
        for link in links:
            if not isinstance(link, str):
                raise ResponseValidationError(
                    "GitHub report metadata contains an invalid download URL"
                )
            GitHubCopilotClient._validate_download_url(link)
            validated.append(link)
        return ReportMetadata(
            scope,
            report_type,
            actual_day,
            tuple(validated),
        )

    @staticmethod
    def _validate_download_url(url: str) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.query
            or parsed.fragment
        ):
            raise ResponseValidationError(
                "GitHub report download URL is not a valid HTTPS signed URL"
            )

    @staticmethod
    def _copy_and_validate_ndjson(
        source: BinaryIO,
        destination: BinaryIO,
        chunk_size: int = 64 * 1024,
    ) -> DownloadResult:
        digest = hashlib.sha256()
        byte_count = 0
        record_count = 0
        pending = bytearray()

        for chunk in iter(lambda: source.read(chunk_size), b""):
            if not isinstance(chunk, bytes):
                raise ResponseValidationError("report download did not return bytes")
            destination.write(chunk)
            digest.update(chunk)
            byte_count += len(chunk)
            pending.extend(chunk)
            while b"\n" in pending:
                raw_line, _, remainder = pending.partition(b"\n")
                pending = bytearray(remainder)
                record_count += GitHubCopilotClient._validate_ndjson_line(raw_line)

        if pending:
            record_count += GitHubCopilotClient._validate_ndjson_line(bytes(pending))
        return DownloadResult(digest.hexdigest(), byte_count, record_count)

    @staticmethod
    def _validate_ndjson_line(raw_line: bytes) -> int:
        if not raw_line.strip():
            return 0
        try:
            value = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ResponseValidationError(
                "GitHub report download contains invalid NDJSON"
            ) from error
        if not isinstance(value, dict):
            raise ResponseValidationError(
                "GitHub report download contains a non-object NDJSON record"
            )
        return 1

    @staticmethod
    def _endpoint(scope: ReportScope, report_type: ReportType) -> str:
        slug = quote(scope.slug, safe="")
        if scope.kind == "enterprise":
            prefix = f"/enterprises/{slug}/copilot/metrics/reports"
            entity_name = "enterprise"
        elif scope.kind == "organization":
            prefix = f"/orgs/{slug}/copilot/metrics/reports"
            entity_name = "organization"
        else:
            raise ValueError("scope kind must be enterprise or organization")
        report_name = {
            ReportType.ENTITY: entity_name,
            ReportType.USERS: "users",
            ReportType.USER_TEAMS: "user-teams",
            ReportType.REPOSITORIES: "repos",
        }[report_type]
        return f"{prefix}/{report_name}-1-day"


def bytes_response(
    status: int,
    body: bytes = b"",
    headers: Mapping[str, str] | None = None,
) -> HttpResponse:
    """Build an in-memory response for adapters and tests."""
    return HttpResponse(status, headers or {}, BytesIO(body))


def iter_report_requests(
    client: GitHubCopilotClient,
    report_types: tuple[ReportType, ...],
    days: tuple[date, ...],
) -> Iterator[tuple[ReportScope, ReportType, date]]:
    """Yield deterministic configured report requests without performing I/O."""
    for scope in client.scopes:
        for report_day in days:
            for report_type in report_types:
                yield scope, report_type, report_day
