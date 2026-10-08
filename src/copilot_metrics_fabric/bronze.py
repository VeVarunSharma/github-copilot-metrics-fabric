"""Reusable, storage-agnostic Bronze ingestion for daily Copilot reports."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Protocol
from uuid import uuid4

from copilot_metrics_fabric.github_client import (
    DownloadResult,
    GitHubCopilotClient,
    ReportScope,
    ReportType,
)

ALL_REPORT_TYPES = tuple(ReportType)
_SAFE_PARTITION_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class BronzeIngestionError(RuntimeError):
    """Base exception for Bronze ingestion failures."""


class BronzeStorageError(BronzeIngestionError):
    """Raised when Bronze storage cannot complete an operation."""


class BronzeValidationError(BronzeIngestionError, ValueError):
    """Raised when an ingestion request or stored manifest is invalid."""


@dataclass(frozen=True, slots=True)
class IngestionFailure:
    """A safe, credential-free description of one failed report request."""

    scope: ReportScope
    report_type: ReportType
    report_day: date
    error_type: str


class BronzeBatchError(BronzeIngestionError):
    """Raised after a batch in which one or more report requests failed."""

    def __init__(
        self,
        failures: tuple[IngestionFailure, ...],
        completed: tuple[IngestionResult, ...],
    ) -> None:
        self.failures = failures
        self.completed = completed
        super().__init__(
            f"{len(failures)} Bronze report request(s) failed; "
            f"{len(completed)} completed"
        )


@dataclass(frozen=True, slots=True)
class BronzeFile:
    path: str
    sha256: str
    byte_count: int
    record_count: int


@dataclass(frozen=True, slots=True)
class IngestionResult:
    scope: ReportScope
    report_type: ReportType
    report_day: date
    ingestion_id: str
    status: str
    manifest_path: str
    files: tuple[BronzeFile, ...] = ()
    duplicate_of: str | None = None


class BronzeStorage(Protocol):
    """Minimal storage contract suitable for local paths or notebook adapters."""

    def write_stream(
        self,
        path: str,
        writer: Callable[[BinaryIO], DownloadResult],
    ) -> DownloadResult:
        """Create a binary object exclusively and stream content into it."""

    def write_json(
        self,
        path: str,
        value: Mapping[str, Any],
        *,
        overwrite: bool = False,
    ) -> None:
        """Write a JSON object."""

    def read_json(self, path: str) -> Mapping[str, Any]:
        """Read a JSON object."""

    def iter_json(self, prefix: str) -> Iterator[tuple[str, Mapping[str, Any]]]:
        """Yield JSON objects below a prefix."""

    def move(self, source: str, destination: str) -> None:
        """Move an object to an immutable destination."""

    def delete_prefix(self, prefix: str) -> None:
        """Delete temporary objects under a prefix."""


class LocalBronzeStorage:
    """Filesystem implementation usable by local jobs and mounted OneLake paths."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write_stream(
        self,
        path: str,
        writer: Callable[[BinaryIO], DownloadResult],
    ) -> DownloadResult:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with target.open("xb") as destination:
                return writer(destination)
        except Exception:
            target.unlink(missing_ok=True)
            raise

    def write_json(
        self,
        path: str,
        value: Mapping[str, Any],
        *,
        overwrite: bool = False,
    ) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if overwrite else "x"
        try:
            with target.open(mode, encoding="utf-8", newline="\n") as destination:
                json.dump(value, destination, sort_keys=True, separators=(",", ":"))
                destination.write("\n")
        except (OSError, TypeError, ValueError) as error:
            raise BronzeStorageError(
                f"cannot write Bronze JSON object: {path}"
            ) from error

    def read_json(self, path: str) -> Mapping[str, Any]:
        try:
            value = json.loads(self._resolve(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BronzeStorageError(
                f"cannot read Bronze JSON object: {path}"
            ) from error
        if not isinstance(value, dict):
            raise BronzeStorageError(f"Bronze JSON object is not a mapping: {path}")
        return value

    def iter_json(self, prefix: str) -> Iterator[tuple[str, Mapping[str, Any]]]:
        directory = self._resolve(prefix)
        if not directory.exists():
            return
        for path in sorted(directory.rglob("*.json")):
            relative = path.relative_to(self.root).as_posix()
            yield relative, self.read_json(relative)

    def move(self, source: str, destination: str) -> None:
        source_path = self._resolve(source)
        destination_path = self._resolve(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if destination_path.exists():
            raise BronzeStorageError(
                f"immutable Bronze destination already exists: {destination}"
            )
        try:
            os.replace(source_path, destination_path)
        except OSError as error:
            raise BronzeStorageError(
                f"cannot commit Bronze object: {destination}"
            ) from error

    def delete_prefix(self, prefix: str) -> None:
        target = self._resolve(prefix)
        try:
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
        except OSError as error:
            raise BronzeStorageError(
                f"cannot remove temporary Bronze objects: {prefix}"
            ) from error

    def _resolve(self, relative: str) -> Path:
        path = PurePosixPath(relative)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise BronzeStorageError("Bronze storage paths must be relative")
        return self.root.joinpath(*path.parts)


class MemoryBronzeStorage:
    """In-memory implementation for tests and lightweight notebook exploration."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def write_stream(
        self,
        path: str,
        writer: Callable[[BinaryIO], DownloadResult],
    ) -> DownloadResult:
        _validate_relative_path(path)
        if path in self.objects:
            raise BronzeStorageError(f"immutable Bronze destination exists: {path}")
        destination = BytesIO()
        result = writer(destination)
        self.objects[path] = destination.getvalue()
        return result

    def write_json(
        self,
        path: str,
        value: Mapping[str, Any],
        *,
        overwrite: bool = False,
    ) -> None:
        _validate_relative_path(path)
        if path in self.objects and not overwrite:
            raise BronzeStorageError(f"immutable Bronze destination exists: {path}")
        self.objects[path] = (
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()

    def read_json(self, path: str) -> Mapping[str, Any]:
        try:
            value = json.loads(self.objects[path])
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BronzeStorageError(
                f"cannot read Bronze JSON object: {path}"
            ) from error
        if not isinstance(value, dict):
            raise BronzeStorageError(f"Bronze JSON object is not a mapping: {path}")
        return value

    def iter_json(self, prefix: str) -> Iterator[tuple[str, Mapping[str, Any]]]:
        normalized = prefix.rstrip("/") + "/"
        for path in sorted(self.objects):
            if path.startswith(normalized) and path.endswith(".json"):
                yield path, self.read_json(path)

    def move(self, source: str, destination: str) -> None:
        _validate_relative_path(source)
        _validate_relative_path(destination)
        if destination in self.objects:
            raise BronzeStorageError(
                f"immutable Bronze destination already exists: {destination}"
            )
        try:
            self.objects[destination] = self.objects.pop(source)
        except KeyError as error:
            raise BronzeStorageError(
                f"Bronze source does not exist: {source}"
            ) from error

    def delete_prefix(self, prefix: str) -> None:
        normalized = prefix.rstrip("/")
        for path in tuple(self.objects):
            if path == normalized or path.startswith(normalized + "/"):
                del self.objects[path]


def report_days(
    *,
    today: date | None = None,
    trailing_days: int = 28,
    start_day: date | None = None,
    end_day: date | None = None,
) -> tuple[date, ...]:
    """Return an inclusive backfill range or a trailing window ending yesterday."""
    effective_today = today or datetime.now(timezone.utc).date()
    if isinstance(effective_today, datetime) or not isinstance(effective_today, date):
        raise BronzeValidationError("today must be a date")
    if (start_day is None) != (end_day is None):
        raise BronzeValidationError(
            "start_day and end_day must both be provided for an explicit backfill"
        )
    latest = effective_today - timedelta(days=1)
    if start_day is None:
        if type(trailing_days) is not int or trailing_days < 1:
            raise BronzeValidationError("trailing_days must be a positive integer")
        start = latest - timedelta(days=trailing_days - 1)
        end = latest
    else:
        if (
            isinstance(start_day, datetime)
            or not isinstance(start_day, date)
            or isinstance(end_day, datetime)
            or not isinstance(end_day, date)
        ):
            raise BronzeValidationError("backfill boundaries must be dates")
        start = start_day
        end = end_day
        if start > end:
            raise BronzeValidationError("start_day must not be after end_day")
        if end > latest:
            raise BronzeValidationError("report ranges must end before today")
    return tuple(
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    )


class BronzeIngestor:
    """Download daily reports and commit immutable, content-aware Bronze records."""

    def __init__(
        self,
        client: GitHubCopilotClient,
        storage: BronzeStorage,
        *,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.client = client
        self.storage = storage
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._id_factory = id_factory or (lambda: uuid4().hex)

    def ingest(
        self,
        days: Iterable[date],
        report_types: Iterable[ReportType] = ALL_REPORT_TYPES,
        *,
        reuse_before: date | None = None,
    ) -> tuple[IngestionResult, ...]:
        """Ingest configured reports, reusing complete history before a cutoff."""
        validated_days = _validate_days(days)
        validated_types = _validate_report_types(report_types)
        if reuse_before is not None and (
            isinstance(reuse_before, datetime) or not isinstance(reuse_before, date)
        ):
            raise BronzeValidationError("reuse_before must be a date")
        completed: list[IngestionResult] = []
        failures: list[IngestionFailure] = []
        for scope in self.client.scopes:
            for report_day in validated_days:
                for report_type in validated_types:
                    try:
                        if reuse_before is not None and report_day < reuse_before:
                            reused = self._reuse_complete(
                                scope, report_type, report_day
                            )
                            if reused is not None:
                                completed.append(reused)
                                continue
                        completed.append(
                            self.ingest_one(scope, report_type, report_day)
                        )
                    except Exception as error:
                        failures.append(
                            IngestionFailure(
                                scope,
                                report_type,
                                report_day,
                                type(error).__name__,
                            )
                        )
        if failures:
            raise BronzeBatchError(tuple(failures), tuple(completed))
        return tuple(completed)

    def _reuse_complete(
        self,
        scope: ReportScope,
        report_type: ReportType,
        report_day: date,
    ) -> IngestionResult | None:
        ingestion_id = self._new_ingestion_id()
        started_at = _utc_iso(self._clock())
        paths = _Paths(scope, report_type, report_day, ingestion_id)
        reusable = self._find_latest_complete(paths.manifest_prefix)
        if reusable is None:
            return None
        result = self._result_from_manifest(reusable[0], reusable[1])
        self._write_audit(
            paths.audit_path,
            started_at,
            "reused",
            scope,
            report_type,
            report_day,
            ingestion_id,
            duplicate_of=result.ingestion_id,
        )
        return IngestionResult(
            result.scope,
            result.report_type,
            result.report_day,
            result.ingestion_id,
            "reused",
            result.manifest_path,
            result.files,
            result.ingestion_id,
        )

    def ingest_one(
        self,
        scope: ReportScope,
        report_type: ReportType,
        report_day: date,
    ) -> IngestionResult:
        """Ingest one scope/type/day request transactionally."""
        if scope not in self.client.scopes:
            raise BronzeValidationError("scope is not configured")
        if not isinstance(report_type, ReportType):
            raise BronzeValidationError("unsupported report type")
        if isinstance(report_day, datetime) or not isinstance(report_day, date):
            raise BronzeValidationError("report_day must be a date")

        ingestion_id = self._new_ingestion_id()
        started_at = _utc_iso(self._clock())
        paths = _Paths(scope, report_type, report_day, ingestion_id)
        manifest_committed = False
        try:
            metadata = self.client.get_daily_report(scope, report_type, report_day)
            if metadata is None:
                duplicate = self._find_duplicate(paths.manifest_prefix, "no_data", ())
                if duplicate is not None:
                    result = self._result_from_manifest(
                        duplicate[0], duplicate[1], True
                    )
                    self._write_audit(
                        paths.audit_path,
                        started_at,
                        "duplicate",
                        scope,
                        report_type,
                        report_day,
                        ingestion_id,
                        duplicate_of=result.ingestion_id,
                    )
                    return result
                manifest = self._manifest(
                    scope,
                    report_type,
                    report_day,
                    ingestion_id,
                    started_at,
                    "no_data",
                    (),
                )
                self.storage.write_json(paths.manifest_path, manifest)
                manifest_committed = True
                self._write_audit(
                    paths.audit_path,
                    started_at,
                    "no_data",
                    scope,
                    report_type,
                    report_day,
                    ingestion_id,
                )
                return self._result_from_manifest(paths.manifest_path, manifest)

            staged: list[tuple[str, DownloadResult]] = []
            for index, link in enumerate(metadata.download_links):
                staging_path = paths.staging_file(index)
                result = self.storage.write_stream(
                    staging_path,
                    lambda destination, url=link: self.client.download_ndjson(
                        url, destination
                    ),
                )
                staged.append((staging_path, result))

            signature = tuple(item.sha256 for _, item in staged)
            duplicate = self._find_duplicate(
                paths.manifest_prefix, "success", signature
            )
            if duplicate is not None:
                self.storage.delete_prefix(paths.staging_prefix)
                result = self._result_from_manifest(duplicate[0], duplicate[1], True)
                self._write_audit(
                    paths.audit_path,
                    started_at,
                    "duplicate",
                    scope,
                    report_type,
                    report_day,
                    ingestion_id,
                    duplicate_of=result.ingestion_id,
                )
                return result

            files: list[BronzeFile] = []
            for index, (staging_path, download) in enumerate(staged):
                destination = paths.data_file(index)
                self.storage.move(staging_path, destination)
                files.append(
                    BronzeFile(
                        destination,
                        download.sha256,
                        download.byte_count,
                        download.record_count,
                    )
                )
            manifest = self._manifest(
                scope,
                report_type,
                report_day,
                ingestion_id,
                started_at,
                "success",
                tuple(files),
            )
            self.storage.write_json(paths.manifest_path, manifest)
            manifest_committed = True
            self.storage.delete_prefix(paths.staging_prefix)
            self._write_audit(
                paths.audit_path,
                started_at,
                "success",
                scope,
                report_type,
                report_day,
                ingestion_id,
            )
            return self._result_from_manifest(paths.manifest_path, manifest)
        except Exception as error:
            self.storage.delete_prefix(paths.staging_prefix)
            if not manifest_committed:
                self.storage.delete_prefix(paths.data_prefix)
            self._write_audit(
                paths.audit_path,
                started_at,
                "failed",
                scope,
                report_type,
                report_day,
                ingestion_id,
                error_type=type(error).__name__,
            )
            raise

    def _find_duplicate(
        self,
        prefix: str,
        status: str,
        signature: tuple[str, ...],
    ) -> tuple[str, Mapping[str, Any]] | None:
        for path, manifest in self.storage.iter_json(prefix):
            try:
                manifest_status = manifest["status"]
                hashes = tuple(item["sha256"] for item in manifest["files"])
            except (KeyError, TypeError) as error:
                raise BronzeValidationError(
                    f"stored Bronze manifest is invalid: {path}"
                ) from error
            if manifest_status == status and hashes == signature:
                return path, manifest
        return None

    def _find_latest_complete(
        self,
        prefix: str,
    ) -> tuple[str, Mapping[str, Any]] | None:
        reusable: list[tuple[str, str, Mapping[str, Any]]] = []
        for path, manifest in self.storage.iter_json(prefix):
            try:
                status = manifest["status"]
                ingested_at = manifest["ingested_at"]
                files = manifest["files"]
            except KeyError as error:
                raise BronzeValidationError(
                    f"stored Bronze manifest is invalid: {path}"
                ) from error
            if (
                status not in {"success", "no_data"}
                or not isinstance(ingested_at, str)
                or not isinstance(files, list)
            ):
                if status in {"success", "no_data"}:
                    raise BronzeValidationError(
                        f"stored Bronze manifest is invalid: {path}"
                    )
                continue
            reusable.append((ingested_at, path, manifest))
        if not reusable:
            return None
        _, path, manifest = max(reusable, key=lambda item: (item[0], item[1]))
        return path, manifest

    @staticmethod
    def _manifest(
        scope: ReportScope,
        report_type: ReportType,
        report_day: date,
        ingestion_id: str,
        ingested_at: str,
        status: str,
        files: tuple[BronzeFile, ...],
    ) -> dict[str, Any]:
        content_fingerprint = hashlib.sha256(
            "\n".join(item.sha256 for item in files).encode()
        ).hexdigest()
        return {
            "schema_version": 1,
            "scope": {"kind": scope.kind, "slug": scope.slug},
            "report_type": report_type.value,
            "report_day": report_day.isoformat(),
            "ingestion_id": ingestion_id,
            "ingested_at": ingested_at,
            "status": status,
            "content_fingerprint": content_fingerprint,
            "files": [
                {
                    "path": item.path,
                    "sha256": item.sha256,
                    "byte_count": item.byte_count,
                    "record_count": item.record_count,
                }
                for item in files
            ],
        }

    def _write_audit(
        self,
        path: str,
        started_at: str,
        status: str,
        scope: ReportScope,
        report_type: ReportType,
        report_day: date,
        ingestion_id: str,
        *,
        duplicate_of: str | None = None,
        error_type: str | None = None,
    ) -> None:
        audit = {
            "schema_version": 1,
            "scope": {"kind": scope.kind, "slug": scope.slug},
            "report_type": report_type.value,
            "report_day": report_day.isoformat(),
            "attempt_id": ingestion_id,
            "started_at": started_at,
            "completed_at": _utc_iso(self._clock()),
            "status": status,
            "duplicate_of": duplicate_of,
            "error_type": error_type,
        }
        self.storage.write_json(path, audit)

    def _new_ingestion_id(self) -> str:
        value = self._id_factory()
        if not isinstance(value, str) or not _SAFE_PARTITION_VALUE.fullmatch(value):
            raise BronzeValidationError("ingestion IDs must be safe partition values")
        return value

    @staticmethod
    def _result_from_manifest(
        path: str,
        manifest: Mapping[str, Any],
        duplicate: bool = False,
    ) -> IngestionResult:
        try:
            scope_value = manifest["scope"]
            files = tuple(
                BronzeFile(
                    item["path"],
                    item["sha256"],
                    item["byte_count"],
                    item["record_count"],
                )
                for item in manifest["files"]
            )
            ingestion_id = manifest["ingestion_id"]
            return IngestionResult(
                ReportScope(scope_value["kind"], scope_value["slug"]),
                ReportType(manifest["report_type"]),
                date.fromisoformat(manifest["report_day"]),
                ingestion_id,
                "duplicate" if duplicate else manifest["status"],
                path,
                files,
                ingestion_id if duplicate else None,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise BronzeValidationError(
                f"stored Bronze manifest is invalid: {path}"
            ) from error


@dataclass(frozen=True, slots=True)
class _Paths:
    scope: ReportScope
    report_type: ReportType
    report_day: date
    ingestion_id: str

    @property
    def partition_prefix(self) -> str:
        return (
            f"scope_kind={self.scope.kind}/scope={self.scope.slug}/"
            f"report_type={self.report_type.value}/"
            f"report_day={self.report_day.isoformat()}"
        )

    @property
    def data_prefix(self) -> str:
        return f"{self.partition_prefix}/ingestion_id={self.ingestion_id}"

    @property
    def manifest_prefix(self) -> str:
        return f"_manifests/{self.partition_prefix}"

    @property
    def manifest_path(self) -> str:
        return f"{self.manifest_prefix}/{self.ingestion_id}.json"

    @property
    def audit_path(self) -> str:
        return f"_audit/{self.ingestion_id}.json"

    @property
    def staging_prefix(self) -> str:
        return f"_staging/{self.ingestion_id}"

    def staging_file(self, index: int) -> str:
        return f"{self.staging_prefix}/part-{index:05d}.ndjson"

    def data_file(self, index: int) -> str:
        return f"{self.data_prefix}/part-{index:05d}.ndjson"


def _validate_relative_path(path: str) -> None:
    parsed = PurePosixPath(path)
    if parsed.is_absolute() or ".." in parsed.parts or not parsed.parts:
        raise BronzeStorageError("Bronze storage paths must be relative")


def _validate_days(days: Iterable[date]) -> tuple[date, ...]:
    values = tuple(days)
    if not values:
        raise BronzeValidationError("at least one report day is required")
    if any(
        isinstance(value, datetime) or not isinstance(value, date)
        for value in values
    ):
        raise BronzeValidationError("report days must be dates")
    if len(set(values)) != len(values):
        raise BronzeValidationError("report days must not contain duplicates")
    return tuple(sorted(values))


def _validate_report_types(
    report_types: Iterable[ReportType],
) -> tuple[ReportType, ...]:
    values = tuple(report_types)
    if not values:
        raise BronzeValidationError("at least one report type is required")
    if any(not isinstance(value, ReportType) for value in values):
        raise BronzeValidationError("unsupported report type")
    if len(set(values)) != len(values):
        raise BronzeValidationError("report types must not contain duplicates")
    return values


def _utc_iso(value: datetime) -> str:
    if not isinstance(value, datetime):
        raise BronzeValidationError("clock must return a datetime")
    if value.tzinfo is None:
        raise BronzeValidationError("clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
