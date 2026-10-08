import json
from datetime import date, datetime, timezone
from io import BytesIO

import pytest

from copilot_metrics_fabric.bronze import (
    BronzeBatchError,
    BronzeIngestor,
    BronzeValidationError,
    LocalBronzeStorage,
    MemoryBronzeStorage,
    report_days,
)
from copilot_metrics_fabric.github_client import (
    DownloadResult,
    ReportMetadata,
    ReportScope,
    ReportType,
    ResponseValidationError,
)

DAY = date(2025, 10, 13)
SCOPE = ReportScope("organization", "example-org")
NOW = datetime(2025, 10, 15, 12, tzinfo=timezone.utc)


class FakeClient:
    scopes = (SCOPE,)

    def __init__(self, reports, downloads):
        self.reports = list(reports)
        self.downloads = list(downloads)
        self.metadata_calls = []
        self.download_calls = []

    def get_daily_report(self, scope, report_type, report_day):
        self.metadata_calls.append((scope, report_type, report_day))
        value = self.reports.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def download_ndjson(self, url, destination):
        self.download_calls.append(url)
        value = self.downloads.pop(0)
        if isinstance(value, Exception):
            raise value
        destination.write(value)
        records = sum(bool(line.strip()) for line in value.splitlines())
        import hashlib

        return DownloadResult(hashlib.sha256(value).hexdigest(), len(value), records)


def metadata(*links):
    return ReportMetadata(SCOPE, ReportType.USERS, DAY, tuple(links))


def ingestor(client, storage=None, ids=None):
    values = iter(ids or ["attempt-1"])
    return BronzeIngestor(
        client,
        storage or MemoryBronzeStorage(),
        clock=lambda: NOW,
        id_factory=lambda: next(values),
    )


def test_downloads_and_commits_multiple_files_with_safe_manifest():
    links = (
        "https://reports.test/one?sig=secret-one",
        "https://reports.test/two?sig=secret-two",
    )
    storage = MemoryBronzeStorage()
    result = ingestor(
        FakeClient([metadata(*links)], [b'{"a":1}\n', b'{"b":2}\n']),
        storage,
    ).ingest_one(SCOPE, ReportType.USERS, DAY)

    assert result.status == "success"
    assert len(result.files) == 2
    assert all(
        "report_type=users/report_day=2025-10-13/ingestion_id=attempt-1"
        in item.path
        for item in result.files
    )
    persisted = b"".join(storage.objects.values())
    assert b"secret-one" not in persisted
    assert b"secret-two" not in persisted
    manifest = storage.read_json(result.manifest_path)
    assert [item["record_count"] for item in manifest["files"]] == [1, 1]


def test_204_is_successful_no_data_and_rerun_is_idempotent():
    storage = MemoryBronzeStorage()
    client = FakeClient([None, None], [])
    loader = ingestor(client, storage, ["first", "second"])

    first = loader.ingest_one(SCOPE, ReportType.USERS, DAY)
    second = loader.ingest_one(SCOPE, ReportType.USERS, DAY)

    assert first.status == "no_data"
    assert second.status == "duplicate"
    assert second.duplicate_of == "first"
    assert len(tuple(storage.iter_json("_manifests"))) == 1
    assert storage.read_json("_audit/second.json")["status"] == "duplicate"


def test_duplicate_content_is_not_committed_again():
    content = b'{"same":true}\n'
    storage = MemoryBronzeStorage()
    client = FakeClient(
        [metadata("https://r.test/a?s=1"), metadata("https://r.test/b?s=2")],
        [content, content],
    )
    loader = ingestor(client, storage, ["first", "second"])

    first = loader.ingest_one(SCOPE, ReportType.USERS, DAY)
    second = loader.ingest_one(SCOPE, ReportType.USERS, DAY)

    assert first.status == "success"
    assert second.status == "duplicate"
    assert second.duplicate_of == "first"
    assert len(tuple(storage.iter_json("_manifests"))) == 1
    assert not any(path.startswith("_staging/") for path in storage.objects)


def test_corrected_late_content_is_retained_as_new_immutable_ingestion():
    storage = MemoryBronzeStorage()
    client = FakeClient(
        [metadata("https://r.test/a?s=1"), metadata("https://r.test/a?s=2")],
        [b'{"value":1}\n', b'{"value":2}\n'],
    )
    loader = ingestor(client, storage, ["original", "corrected"])

    original = loader.ingest_one(SCOPE, ReportType.USERS, DAY)
    corrected = loader.ingest_one(SCOPE, ReportType.USERS, DAY)

    assert corrected.status == "success"
    assert original.ingestion_id != corrected.ingestion_id
    assert len(tuple(storage.iter_json("_manifests"))) == 2
    assert storage.objects[original.files[0].path] == b'{"value":1}\n'
    assert storage.objects[corrected.files[0].path] == b'{"value":2}\n'


def test_complete_history_before_refresh_window_is_reused_without_github_calls():
    storage = MemoryBronzeStorage()
    original_client = FakeClient(
        [metadata("https://r.test/original?s=1")],
        [b'{"value":1}\n'],
    )
    ingestor(original_client, storage, ["original"]).ingest_one(
        SCOPE, ReportType.USERS, DAY
    )
    reuse_client = FakeClient([], [])

    result = ingestor(reuse_client, storage, ["reuse-attempt"]).ingest(
        [DAY],
        [ReportType.USERS],
        reuse_before=date(2025, 10, 14),
    )

    assert result[0].status == "reused"
    assert result[0].duplicate_of == "original"
    assert reuse_client.metadata_calls == []
    assert reuse_client.download_calls == []
    assert storage.read_json("_audit/reuse-attempt.json")["status"] == "reused"


def test_refresh_window_still_checks_github_for_late_corrections():
    storage = MemoryBronzeStorage()
    loader = ingestor(
        FakeClient(
            [
                metadata("https://r.test/original?s=1"),
                metadata("https://r.test/corrected?s=2"),
            ],
            [b'{"value":1}\n', b'{"value":2}\n'],
        ),
        storage,
        ["original", "corrected"],
    )
    loader.ingest_one(SCOPE, ReportType.USERS, DAY)

    result = loader.ingest(
        [DAY],
        [ReportType.USERS],
        reuse_before=DAY,
    )

    assert result[0].status == "success"
    assert result[0].ingestion_id == "corrected"
    assert len(tuple(storage.iter_json("_manifests"))) == 2


def test_invalid_ndjson_failure_leaves_no_partial_data_or_manifest():
    storage = MemoryBronzeStorage()
    client = FakeClient(
        [metadata("https://r.test/a?s=secret")],
        [ResponseValidationError("invalid NDJSON")],
    )

    with pytest.raises(ResponseValidationError):
        ingestor(client, storage).ingest_one(SCOPE, ReportType.USERS, DAY)

    assert not any("report_type=" in path for path in storage.objects)
    assert not any(path.startswith("_manifests/") for path in storage.objects)
    audit = storage.read_json("_audit/attempt-1.json")
    assert audit["status"] == "failed"
    assert audit["error_type"] == "ResponseValidationError"
    assert "secret" not in json.dumps(audit)


def test_local_storage_removes_partial_download_when_writer_fails(tmp_path):
    storage = LocalBronzeStorage(tmp_path)

    def fail_after_partial_write(destination):
        destination.write(b'{"partial":')
        raise ResponseValidationError(
            "invalid https://reports.test/file?signature=secret"
        )

    with pytest.raises(ResponseValidationError):
        storage.write_stream(
            "_staging/attempt/report.ndjson", fail_after_partial_write
        )

    assert not (tmp_path / "_staging/attempt/report.ndjson").exists()


def test_batch_preserves_successes_and_reports_partial_failures():
    storage = MemoryBronzeStorage()
    client = FakeClient(
        [
            metadata("https://r.test/ok?s=1"),
            RuntimeError("must not persist https://r.test/fail?sig=secret"),
        ],
        [b'{"ok":true}\n'],
    )
    loader = ingestor(client, storage, ["success-id", "failure-id"])

    with pytest.raises(BronzeBatchError) as caught:
        loader.ingest([DAY], [ReportType.USERS, ReportType.REPOSITORIES])

    assert len(caught.value.completed) == 1
    assert caught.value.failures[0].error_type == "RuntimeError"
    assert storage.read_json("_audit/failure-id.json")["error_type"] == "RuntimeError"
    assert b"sig=secret" not in b"".join(storage.objects.values())


def test_retries_are_delegated_to_client_once_per_link():
    client = FakeClient(
        [metadata("https://r.test/retried?s=1")],
        [b'{"eventually":"returned"}\n'],
    )

    result = ingestor(client).ingest_one(SCOPE, ReportType.USERS, DAY)

    assert result.status == "success"
    assert client.download_calls == ["https://r.test/retried?s=1"]


def test_default_window_ends_yesterday_and_explicit_backfill_is_inclusive():
    assert report_days(today=date(2025, 10, 15), trailing_days=3) == (
        date(2025, 10, 12),
        date(2025, 10, 13),
        date(2025, 10, 14),
    )
    assert report_days(
        today=date(2025, 10, 15),
        start_day=date(2025, 10, 10),
        end_day=date(2025, 10, 12),
    ) == (
        date(2025, 10, 10),
        date(2025, 10, 11),
        date(2025, 10, 12),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"trailing_days": 0},
        {"start_day": DAY},
        {"start_day": date(2025, 10, 14), "end_day": DAY},
        {"start_day": DAY, "end_day": date(2025, 10, 15)},
    ],
)
def test_rejects_invalid_date_windows(kwargs):
    with pytest.raises(BronzeValidationError):
        report_days(today=date(2025, 10, 15), **kwargs)


def test_memory_storage_stream_callback_can_use_real_client_shape():
    storage = MemoryBronzeStorage()

    result = storage.write_stream(
        "path/file.ndjson",
        lambda target: _write_result(target, b'{"ok":true}\n'),
    )

    assert result.record_count == 1
    assert storage.objects["path/file.ndjson"] == b'{"ok":true}\n'


def _write_result(target: BytesIO, content: bytes) -> DownloadResult:
    import hashlib

    target.write(content)
    return DownloadResult(hashlib.sha256(content).hexdigest(), len(content), 1)
