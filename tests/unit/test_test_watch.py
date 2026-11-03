from types import SimpleNamespace

from test_build.watch import (
    decode_observed_payload,
    default_group_id,
    format_message,
    matches_job_id,
)


def test_decodes_only_supported_result_and_dlq_events() -> None:
    payload = b'{"event_type":"summary_generated","job_id":"job-1"}'

    assert decode_observed_payload(payload) == {
        "event_type": "summary_generated",
        "job_id": "job-1",
    }
    assert decode_observed_payload(b'{"event_type":"other"}') is None
    assert decode_observed_payload(b"not-json") is None


def test_job_id_filter_allows_only_matching_events() -> None:
    assert matches_job_id({"job_id": "job-1"}, "job-1")
    assert not matches_job_id({"job_id": "job-2"}, "job-1")
    assert not matches_job_id({"event_type": "invalid_material_uploaded"}, "job-1")
    assert matches_job_id({"event_type": "invalid_material_uploaded"}, None)


def test_formats_message_as_pretty_json_with_watcher_group() -> None:
    message = SimpleNamespace(topic="transcript.events", partition=0, offset=12)

    rendered = format_message(message, {"event_type": "transcription_failed"}, "manual-watch")

    assert '"consumer_group": "manual-watch"' in rendered
    assert '"event_type": "transcription_failed"' in rendered


def test_default_group_is_unique_and_dedicated_to_manual_watch() -> None:
    assert default_group_id().startswith("transcript-manual-watch-")
    assert default_group_id() != default_group_id()
