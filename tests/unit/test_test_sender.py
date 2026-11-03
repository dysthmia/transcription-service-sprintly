from test_build.sender import (
    EventIdentifiers,
    build_s3_event,
    build_youtube_event,
    generated_identifiers,
)


def test_generated_identifiers_are_unique_and_prefixed() -> None:
    first = generated_identifiers()
    second = generated_identifiers()

    assert first != second
    assert first.event_id.startswith("evt_")
    assert first.job_id.startswith("job_")
    assert first.correlation_id.startswith("corr_")
    assert first.material_id.startswith("material_")


def test_build_s3_event_matches_material_uploaded_schema() -> None:
    identifiers = EventIdentifiers("evt-1", "job-1", "corr-1", "material-1", "user-1")

    event = build_s3_event(identifiers, "learning-materials", "manual/sample.txt")

    assert event.model_dump() == {
        "event_id": "evt-1",
        "event_type": "material_uploaded",
        "schema_version": 1,
        "correlation_id": "corr-1",
        "job_id": "job-1",
        "user_id": "user-1",
        "material_id": "material-1",
        "source": {"type": "s3", "bucket": "learning-materials", "key": "manual/sample.txt"},
    }


def test_build_youtube_event_validates_the_current_schema() -> None:
    identifiers = EventIdentifiers("evt-1", "job-1", "corr-1", "material-1", "user-1")

    event = build_youtube_event(identifiers, "https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    assert event.source.type == "youtube"
