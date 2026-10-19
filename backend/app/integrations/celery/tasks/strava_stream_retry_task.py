"""Delayed retry for Strava per-sample stream ingestion.

Strava fires the activity webhook seconds after upload, before stream
processing on their side has finished — the immediate /streams fetch in the
webhook path then comes back empty and the workout is saved without samples.
The hourly pull can't repair it either: the webhook bumps last_synced_at past
the activity's start time, so the pull window never re-covers the workout.

This task simply re-runs the regular webhook processing flow
(``get_workout_detail_from_api`` + ``process_push_activity``) a few minutes
later, when Strava has finished processing. The flow is idempotent (workout
upsert + skip-if-samples-exist guard) and re-schedules itself with backoff
until samples land or the attempt budget (``len(settings.
strava_stream_retry_countdown_seconds)``) is exhausted.
"""

from logging import getLogger
from uuid import UUID

from celery import shared_task

from app.config import settings
from app.database import DbSession, SessionLocal
from app.schemas.providers.strava import ActivityJSON as StravaActivityJSON
from app.utils.sentry_helpers import log_and_capture_error
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


def max_stream_retry_attempts() -> int:
    """One attempt per configured countdown."""
    return len(settings.strava_stream_retry_countdown_seconds)


@shared_task
def retry_strava_stream_ingest(user_id: str, activity_id: str, attempt: int = 1) -> None:
    """Re-run the webhook processing flow for an activity whose streams weren't ready."""
    try:
        with SessionLocal() as db:
            _retry_ingest(db, UUID(user_id), str(activity_id), attempt)
    except Exception as e:
        # A transient failure (detail fetch, token refresh, DB hiccup) must not
        # end the retry sequence — keep rescheduling until the budget runs out.
        log_and_capture_error(e, logger, "Strava stream retry failed", extra={"activity_id": activity_id})
        if attempt < max_stream_retry_attempts():
            schedule_stream_retry(user_id, str(activity_id), attempt + 1)


def _retry_ingest(db: DbSession, user_uuid: UUID, activity_id: str, attempt: int) -> None:
    # Imported lazily: factory -> strategies -> strava.workouts imports this
    # module for schedule_stream_retry, so a module-level import would cycle.
    from app.services.providers.factory import ProviderFactory

    workouts = ProviderFactory().get_provider("strava").workouts
    activity_data = workouts.get_workout_detail_from_api(db, user_uuid, activity_id)
    if not activity_data:
        log_structured(
            logger,
            "warning",
            "Strava stream retry: no data returned for activity, giving up",
            provider="strava",
            action="stream_retry_no_activity",
            activity_id=activity_id,
        )
        return

    activity = StravaActivityJSON(**activity_data)
    # The regular webhook flow: upserts the workout (idempotent), ingests streams
    # with the skip-if-samples-exist guard, and — when streams are still not
    # ready — schedules the next attempt itself (or logs exhaustion).
    workouts.process_push_activity(db=db, activity=activity, user_id=user_uuid, stream_retry_attempt=attempt + 1)


def schedule_stream_retry(user_id: str, activity_id: str, attempt: int) -> None:
    """Schedule attempt N with its configured backoff."""
    countdowns = settings.strava_stream_retry_countdown_seconds
    countdown = countdowns[min(attempt, len(countdowns)) - 1]
    retry_strava_stream_ingest.apply_async(
        kwargs={"user_id": user_id, "activity_id": activity_id, "attempt": attempt},
        countdown=countdown,
    )
    log_structured(
        logger,
        "info",
        "Strava streams not ready; scheduled delayed ingest",
        provider="strava",
        action="stream_retry_scheduled",
        activity_id=activity_id,
        attempt=attempt,
        countdown_seconds=countdown,
    )
