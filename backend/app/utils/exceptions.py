from typing import ClassVar
from uuid import UUID

from fastapi import status
from psycopg import Error as PsycopgError
from psycopg.errors import ForeignKeyViolation, UniqueViolation
from psycopg.errors import IntegrityError as PsycopgIntegrityError
from sqlalchemy.exc import IntegrityError

# ORM writes raise SQLAlchemy's wrapper; writes on the raw psycopg connection (COPY) raise psycopg's own
INTEGRITY_ERRORS = (IntegrityError, PsycopgIntegrityError)


class AppError(Exception):
    """Base of the domain errors raised by services and repositories.

    Raise a subclass instead of ``HTTPException``: the global handler in ``app.main`` turns
    ``status_code`` and ``detail`` into the response, and Celery tasks can catch it by type.
    """

    status_code: ClassVar[int] = status.HTTP_500_INTERNAL_SERVER_ERROR
    headers: ClassVar[dict[str, str] | None] = None

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)

    @property
    def is_client_error(self) -> bool:
        return 400 <= self.status_code < 500


class InvalidInputError(AppError):
    status_code = status.HTTP_400_BAD_REQUEST


class AuthenticationError(AppError):
    status_code = status.HTTP_401_UNAUTHORIZED
    headers = {"WWW-Authenticate": "Bearer"}


class WebhookVerificationError(AuthenticationError):
    """A provider callback failed verification (signature, token): no bearer scheme to advertise."""

    headers = None


class PermissionDeniedError(AppError):
    status_code = status.HTTP_403_FORBIDDEN


class NotFoundError(AppError):
    status_code = status.HTTP_404_NOT_FOUND


class ConflictError(AppError):
    status_code = status.HTTP_409_CONFLICT


class ResourceNotFoundError(NotFoundError):
    def __init__(self, entity_name: str, entity_id: int | UUID | None = None):
        self.entity_name = entity_name
        if entity_id:
            super().__init__(f"{entity_name.capitalize()} with ID: {entity_id} not found.")
        else:
            super().__init__(f"{entity_name.capitalize()} not found.")


class UnsupportedProviderError(InvalidInputError):
    def __init__(self, provider: str, operation: str = "this operation"):
        super().__init__(f"Provider '{provider}' does not support {operation}.")


class InvalidCursorError(InvalidInputError):
    def __init__(self, cursor: str):
        super().__init__(f"Invalid cursor format: '{cursor}'. Expected 'timestamp|id'.")


class DatetimeParseError(InvalidInputError, ValueError):
    def __init__(self, value: str):
        super().__init__(f"Invalid datetime format: '{value}'. Expected ISO 8601 format or Unix timestamp.")


class UpstreamError(AppError):
    """A service we call failed: a provider API, object storage.

    Not our bug, so unlike other 5xx errors the client gets this status and ``detail``
    instead of a bare 500, and it is logged as a warning rather than reported to Sentry.
    """

    status_code = status.HTTP_502_BAD_GATEWAY


class ServiceUnavailableError(UpstreamError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE


def provider_label(provider: str) -> str:
    return provider.capitalize()


class ProviderError(UpstreamError):
    """A provider API call failed.

    ``upstream_status`` is the provider's HTTP status, None when no response came back.
    ``upstream_body`` is what the provider answered: for logs and for callers that branch
    on it, never for ``detail``, which the client sees.
    """

    def __init__(
        self,
        provider: str,
        detail: str | None = None,
        *,
        upstream_status: int | None = None,
        upstream_body: str | None = None,
    ) -> None:
        self.provider = provider
        self.upstream_status = upstream_status
        self.upstream_body = upstream_body
        if detail is None:
            label = provider_label(provider)
            detail = (
                f"{label} API error (status {upstream_status})"
                if upstream_status is not None
                else f"{label} API request failed"
            )
        super().__init__(detail)


class ProviderRateLimitError(ProviderError):
    """The provider kept throttling us after the client's own backoff."""

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE


class ProviderConnectionError(AppError):
    """The user's connection to the provider can't be used: no request to the provider
    succeeds, and no retry helps, until the user connects again."""

    def __init__(self, provider: str, detail: str) -> None:
        self.provider = provider
        super().__init__(detail)


class ProviderNotConnectedError(ProviderConnectionError, NotFoundError):
    def __init__(self, provider: str) -> None:
        super().__init__(provider, f"User is not connected to {provider_label(provider)}.")


class ProviderTokenUnavailableError(ProviderConnectionError, InvalidInputError):
    """The connection has no API token, e.g. an SDK-based provider."""

    def __init__(self, provider: str) -> None:
        super().__init__(provider, f"No {provider_label(provider)} API access token for this user.")


class ProviderReauthorizationRequiredError(ProviderConnectionError, ConflictError):
    """The provider grant is gone: expired with no refresh token, or the provider rejected it."""

    def __init__(self, provider: str) -> None:
        super().__init__(
            provider, f"{provider_label(provider)} authorization expired or was revoked; the user must reconnect."
        )


def integrity_violation(exc: IntegrityError | PsycopgIntegrityError) -> BaseException | None:
    """The driver error behind ``exc``, whether SQLAlchemy wrapped it or not."""
    return exc.orig if isinstance(exc, IntegrityError) else exc


def describe_integrity_error(exc: IntegrityError | PsycopgIntegrityError) -> tuple[str, str | None]:
    """The violation type and constraint name - never the message, which carries the offending values."""
    violation = integrity_violation(exc)
    constraint = violation.diag.constraint_name if isinstance(violation, PsycopgError) else None
    return type(violation).__name__, constraint


def app_error_from_integrity_error(exc: IntegrityError | PsycopgIntegrityError) -> AppError | None:
    """The client-caused constraint violation behind ``exc``, or None for one that is our bug."""
    orig = integrity_violation(exc)
    if isinstance(orig, UniqueViolation):
        entity = (orig.diag.table_name or "resource").replace("_", " ").capitalize()
        return ConflictError(f"{entity} already exists.")
    if isinstance(orig, ForeignKeyViolation):
        # FKs cascade or null on delete, except to series_type_definition (RESTRICT), whose
        # seeded rows no endpoint deletes - so this is a reference to a missing row
        return InvalidInputError("Referenced resource does not exist.")
    return None


def provider_error_from_response(
    provider: str, upstream_status: int | None, upstream_body: str | None = None
) -> ProviderError | ProviderReauthorizationRequiredError:
    """The error for a failed provider response: a rejected token means the user must
    reconnect, a throttle we couldn't wait out is a 503, anything else a 502."""
    if upstream_status == 401:
        return ProviderReauthorizationRequiredError(provider)
    if upstream_status == 429:
        return ProviderRateLimitError(provider, upstream_status=upstream_status, upstream_body=upstream_body)
    return ProviderError(provider, upstream_status=upstream_status, upstream_body=upstream_body)
