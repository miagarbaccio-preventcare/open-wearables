"""Tests for SDK sync endpoints authentication."""

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import httpx
import pytest
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from app.services.sdk_token_service import create_sdk_user_token
from tests.factories import ApiKeyFactory, DeveloperFactory
from tests.utils import developer_auth_headers


@pytest.fixture(autouse=True)
def mock_celery_tasks() -> Generator[MagicMock, None, None]:
    """Mock Celery tasks to prevent actual task execution during tests."""
    with patch("app.api.routes.v1.sdk_sync.process_sdk_upload") as mock:
        mock.delay.return_value = None
        yield mock


class TestSDKSyncWithSDKToken:
    """Tests for SDK sync endpoints with SDK token authentication."""

    def test_apple_health_sdk_endpoint_accepts_sdk_token(
        self, client: TestClient, db: Session, api_v1_prefix: str
    ) -> None:
        """SDK token should be accepted for apple-health-sdk sync."""
        user_id = "123e4567-e89b-12d3-a456-426614174000"
        token = create_sdk_user_token("app_123", user_id)

        response = client.post(
            f"{api_v1_prefix}/sdk/users/{user_id}/sync/",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "data": {
                    "provider": "apple",
                    "sdkVersion": "1.0.0",
                    "syncTimestamp": "2021-01-01T00:00:00Z",
                    "workouts": [],
                    "records": [],
                }
            },
        )

        # Should not be 401 (auth should pass)
        # May be 400/422 if data format is wrong, but auth should pass
        assert response.status_code != 401

    def test_apple_health_sdk_still_accepts_api_key(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """API key should still work for apple-health-sdk (backwards compatibility)."""
        api_key = ApiKeyFactory()
        user_id = "123e4567-e89b-12d3-a456-426614174000"

        response = client.post(
            f"{api_v1_prefix}/sdk/users/{user_id}/sync/",
            headers={"X-Open-Wearables-API-Key": api_key.id},
            json={
                "data": {
                    "provider": "apple",
                    "sdkVersion": "1.0.0",
                    "syncTimestamp": "2021-01-01T00:00:00Z",
                    "workouts": [],
                    "records": [],
                }
            },
        )

        # Should not be 401
        assert response.status_code != 401

    def test_no_auth_returns_401(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """No authentication should return 401."""
        response = client.post(
            f"{api_v1_prefix}/sdk/users/user_456/sync/",
            json={
                "data": {
                    "provider": "apple",
                    "sdkVersion": "1.0.0",
                    "syncTimestamp": "2021-01-01T00:00:00Z",
                    "workouts": [],
                    "records": [],
                }
            },
        )

        assert response.status_code == 401


def _resting_hr_records(count: int) -> list[dict]:
    return [
        {
            "type": "HKQuantityTypeIdentifierRestingHeartRate",
            "startDate": f"2026-06-{day:02d}T07:00:00Z",
            "endDate": f"2026-06-{day:02d}T07:00:00Z",
            "value": 58,
            "unit": "count/min",
        }
        for day in range(1, count + 1)
    ]


class TestSDKSyncInlineImport:
    """Small batches import in the request; the rest are queued."""

    def _post(self, client: TestClient, api_v1_prefix: str, records: list[dict]) -> httpx.Response:
        api_key = ApiKeyFactory()
        user_id = "123e4567-e89b-12d3-a456-426614174000"
        return client.post(
            f"{api_v1_prefix}/sdk/users/{user_id}/sync/",
            headers={"X-Open-Wearables-API-Key": api_key.id},
            json={
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-06-15T12:00:00Z",
                "data": {"records": records, "sleep": [], "workouts": []},
            },
        )

    def test_a_small_batch_imports_without_the_queue(
        self, client: TestClient, db: Session, api_v1_prefix: str, mock_celery_tasks: MagicMock
    ) -> None:
        with patch("app.api.routes.v1.sdk_sync.store_raw_payload"):
            response = self._post(client, api_v1_prefix, _resting_hr_records(3))

        assert response.status_code == 202
        assert response.json()["response"] == "Import completed"
        mock_celery_tasks.assert_called_once()
        assert mock_celery_tasks.call_args.kwargs["provider"] == "apple"
        mock_celery_tasks.delay.assert_not_called()

    def test_a_failed_inline_import_is_queued_instead(
        self, client: TestClient, db: Session, api_v1_prefix: str, mock_celery_tasks: MagicMock
    ) -> None:
        mock_celery_tasks.side_effect = RuntimeError("db down")
        with patch("app.api.routes.v1.sdk_sync.store_raw_payload"):
            response = self._post(client, api_v1_prefix, _resting_hr_records(3))

        assert response.status_code == 202
        assert response.json()["response"] == "Import task queued successfully"
        mock_celery_tasks.delay.assert_called_once()
        assert mock_celery_tasks.delay.call_args.kwargs == mock_celery_tasks.call_args.kwargs

    def test_a_batch_over_the_limit_is_queued(
        self, client: TestClient, db: Session, api_v1_prefix: str, mock_celery_tasks: MagicMock
    ) -> None:
        with (
            patch("app.api.routes.v1.sdk_sync.store_raw_payload"),
            patch("app.api.routes.v1.sdk_sync.settings.sdk_inline_max_items", 2),
        ):
            response = self._post(client, api_v1_prefix, _resting_hr_records(3))

        assert response.status_code == 202
        mock_celery_tasks.assert_not_called()
        mock_celery_tasks.delay.assert_called_once()

    def test_an_empty_batch_is_queued(
        self, client: TestClient, db: Session, api_v1_prefix: str, mock_celery_tasks: MagicMock
    ) -> None:
        with patch("app.api.routes.v1.sdk_sync.store_raw_payload"):
            response = self._post(client, api_v1_prefix, [])

        assert response.status_code == 202
        mock_celery_tasks.assert_not_called()
        mock_celery_tasks.delay.assert_called_once()


class TestSDKTokenBlockedElsewhere:
    """Tests for SDK tokens being rejected on non-SDK endpoints."""

    def test_sdk_token_rejected_on_auth_me(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """SDK token should be rejected on /auth/me."""
        token = create_sdk_user_token("app_123", "user_456")

        response = client.get(
            f"{api_v1_prefix}/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401

    def test_sdk_token_rejected_on_dashboard(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """SDK token should be rejected on dashboard endpoints."""
        token = create_sdk_user_token("app_123", "user_456")

        response = client.get(
            f"{api_v1_prefix}/dashboard/stats",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401

    def test_sdk_token_rejected_on_applications(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """SDK token should be rejected on applications endpoints."""
        token = create_sdk_user_token("app_123", "user_456")

        response = client.get(
            f"{api_v1_prefix}/applications",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401

    def test_sdk_token_rejected_on_api_keys(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """SDK token should be rejected on API keys endpoints."""
        token = create_sdk_user_token("app_123", "user_456")

        response = client.get(
            f"{api_v1_prefix}/developer/api-keys",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401

    def test_sdk_token_rejected_on_users_list(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """SDK token should be rejected on users list endpoint."""
        token = create_sdk_user_token("app_123", "user_456")

        response = client.get(
            f"{api_v1_prefix}/users",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401

    def test_developer_token_still_works_on_auth_me(self, client: TestClient, db: Session, api_v1_prefix: str) -> None:
        """Developer token should still work on /auth/me."""
        developer = DeveloperFactory()
        headers = developer_auth_headers(developer.id)

        response = client.get(
            f"{api_v1_prefix}/auth/me",
            headers=headers,
        )

        assert response.status_code == 200
