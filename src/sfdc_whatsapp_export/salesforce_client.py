from __future__ import annotations

import time
import typing as t

import requests


class SalesforceClient:
    def __init__(
        self,
        instance_url: str,
        access_token: str,
        api_version: str = "62.0",
        conversation_api_base_url: str = "https://api.salesforce.com/platform/engagement/v1.0",
        entries_api: str = "conversation-data",
        legacy_fallback: bool = True,
        max_requests_per_minute: t.Optional[int] = None,
        session: t.Optional[requests.Session] = None,
        timeout: int = 30,
    ) -> None:
        if not instance_url or not access_token:
            raise ValueError("instance_url e access_token são obrigatórios")
        self.instance_url = instance_url.rstrip("/")
        self.access_token = access_token
        self.api_version = api_version
        self.conversation_api_base_url = conversation_api_base_url.rstrip("/")
        self.entries_api = entries_api
        self.legacy_fallback = legacy_fallback
        self.timeout = timeout
        self.max_requests_per_minute = max_requests_per_minute
        self._min_request_interval = (
            60.0 / float(max_requests_per_minute) if max_requests_per_minute and max_requests_per_minute > 0 else 0.0
        )
        self._last_request_started_at = 0.0
        self._session = session or requests.Session()

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, params: dict | None = None) -> dict:
        # path pode ser absoluto (começando com /) ou relativo ao instance_url
        if path.startswith("http://") or path.startswith("https://"):
            url = path
        elif path.startswith("/"):
            url = f"{self.instance_url}{path}"
        else:
            url = f"{self.instance_url}/{path}"
        attempts = 0
        while True:
            self._respect_rate_limit()
            resp = self._session.request(method, url, headers=self._headers(), params=params, timeout=self.timeout)
            if resp.status_code != 429:
                break
            attempts += 1
            if attempts >= 5:
                raise requests.HTTPError(f"HTTP 429 for {url}: {resp.text}")
            self._sleep_after_rate_limit(resp, attempts)
        if resp.status_code >= 400:
            raise requests.HTTPError(f"HTTP {resp.status_code} for {url}: {resp.text}")
        if not resp.text:
            return {}
        return resp.json()

    def _respect_rate_limit(self) -> None:
        if self._min_request_interval <= 0:
            return
        now = time.monotonic()
        wait_for = self._min_request_interval - (now - self._last_request_started_at)
        if wait_for > 0:
            time.sleep(wait_for)
        self._last_request_started_at = time.monotonic()

    def _sleep_after_rate_limit(self, response: requests.Response, attempts: int) -> None:
        retry_after_header = response.headers.get("Retry-After")
        retry_after = 0.0
        if retry_after_header:
            try:
                retry_after = float(retry_after_header)
            except ValueError:
                retry_after = 0.0
        backoff = max(self._min_request_interval, retry_after, min(30.0, float(attempts)))
        time.sleep(backoff)

    def soql(self, query: str) -> list[dict]:
        # Paginação com nextRecordsUrl
        records: list[dict] = []
        endpoint = f"/services/data/v{self.api_version}/query"
        params = {"q": query}
        data = self._request("GET", endpoint, params=params)
        records.extend(data.get("records", []))
        next_url = data.get("nextRecordsUrl")
        while next_url:
            data = self._request("GET", next_url)
            records.extend(data.get("records", []))
            next_url = data.get("nextRecordsUrl")
            # breve backoff amistoso
            time.sleep(0.1)
        return records

    def get_conversation_entries_connect(
        self,
        conversation_identifier: str,
        record_limit: t.Optional[int] = None,
        start_timestamp: t.Optional[int] = None,
        end_timestamp: t.Optional[int] = None,
        page_token: t.Optional[str] = None,
    ) -> dict:
        # Endpoint: /services/data/v{api_version}/connect/conversation/{id}/entries
        params: dict[str, t.Any] = {}
        if record_limit is not None:
            params["recordLimit"] = int(record_limit)
        if start_timestamp is not None:
            params["startTimestamp"] = int(start_timestamp)
        if end_timestamp is not None:
            params["endTimestamp"] = int(end_timestamp)
        if page_token:
            params["pageToken"] = page_token
        path = f"/services/data/v{self.api_version}/connect/conversation/{conversation_identifier}/entries"
        return self._request("GET", path, params=params)

    def get_conversation_entries_conversation_data(
        self,
        conversation_identifier: str,
        record_limit: t.Optional[int] = None,
        start_timestamp: t.Optional[int] = None,
        end_timestamp: t.Optional[int] = None,
        page_token: t.Optional[str] = None,
    ) -> dict:
        params: dict[str, t.Any] = {"conversationIdentifier": conversation_identifier}
        if record_limit is not None:
            params["recordLimit"] = int(record_limit)
        if start_timestamp is not None:
            params["startTimestamp"] = int(start_timestamp)
        if end_timestamp is not None:
            params["endTimestamp"] = int(end_timestamp)
        if page_token:
            params["pageToken"] = page_token
        path = f"{self.conversation_api_base_url}/conversation-entries"
        return self._request("GET", path, params=params)

    def get_conversation_entries(
        self,
        conversation_identifier: str,
        record_limit: t.Optional[int] = None,
        start_timestamp: t.Optional[int] = None,
        end_timestamp: t.Optional[int] = None,
        page_token: t.Optional[str] = None,
    ) -> dict:
        if self.entries_api == "connect":
            return self.get_conversation_entries_connect(
                conversation_identifier,
                record_limit=record_limit,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                page_token=page_token,
            )

        try:
            return self.get_conversation_entries_conversation_data(
                conversation_identifier,
                record_limit=record_limit,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                page_token=page_token,
            )
        except requests.HTTPError:
            if not self.legacy_fallback or page_token:
                raise
            return self.get_conversation_entries_connect(
                conversation_identifier,
                record_limit=record_limit,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                page_token=page_token,
            )

    def get_conversation_entries_all(
        self,
        conversation_identifier: str,
        record_limit: t.Optional[int] = None,
        start_timestamp: t.Optional[int] = None,
        end_timestamp: t.Optional[int] = None,
    ) -> dict:
        # Consolida todas as páginas se a org retornar paginação via `nextPageToken`
        first = self.get_conversation_entries(
            conversation_identifier,
            record_limit=record_limit,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        # Se não houver paginação conhecida, apenas retorna o primeiro payload
        if "nextPageToken" not in first:
            return first

        entries = list(first.get("conversationEntries", []))
        current = first
        visited = 0
        while current.get("nextPageToken"):
            token = current.get("nextPageToken")
            current = self.get_conversation_entries(
                conversation_identifier,
                record_limit=record_limit,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                page_token=token,
            )
            entries.extend(current.get("conversationEntries", []))
            visited += 1
            time.sleep(0.1)

        # Retorna estrutura similar ao payload original, mas com entries consolidados
        combined = dict(first)
        combined["conversationEntries"] = entries
        # Remove token residual para indicar fim
        combined.pop("nextPageToken", None)
        return combined
