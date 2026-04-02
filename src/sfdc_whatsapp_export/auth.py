from __future__ import annotations

import typing as t
import requests


class OAuthError(RuntimeError):
    pass


def token_client_credentials(
    auth_url: str,
    client_id: str,
    client_secret: str,
    scope: str | None = None,
    audience: str | None = None,
    timeout: int = 30,
) -> dict:
    if not auth_url.endswith("/services/oauth2/token"):
        # Permite tanto a URL completa quanto o domínio base
        if auth_url.rstrip("/").endswith("/services/oauth2"):
            auth_url = auth_url.rstrip("/") + "/token"
        elif auth_url.rstrip("/").endswith("/oauth2"):
            auth_url = auth_url.rstrip("/") + "/token"
        elif "/services/oauth2/token" not in auth_url:
            # tenta normalizar para domínio base
            auth_url = auth_url.rstrip("/") + "/services/oauth2/token"

    data: dict[str, t.Any] = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        data["scope"] = scope
    if audience:
        data["audience"] = audience

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(auth_url, data=data, headers=headers, timeout=timeout)
    if resp.status_code >= 400:
        raise OAuthError(f"OAuth token falhou ({resp.status_code}): {resp.text}")
    try:
        payload = resp.json()
    except Exception as e:
        raise OAuthError(f"Resposta OAuth inválida: {e}; body={resp.text[:200]}...")
    if "access_token" not in payload:
        raise OAuthError(f"Token ausente na resposta OAuth: {payload}")
    return payload

