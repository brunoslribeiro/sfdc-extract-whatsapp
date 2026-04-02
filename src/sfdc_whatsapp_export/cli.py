from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from .auth import OAuthError, token_client_credentials
from .exporter import ExportConfig, export_conversations
from .salesforce_client import SalesforceClient


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SystemExit(f"Datetime invalido: {value}") from exc
    if dt.tzinfo is None:
        raise SystemExit(f"Datetime deve incluir timezone/offset: {value}")
    return dt


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extrai conversas de WhatsApp do Salesforce por janela temporal."
    )
    p.add_argument("--instance-url", default=os.getenv("SF_INSTANCE_URL"), help="URL da instancia Salesforce, ex.: https://org.my.salesforce.com")
    p.add_argument("--access-token", default=os.getenv("SF_ACCESS_TOKEN"), help="Bearer token OAuth do Salesforce")
    p.add_argument("--channel", default="WhatsApp SAS", help="Nome do canal (ChannelName). Usado pelo fluxo legado com MessagingSession")
    p.add_argument("--days", type=int, default=30, help="Janela relativa em dias quando start/end nao forem informados")
    p.add_argument("--start-datetime", default=None, help="Inicio da janela em ISO-8601 com timezone, ex.: 2026-04-01T00:00:00Z")
    p.add_argument("--end-datetime", default=None, help="Fim da janela em ISO-8601 com timezone, ex.: 2026-04-01T01:00:00Z")
    p.add_argument("--window-size-minutes", type=int, default=None, help="Quebra a janela total em sub-janelas deste tamanho")
    p.add_argument("--api-version", default="62.0", help="Versao da API (ex.: 62.0)")
    p.add_argument("--entries-api", choices=["conversation-data", "connect"], default="conversation-data", help="API usada para baixar entries")
    p.add_argument("--conversation-api-base-url", default=os.getenv("SF_CONVERSATION_API_BASE_URL", "https://api.salesforce.com/platform/engagement/v1.0"), help="Base URL da Conversation Data API")
    p.add_argument("--no-legacy-fallback", action="store_true", help="Desabilita fallback automatico para Connect ao usar conversation-data")
    p.add_argument("--max-requests-per-minute", type=int, default=int(os.getenv("SF_MAX_REQUESTS_PER_MINUTE", "90")), help="Limite de requisicoes por minuto para respeitar rate limit")
    p.add_argument("--out", default="output", help="Diretorio de saida")
    p.add_argument("--record-limit", type=int, default=None, help="Quantidade maxima de entries por chamada (se suportado)")
    p.add_argument("--page-size", type=int, default=None, help=argparse.SUPPRESS)
    p.add_argument("--ndjson", action="store_true", help="Grava arquivo all_conversations.ndjson agregado")
    p.add_argument("--entries-csv", default=None, help="Grava CSV agregado por entry")
    p.add_argument("--with-session-enrichment", action="store_true", help="Na Conversation Data API, consulta MessagingSession apenas para enriquecer nome/numero do contato")
    p.add_argument("--dump-sessions", default=None, help="Caminho CSV para exportar a lista de MessagingSessions consultadas para enriquecimento")
    p.add_argument("--log-root", default=None, help="Diretorio onde serao criadas as pastas de log por execucao (default: <out>/logs)")
    p.add_argument("--state-dir", default=None, help="Diretorio do estado consolidado e dos stages por execucao (default: <out>/state)")
    p.add_argument("--no-logs", action="store_true", help="Desabilita criacao de pasta de log por execucao")
    p.add_argument("--created-only", action="store_true", help="Usa apenas CreatedDate no filtro legado de MessagingSession")
    p.add_argument("--use-systemmodstamp", action="store_true", help="Usa SystemModstamp em vez de LastModifiedDate no filtro legado de MessagingSession")

    p.add_argument("--auth-url", default=os.getenv("SF_AUTH_URL"), help="URL do token OAuth (ex.: https://mydomain.my.salesforce.com/services/oauth2/token)")
    p.add_argument("--client-id", default=os.getenv("SF_CLIENT_ID"), help="Client ID do Connected App")
    p.add_argument("--client-secret", default=os.getenv("SF_CLIENT_SECRET"), help="Client Secret do Connected App")
    p.add_argument("--scope", default=os.getenv("SF_SCOPE"), help="Scopes opcionais separados por espaco")
    p.add_argument("--audience", default=os.getenv("SF_AUDIENCE"), help="Audience opcional (se requerido)")
    return p.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    access_token = args.access_token
    instance_url = args.instance_url

    if not access_token:
        if not args.client_id or not args.client_secret or not args.auth_url:
            raise SystemExit(
                "Informe --access-token ou configure Client Credentials: --auth-url, --client-id e --client-secret (ou via .env)"
            )
        try:
            oauth = token_client_credentials(
                auth_url=args.auth_url,
                client_id=args.client_id,
                client_secret=args.client_secret,
                scope=args.scope,
                audience=args.audience,
            )
            access_token = oauth["access_token"]
            if not instance_url:
                if "/services/oauth2/token" in args.auth_url:
                    instance_url = args.auth_url.split("/services/oauth2/token", 1)[0]
                else:
                    instance_url = args.auth_url.rstrip("/")
        except OAuthError as e:
            raise SystemExit(f"Falha ao obter token OAuth: {e}")

    if not instance_url:
        raise SystemExit("--instance-url, env SF_INSTANCE_URL, ou derivacao a partir de --auth-url e obrigatorio")

    out_dir = Path(args.out)
    start_datetime = parse_iso_datetime(args.start_datetime)
    end_datetime = parse_iso_datetime(args.end_datetime)

    client = SalesforceClient(
        instance_url=instance_url,
        access_token=access_token,
        api_version=args.api_version,
        conversation_api_base_url=args.conversation_api_base_url,
        entries_api=args.entries_api,
        legacy_fallback=(not args.no_legacy_fallback),
        max_requests_per_minute=args.max_requests_per_minute,
    )

    config = ExportConfig(
        channel=args.channel,
        days=args.days,
        out_dir=out_dir,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        window_size_minutes=args.window_size_minutes,
        api_version=args.api_version,
        entries_api=args.entries_api,
        record_limit=(args.record_limit if args.record_limit is not None else args.page_size),
        write_ndjson=args.ndjson,
        entries_csv=Path(args.entries_csv) if args.entries_csv else None,
        dump_sessions_csv=Path(args.dump_sessions) if args.dump_sessions else None,
        log_root=Path(args.log_root) if args.log_root else None,
        state_dir=Path(args.state_dir) if args.state_dir else None,
        enable_logs=(not args.no_logs),
        include_updated=(not args.created_only),
        use_systemmodstamp=args.use_systemmodstamp,
        enrich_messaging_sessions=(args.with_session_enrichment or bool(args.dump_sessions)),
    )

    export_conversations(client, config)


if __name__ == "__main__":
    main()
