from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from .salesforce_client import SalesforceClient


@dataclass
class ExportConfig:
    channel: str
    days: int
    out_dir: Path
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    window_size_minutes: Optional[int] = None
    api_version: str = "62.0"
    entries_api: str = "conversation-data"
    record_limit: Optional[int] = None
    write_ndjson: bool = False
    entries_csv: Optional[Path] = None
    dump_sessions_csv: Optional[Path] = None
    log_root: Optional[Path] = None
    state_dir: Optional[Path] = None
    enable_logs: bool = True
    include_updated: bool = True
    use_systemmodstamp: bool = False
    enrich_messaging_sessions: bool = False


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_state(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_managed_output_path(path: Optional[Path], managed_dir: Path) -> Optional[Path]:
    if path is None:
        return None
    if path.is_absolute():
        return path
    if path.parent == Path("."):
        return managed_dir / path.name
    return path


def conversation_is_unchanged(current_last_modified: Optional[str], state_row: Optional[dict]) -> bool:
    if not current_last_modified or not state_row:
        return False
    saved_last_modified = state_row.get("lastModifiedDate")
    if not saved_last_modified:
        return False
    current_dt = parse_sf_dt(current_last_modified)
    saved_dt = parse_sf_dt(saved_last_modified)
    if current_dt and saved_dt:
        return current_dt <= saved_dt
    return current_last_modified == saved_last_modified


def extract_message_text(entry: dict) -> Optional[str]:
    message_text = entry.get("messageText")
    if message_text:
        return message_text

    payload = entry.get("entryPayload") or {}
    abstract_message = payload.get("abstractMessage") or {}
    static_content = abstract_message.get("staticContent") or {}
    if static_content.get("text"):
        return static_content.get("text")

    attachments = static_content.get("attachments") or []
    if attachments:
        names = [a.get("name") for a in attachments if isinstance(a, dict) and a.get("name")]
        if names:
            return ", ".join(names)
        return "[attachment]"

    external_template = static_content.get("externalTemplate") or {}
    if external_template.get("name"):
        return f"[template] {external_template.get('name')}"

    return None


def extract_sort_timestamp(entry: dict) -> int:
    return (
        entry.get("transcriptedTimestamp")
        or entry.get("serverReceivedTimestamp")
        or entry.get("clientTimestamp")
        or 0
    )


def sanitize_csv_text(value: object) -> object:
    if value is None or not isinstance(value, str):
        return value
    return " ".join(value.replace("\r", "\n").splitlines()).strip()


def json_dumps_single_line(value: object) -> str:
    return sanitize_csv_text(json.dumps(value, ensure_ascii=False)) or ""


def timestamp_to_utc_iso(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    # Salesforce mistura segundos e milissegundos em campos diferentes.
    if ts >= 10_000_000_000:
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
    else:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def flatten_entry_row(conversation_id: str, entry: dict) -> dict[str, object]:
    sender = entry.get("sender") or {}
    recipients = entry.get("recipients") or {}
    payload = entry.get("entryPayload") or {}
    abstract_message = payload.get("abstractMessage") or {}
    static_content = abstract_message.get("staticContent") or {}
    external_template = static_content.get("externalTemplate") or {}
    attachments = static_content.get("attachments") or []
    client_timestamp = entry.get("clientTimestamp")
    server_received_timestamp = entry.get("serverReceivedTimestamp")
    transcripted_timestamp = entry.get("transcriptedTimestamp")
    sort_timestamp = extract_sort_timestamp(entry)

    return {
        "conversationId": conversation_id,
        "identifier": entry.get("identifier"),
        "entryType": entry.get("entryType"),
        "messageText": sanitize_csv_text(extract_message_text(entry)),
        "messageType": sanitize_csv_text(abstract_message.get("messageType")),
        "messageReason": sanitize_csv_text(payload.get("messageReason")),
        "formatType": sanitize_csv_text(static_content.get("formatType")),
        "templateName": sanitize_csv_text(external_template.get("name")),
        "attachmentCount": len(attachments) if isinstance(attachments, list) else 0,
        "senderRole": sanitize_csv_text(sender.get("role")),
        "senderAppType": sanitize_csv_text(sender.get("appType")),
        "senderSubject": sanitize_csv_text(sender.get("subject")),
        "clientTimestamp": client_timestamp,
        "clientTimestampUtc": timestamp_to_utc_iso(client_timestamp),
        "serverReceivedTimestamp": server_received_timestamp,
        "serverReceivedTimestampUtc": timestamp_to_utc_iso(server_received_timestamp),
        "transcriptedTimestamp": transcripted_timestamp,
        "transcriptedTimestampUtc": timestamp_to_utc_iso(transcripted_timestamp),
        "sortTimestamp": sort_timestamp,
        "sortTimestampUtc": timestamp_to_utc_iso(sort_timestamp),
        "relatedRecordsJson": json_dumps_single_line(entry.get("relatedRecords")),
        "recipientsJson": json_dumps_single_line(recipients),
        "rawEntryJson": json_dumps_single_line(entry),
    }


def parse_sf_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if value[-5] in ["+", "-"] and value[-3] != ":":
            value = value[:-2] + ":" + value[-2:]
        return datetime.fromisoformat(value)
    except Exception:
        return None


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_soql_datetime(dt: datetime) -> str:
    return ensure_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_conversation_query(window_start: datetime, window_end: datetime) -> str:
    start_str = format_soql_datetime(window_start)
    end_str = format_soql_datetime(window_end)
    return (
        "SELECT Id, StartTime, EndTime, LastModifiedDate, ConversationIdentifier "
        f"FROM Conversation WHERE LastModifiedDate >= {start_str} AND LastModifiedDate < {end_str}"
    )


def build_messaging_session_query(
    channel: str,
    window_start: datetime,
    window_end: datetime,
    include_updated: bool,
    use_systemmodstamp: bool,
) -> str:
    safe_channel = channel.replace("'", "\\'")
    date_field = "SystemModstamp" if use_systemmodstamp else "LastModifiedDate"
    start_str = format_soql_datetime(window_start)
    end_str = format_soql_datetime(window_end)
    select_fields = (
        "Id, ChannelName, Conversation.ConversationIdentifier, CreatedDate, "
        f"{date_field}, MessagingEndUser.Name, MessagingEndUser.MessagingPlatformKey"
    )
    if include_updated:
        where = (
            f"ChannelName = '{safe_channel}' AND "
            f"((CreatedDate >= {start_str} AND CreatedDate < {end_str}) "
            f"OR ({date_field} >= {start_str} AND {date_field} < {end_str}))"
        )
    else:
        where = (
            f"ChannelName = '{safe_channel}' AND "
            f"(CreatedDate >= {start_str} AND CreatedDate < {end_str})"
        )
    return f"SELECT {select_fields} FROM MessagingSession WHERE {where}"


def build_sessions_by_identifier_query(identifiers: list[str]) -> str:
    quoted = ",".join("'" + cid.replace("'", "\\'") + "'" for cid in identifiers)
    return (
        "SELECT Id, ChannelName, Conversation.ConversationIdentifier, CreatedDate, LastModifiedDate, "
        "MessagingEndUser.Name, MessagingEndUser.MessagingPlatformKey "
        f"FROM MessagingSession WHERE Conversation.ConversationIdentifier IN ({quoted})"
    )


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i:i + size] for i in range(0, len(values), size)]


def compute_execution_window(config: ExportConfig) -> tuple[datetime, datetime]:
    if config.start_datetime and config.end_datetime:
        start_dt = ensure_utc(config.start_datetime)
        end_dt = ensure_utc(config.end_datetime)
    elif config.start_datetime or config.end_datetime:
        raise ValueError("start_datetime e end_datetime devem ser informados juntos")
    else:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=config.days)
    if start_dt >= end_dt:
        raise ValueError("start_datetime deve ser menor que end_datetime")
    return start_dt, end_dt


def split_windows(window_start: datetime, window_end: datetime, window_size_minutes: Optional[int]) -> list[tuple[datetime, datetime]]:
    if not window_size_minutes or window_size_minutes <= 0:
        return [(window_start, window_end)]
    windows: list[tuple[datetime, datetime]] = []
    current = window_start
    delta = timedelta(minutes=window_size_minutes)
    while current < window_end:
        next_end = min(current + delta, window_end)
        windows.append((current, next_end))
        current = next_end
    return windows


def export_conversations(client: SalesforceClient, config: ExportConfig) -> dict:
    import csv

    ensure_dir(config.out_dir)
    json_root_dir = config.out_dir / "json"
    csv_root_dir = config.out_dir / "csv"
    ensure_dir(json_root_dir)
    ensure_dir(csv_root_dir)
    window_start, window_end = compute_execution_window(config)
    start_timestamp_ms = int(window_start.timestamp() * 1000)
    end_timestamp_ms = int(window_end.timestamp() * 1000)
    windows = split_windows(window_start, window_end, config.window_size_minutes)
    run_id = "run_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    state_root = Path(config.state_dir or (config.out_dir / "state"))
    ensure_dir(state_root)
    latest_state_path = state_root / "latest_state.json"
    run_state_dir = state_root / run_id
    ensure_dir(run_state_dir)
    json_out_dir = json_root_dir / run_id
    csv_out_dir = csv_root_dir / run_id
    ensure_dir(json_out_dir)
    ensure_dir(csv_out_dir)
    conversations_state = load_state(latest_state_path)
    entries_csv_path = resolve_managed_output_path(config.entries_csv, csv_out_dir)
    dump_sessions_csv_path = resolve_managed_output_path(config.dump_sessions_csv, csv_out_dir)

    run_dir: Optional[Path] = None
    if config.enable_logs:
        log_root = Path(config.log_root) if isinstance(config.log_root, (str,)) else (config.log_root or (config.out_dir / "logs"))
        log_root = Path(log_root)
        ensure_dir(log_root)
        run_dir = log_root / run_id
        ensure_dir(run_dir)

    records: list[dict] = []
    sessions_rows: list[tuple] = []
    identifiers: list[str] = []
    seen: set[str] = set()
    conversation_meta: dict[str, dict[str, object]] = {}
    windows_rows: list[dict[str, object]] = []
    created_within = 0
    modified_within = 0
    updated_only = 0

    if config.entries_api == "conversation-data":
        print(
            f"Consultando SOQL em Conversation de {format_soql_datetime(window_start)} "
            f"até {format_soql_datetime(window_end)} em {len(windows)} janela(s)..."
        )
    else:
        print(
            f"Consultando SOQL por canal '{config.channel}' de {format_soql_datetime(window_start)} "
            f"até {format_soql_datetime(window_end)} em {len(windows)} janela(s)..."
        )

    for idx, (chunk_start, chunk_end) in enumerate(windows, start=1):
        if config.entries_api == "conversation-data":
            window_records = client.soql(build_conversation_query(chunk_start, chunk_end))
        else:
            window_records = client.soql(
                build_messaging_session_query(
                    config.channel,
                    chunk_start,
                    chunk_end,
                    include_updated=config.include_updated,
                    use_systemmodstamp=config.use_systemmodstamp,
                )
            )

        windows_rows.append(
            {
                "index": idx,
                "windowStart": format_soql_datetime(chunk_start),
                "windowEnd": format_soql_datetime(chunk_end),
                "sourceRecords": len(window_records),
            }
        )
        records.extend(window_records)

        if config.entries_api == "conversation-data":
            for r in window_records:
                cid = r.get("ConversationIdentifier")
                if cid and cid not in seen:
                    seen.add(cid)
                    identifiers.append(cid)
                if cid:
                    candidate_last_modified = r.get("LastModifiedDate")
                    current_last_modified = conversation_meta.get(cid, {}).get("lastModifiedDate")
                    candidate_dt = parse_sf_dt(candidate_last_modified if isinstance(candidate_last_modified, str) else None)
                    current_dt = parse_sf_dt(current_last_modified if isinstance(current_last_modified, str) else None)
                    if current_dt is None or (candidate_dt is not None and candidate_dt > current_dt):
                        conversation_meta[cid] = {
                            "conversationIdentifier": cid,
                            "sourceId": r.get("Id"),
                            "startTime": r.get("StartTime"),
                            "endTime": r.get("EndTime"),
                            "lastModifiedDate": candidate_last_modified,
                            "sourceObject": "Conversation",
                        }

                start_dt = parse_sf_dt(r.get("StartTime"))
                modified_dt = parse_sf_dt(r.get("LastModifiedDate"))
                swin = start_dt is not None and start_dt >= window_start
                mwin = modified_dt is not None and modified_dt >= window_start
                if swin:
                    created_within += 1
                if mwin:
                    modified_within += 1
                if (not swin) and mwin:
                    updated_only += 1
        else:
            for r in window_records:
                conv = r.get("Conversation") or {}
                cid = conv.get("ConversationIdentifier")
                end_user = r.get("MessagingEndUser") or {}
                end_user_name = end_user.get("Name") if isinstance(end_user, dict) else None
                end_user_key = end_user.get("MessagingPlatformKey") if isinstance(end_user, dict) else None
                if cid and cid not in seen:
                    seen.add(cid)
                    identifiers.append(cid)
                created_dt = parse_sf_dt(r.get("CreatedDate"))
                mod_field = "SystemModstamp" if config.use_systemmodstamp else "LastModifiedDate"
                modified_dt = parse_sf_dt(r.get(mod_field))
                if cid:
                    candidate_last_modified = r.get(mod_field)
                    current_last_modified = conversation_meta.get(cid, {}).get("lastModifiedDate")
                    candidate_dt = parse_sf_dt(candidate_last_modified if isinstance(candidate_last_modified, str) else None)
                    current_dt = parse_sf_dt(current_last_modified if isinstance(current_last_modified, str) else None)
                    if current_dt is None or (candidate_dt is not None and candidate_dt > current_dt):
                        conversation_meta[cid] = {
                            "conversationIdentifier": cid,
                            "sourceId": r.get("Id"),
                            "createdDate": r.get("CreatedDate"),
                            "lastModifiedDate": candidate_last_modified,
                            "channel": r.get("ChannelName"),
                            "endUserName": end_user_name or "",
                            "endUserMessagingPlatformKey": end_user_key or "",
                            "sourceObject": "MessagingSession",
                        }
                cwin = created_dt is not None and created_dt >= window_start
                mwin = modified_dt is not None and modified_dt >= window_start
                if cwin:
                    created_within += 1
                if mwin:
                    modified_within += 1
                if (not cwin) and mwin:
                    updated_only += 1

                sessions_rows.append((
                    r.get("Id"),
                    r.get("ChannelName"),
                    cid or "",
                    r.get("CreatedDate"),
                    r.get(mod_field),
                    end_user_name or "",
                    end_user_key or "",
                ))

    if config.enrich_messaging_sessions and config.entries_api == "conversation-data" and identifiers:
        print("Consultando MessagingSession para enriquecimento opcional...")
        enriched_rows: list[tuple] = []
        for chunk in _chunked(identifiers, 200):
            for r in client.soql(build_sessions_by_identifier_query(chunk)):
                conv = r.get("Conversation") or {}
                end_user = r.get("MessagingEndUser") or {}
                enriched_rows.append((
                    r.get("Id"),
                    r.get("ChannelName"),
                    conv.get("ConversationIdentifier") or "",
                    r.get("CreatedDate"),
                    r.get("LastModifiedDate"),
                    end_user.get("Name") if isinstance(end_user, dict) else "",
                    end_user.get("MessagingPlatformKey") if isinstance(end_user, dict) else "",
                ))
        sessions_rows = enriched_rows

    total_records = len(records)
    skipped_rows: list[dict[str, object]] = []
    download_queue: list[str] = []
    seen_rows: list[dict[str, object]] = []
    for conv_id in identifiers:
        meta_row = dict(conversation_meta.get(conv_id, {}))
        state_row = conversations_state.get(conv_id)
        current_last_modified = meta_row.get("lastModifiedDate")
        unchanged = conversation_is_unchanged(
            current_last_modified if isinstance(current_last_modified, str) else None,
            state_row,
        )
        seen_row = {
            "conversationIdentifier": conv_id,
            "lastModifiedDate": meta_row.get("lastModifiedDate"),
            "stateLastModifiedDate": (state_row or {}).get("lastModifiedDate"),
            "willDownload": (not unchanged),
        }
        seen_rows.append(seen_row)
        if unchanged:
            skipped_rows.append({
                "conversationIdentifier": conv_id,
                "reason": "unchanged",
                "lastModifiedDate": meta_row.get("lastModifiedDate"),
                "stateLastModifiedDate": (state_row or {}).get("lastModifiedDate"),
            })
        else:
            download_queue.append(conv_id)

    if run_dir is not None:
        if sessions_rows:
            sessions_csv = run_dir / "sessions.csv"
            with open(sessions_csv, "w", encoding="utf-8", newline="") as fp:
                writer = csv.writer(fp)
                writer.writerow([
                    "MessagingSessionId",
                    "ChannelName",
                    "ConversationIdentifier",
                    "CreatedDate",
                    "LastModifiedDate",
                    "EndUserName",
                    "EndUserMessagingPlatformKey",
                ])
                writer.writerows(sessions_rows)

        (run_dir / "identifiers.txt").write_text("\n".join(identifiers), encoding="utf-8")

        meta = {
            "days": config.days,
            "apiVersion": config.api_version,
            "entriesApi": config.entries_api,
            "recordLimit": config.record_limit,
            "startTimestamp": start_timestamp_ms,
            "endTimestamp": end_timestamp_ms,
            "windowStart": format_soql_datetime(window_start),
            "windowEnd": format_soql_datetime(window_end),
            "windowCount": len(windows),
            "windowSizeMinutes": config.window_size_minutes,
            "outDir": str(config.out_dir),
            "jsonRootDir": str(json_root_dir),
            "csvRootDir": str(csv_root_dir),
            "jsonOutDir": str(json_out_dir),
            "csvOutDir": str(csv_out_dir),
            "instanceUrl": getattr(client, "instance_url", None),
            "sourceObject": ("Conversation" if config.entries_api == "conversation-data" else "MessagingSession"),
            "sourceRecords": total_records,
            "uniqueIdentifiers": len(identifiers),
            "createdWithinWindow": created_within,
            "modifiedWithinWindow": modified_within,
            "updatedOnly": updated_only,
            "stateDir": str(state_root),
            "stateFile": str(latest_state_path),
            "runId": run_id,
            "seenConversations": len(identifiers),
            "downloadQueue": len(download_queue),
            "skippedConversations": len(skipped_rows),
            "sessionEnrichmentEnabled": config.enrich_messaging_sessions,
            "sessionRows": len(sessions_rows),
        }
        if config.entries_api != "conversation-data":
            meta["channel"] = config.channel
        (run_dir / "params.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    write_json(run_state_dir / "seen_conversations.json", seen_rows)
    write_json(run_state_dir / "skipped_conversations.json", skipped_rows)
    write_json(run_state_dir / "windows.json", windows_rows)

    if dump_sessions_csv_path and sessions_rows:
        csv_path = dump_sessions_csv_path
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerow([
                "MessagingSessionId",
                "ChannelName",
                "ConversationIdentifier",
                "CreatedDate",
                "LastModifiedDate",
                "EndUserName",
                "EndUserMessagingPlatformKey",
            ])
            writer.writerows(sessions_rows)

    source_label = "Conversations" if config.entries_api == "conversation-data" else "Sessões"
    print(f"{source_label}: {total_records}, identifiers únicos: {len(identifiers)}")
    print(
        f"Encontradas {len(identifiers)} conversa(s) únicas. "
        f"Download: {len(download_queue)}, ignoradas sem mudança: {len(skipped_rows)}."
    )

    ndjson_path = json_out_dir / "all_conversations.ndjson"
    ndjson_fp = None
    if config.write_ndjson:
        ndjson_fp = open(ndjson_path, "w", encoding="utf-8")

    csv_fp = None
    csv_writer = None
    csv_path = entries_csv_path
    if csv_path:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_fp = open(csv_path, "w", encoding="utf-8", newline="")
        csv_writer = csv.DictWriter(
            csv_fp,
            fieldnames=[
                "conversationId",
                "identifier",
                "entryType",
                "messageText",
                "messageType",
                "messageReason",
                "formatType",
                "templateName",
                "attachmentCount",
                "senderRole",
                "senderAppType",
                "senderSubject",
                "clientTimestamp",
                "clientTimestampUtc",
                "serverReceivedTimestamp",
                "serverReceivedTimestampUtc",
                "transcriptedTimestamp",
                "transcriptedTimestampUtc",
                "sortTimestamp",
                "sortTimestampUtc",
                "relatedRecordsJson",
                "recipientsJson",
                "rawEntryJson",
            ],
        )
        csv_writer.writeheader()

    summary: dict[str, int | str] = {
        "conversations": len(identifiers),
        "downloadedConversations": 0,
        "skippedConversations": len(skipped_rows),
        "messages": 0,
        "failedConversations": 0,
    }

    err_writer = None
    err_fp = None
    if run_dir is not None:
        err_fp = open(run_dir / "errors.csv", "w", encoding="utf-8", newline="")
        err_writer = csv.writer(err_fp)
        err_writer.writerow(["ConversationIdentifier", "Error"])

    downloaded_rows: list[dict[str, object]] = []
    failed_rows: list[dict[str, object]] = []

    def persist_progress() -> None:
        write_json(run_state_dir / "downloaded_conversations.json", downloaded_rows)
        write_json(run_state_dir / "failed_conversations.json", failed_rows)
        write_json(run_state_dir / "summary.json", summary)
        write_json(latest_state_path, conversations_state)

    for conv_id in tqdm(download_queue, desc="Baixando conversas", unit="conv"):
        try:
            payload = client.get_conversation_entries_all(
                conv_id,
                record_limit=config.record_limit,
                start_timestamp=start_timestamp_ms,
                end_timestamp=end_timestamp_ms,
            )
            out_file = json_out_dir / f"{conv_id}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            entries = payload.get("conversationEntries", []) or []
            summary["downloadedConversations"] = int(summary.get("downloadedConversations", 0)) + 1
            summary["messages"] = int(summary.get("messages", 0)) + len(entries)
            state_row = dict(conversation_meta.get(conv_id, {}))
            state_row.update({
                "lastDownloadedAt": datetime.now(timezone.utc).isoformat(),
                "lastRunId": run_id,
                "messageCount": len(entries),
                "outputFile": str(out_file),
            })
            conversations_state[conv_id] = state_row
            downloaded_rows.append({
                "conversationIdentifier": conv_id,
                "lastModifiedDate": state_row.get("lastModifiedDate"),
                "messageCount": len(entries),
                "outputFile": str(out_file),
            })

            if ndjson_fp and entries:
                for e in entries:
                    row = {
                        "conversationId": conv_id,
                        "identifier": e.get("identifier"),
                        "entryType": e.get("entryType"),
                        "messageText": extract_message_text(e),
                        "clientTimestamp": e.get("clientTimestamp"),
                        "serverReceivedTimestamp": e.get("serverReceivedTimestamp"),
                        "transcriptedTimestamp": e.get("transcriptedTimestamp"),
                        "sortTimestamp": extract_sort_timestamp(e),
                        "sender": e.get("sender"),
                        "recipients": e.get("recipients"),
                        "relatedRecords": e.get("relatedRecords"),
                        "entry": e,
                    }
                    ndjson_fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            if csv_writer and entries:
                for e in entries:
                    csv_writer.writerow(flatten_entry_row(conv_id, e))
            persist_progress()
        except Exception as ex:
            summary["failedConversations"] = int(summary.get("failedConversations", 0)) + 1
            failed_rows.append({
                "conversationIdentifier": conv_id,
                "error": str(ex),
                "lastModifiedDate": conversation_meta.get(conv_id, {}).get("lastModifiedDate"),
            })
            persist_progress()
            if err_writer is not None:
                err_writer.writerow([conv_id, str(ex)])

    if ndjson_fp:
        ndjson_fp.close()
        print(f"NDJSON agregado salvo em: {ndjson_path}")

    if csv_fp:
        csv_fp.close()
        print(f"CSV agregado salvo em: {csv_path}")

    if err_fp:
        err_fp.close()

    persist_progress()

    print(
        f"Exportação concluída. Conversas vistas: {summary['conversations']}, "
        f"baixadas: {summary['downloadedConversations']}, ignoradas: {summary['skippedConversations']}, "
        f"mensagens: {summary['messages']}, falhas: {summary['failedConversations']}. Saída: {config.out_dir}"
    )
    if run_dir is not None:
        (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Logs da execução em: {run_dir}")
    print(f"Estado consolidado em: {latest_state_path}")
    print(f"Stage da execução em: {run_state_dir}")
    if dump_sessions_csv_path and sessions_rows:
        print(f"Lista de sessões (cópia) gravada em: {dump_sessions_csv_path}")
    return summary
