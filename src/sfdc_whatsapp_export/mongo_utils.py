from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable, Dict

from pymongo import MongoClient, ASCENDING, DESCENDING, ReplaceOne, errors


@dataclass
class MongoConfig:
    uri: str
    db_name: str
    messages_collection: str = "whatsapp_messages"
    conversations_collection: str = "whatsapp_conversations"


def get_client(uri: str) -> MongoClient:
    return MongoClient(uri, appname="sfdc_whatsapp_export")


def ensure_indexes(cfg: MongoConfig, client: MongoClient) -> None:
    db = client[cfg.db_name]
    msgs = db[cfg.messages_collection]
    convs = db[cfg.conversations_collection]
    try:
        msgs.create_index([("conversationId", ASCENDING), ("sortTimestamp", ASCENDING)])
        msgs.create_index([("identifier", ASCENDING)], unique=False)
        msgs.create_index([("_id", ASCENDING)], unique=True)
        convs.create_index([("conversationId", ASCENDING)], unique=True)
        convs.create_index([("endUserMessagingPlatformKey", ASCENDING)], unique=False)
        convs.create_index([("lastEndUserTimestamp", DESCENDING)], unique=False)
    except errors.PyMongoError:
        pass


def iter_conversation_json_files(directory: Path) -> Iterable[Path]:
    search_dir = directory / "json" if (directory / "json").exists() and (directory / "json").is_dir() else directory
    run_dirs = [p for p in search_dir.iterdir() if p.is_dir() and p.name.startswith("run_")] if search_dir.exists() else []
    if run_dirs:
        run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        search_dir = run_dirs[0]
    for p in search_dir.glob("*.json"):
        if p.name == "all_conversations.ndjson":
            continue
        yield p


def _discover_latest_sessions_csv(base_output_dir: Path) -> Optional[Path]:
    logs_dir = base_output_dir / "logs"
    if not logs_dir.exists() or not logs_dir.is_dir():
        return None
    # procura subpastas run_*
    runs = [p for p in logs_dir.iterdir() if p.is_dir() and p.name.startswith("run_")]
    if not runs:
        return None
    # pega a mais recente por mtime
    runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    candidate = runs[0] / "sessions.csv"
    return candidate if candidate.exists() else None


def _load_end_user_map_from_csv(csv_path: Path) -> Dict[str, Dict[str, Optional[str]]]:
    import csv
    mapping: Dict[str, Dict[str, Optional[str]]] = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        # Espera colunas: ConversationIdentifier, EndUserName, EndUserMessagingPlatformKey
        for row in reader:
            conv_id = row.get("ConversationIdentifier") or ""
            if not conv_id:
                continue
            mapping[conv_id] = {
                "endUserName": row.get("EndUserName"),
                "endUserMessagingPlatformKey": row.get("EndUserMessagingPlatformKey"),
            }
    return mapping


def _extract_message_text(entry: dict) -> Optional[str]:
    message_text = entry.get("messageText")
    if message_text:
        return message_text

    payload = _entry_payload(entry)
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


def _extract_sort_timestamp(entry: dict) -> int:
    return (
        entry.get("transcriptedTimestamp")
        or entry.get("serverReceivedTimestamp")
        or entry.get("clientTimestamp")
        or 0
    )


def _extract_sender_role(entry: dict) -> Optional[str]:
    sender = entry.get("sender") or {}
    if isinstance(sender, dict):
        return sender.get("role")
    return None


def _extract_sender_app_type(entry: dict) -> Optional[str]:
    sender = entry.get("sender") or {}
    if isinstance(sender, dict):
        return sender.get("appType")
    return None


def _extract_attachment_count(entry: dict) -> int:
    payload = _entry_payload(entry)
    abstract_message = payload.get("abstractMessage") or {}
    static_content = abstract_message.get("staticContent") or {}
    attachments = static_content.get("attachments") or []
    return len(attachments) if isinstance(attachments, list) else 0


def _title_item_text(item: object) -> Optional[str]:
    if not isinstance(item, dict):
        return None
    for key in ("title", "text", "subTitle", "secondarySubTitle", "tertiarySubTitle"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _choice_option_title(option: object) -> Optional[str]:
    if not isinstance(option, dict):
        return None
    for key in ("titleItem", "optionTitle"):
        title = _title_item_text(option.get(key))
        if title:
            return title
    return _title_item_text(option)


def _entry_payload(entry: dict) -> dict:
    payload = entry.get("entryPayload") or {}
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return payload if isinstance(payload, dict) else {}


def _extract_menu_fields(entry: dict) -> dict[str, object]:
    payload = _entry_payload(entry)
    abstract_message = payload.get("abstractMessage") or {}
    choices = abstract_message.get("choices") or {}
    choices_response = abstract_message.get("choicesResponse") or {}

    option_items = choices.get("optionItems") if isinstance(choices, dict) else None
    selected_options = choices_response.get("selectedOptions") if isinstance(choices_response, dict) else None
    option_titles = [
        title
        for title in (_choice_option_title(option) for option in (option_items or []))
        if title
    ]
    selected_titles = [
        title
        for title in (_choice_option_title(option) for option in (selected_options or []))
        if title
    ]
    selected_identifiers = [
        option.get("optionIdentifier")
        for option in (selected_options or [])
        if isinstance(option, dict) and option.get("optionIdentifier")
    ]

    return {
        "menuText": choices.get("text") if isinstance(choices, dict) else None,
        "menuFormatType": choices.get("formatType") if isinstance(choices, dict) else None,
        "menuOptionsText": " | ".join(option_titles) if option_titles else None,
        "menuOptions": option_items or [],
        "selectedOptionsText": " | ".join(selected_titles) if selected_titles else None,
        "selectedOptionIdentifiers": selected_identifiers,
        "selectedOptions": selected_options or [],
    }


def import_directory(
    cfg: MongoConfig,
    client: MongoClient,
    directory: Path,
    sessions_csv: Optional[Path] = None,
) -> dict:
    db = client[cfg.db_name]
    msgs = db[cfg.messages_collection]
    convs = db[cfg.conversations_collection]

    ensure_indexes(cfg, client)

    inserted = 0
    upserted_convs = 0

    # Carrega mapeamento de end-user (nome e número) a partir do sessions.csv, se disponível
    end_user_map: Dict[str, Dict[str, Optional[str]]] = {}
    if sessions_csv and Path(sessions_csv).exists():
        end_user_map = _load_end_user_map_from_csv(Path(sessions_csv))
    else:
        auto_csv = _discover_latest_sessions_csv(directory)
        if auto_csv:
            end_user_map = _load_end_user_map_from_csv(auto_csv)
    files = list(iter_conversation_json_files(directory))
    for fpath in files:
        conversation_id = fpath.stem
        try:
            payload = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception as e:
            continue
        entries = payload.get("conversationEntries", []) or []
        if not isinstance(entries, list):
            continue

        # Build conversation summary while scanning entries once.
        last_enduser_ts = 0
        first_message_ts = 0
        last_message_ts = 0
        last_message_text: Optional[str] = None
        last_message_type: Optional[str] = None
        last_sender_role: Optional[str] = None
        participants: set[str] = set()
        entry_types: set[str] = set()
        has_attachments = False
        has_templates = False
        for e in entries:
            try:
                role = _extract_sender_role(e)
                app_type = _extract_sender_app_type(e)
                ts = _extract_sort_timestamp(e)
                entry_type = e.get("entryType")
                message_text = _extract_message_text(e)

                if role:
                    participants.add(role)
                if app_type:
                    participants.add(f"{role}:{app_type}" if role else app_type)
                if entry_type:
                    entry_types.add(entry_type)

                if role == "EndUser":
                    if isinstance(ts, int) and ts > last_enduser_ts:
                        last_enduser_ts = ts
                if isinstance(ts, int) and ts > 0:
                    if first_message_ts == 0 or ts < first_message_ts:
                        first_message_ts = ts
                    if ts >= last_message_ts:
                        last_message_ts = ts
                        last_message_text = message_text
                        last_message_type = entry_type
                        last_sender_role = role
                if _extract_attachment_count(e) > 0:
                    has_attachments = True
                if message_text and message_text.startswith("[template] "):
                    has_templates = True
            except Exception:
                pass

        # Upsert conversation meta
        try:
            set_fields = {
                "messagesCount": len(entries),
                "firstMessageTimestamp": first_message_ts or None,
                "lastMessageTimestamp": last_message_ts or None,
                "lastMessageText": last_message_text,
                "lastMessageType": last_message_type,
                "lastSenderRole": last_sender_role,
                "participants": sorted(participants),
                "entryTypes": sorted(entry_types),
                "hasAttachments": has_attachments,
                "hasTemplates": has_templates,
            }
            eu = end_user_map.get(conversation_id)
            if eu:
                if eu.get("endUserName"):
                    set_fields["endUserName"] = eu.get("endUserName")
                if eu.get("endUserMessagingPlatformKey"):
                    set_fields["endUserMessagingPlatformKey"] = eu.get("endUserMessagingPlatformKey")
            if last_enduser_ts:
                set_fields["lastEndUserTimestamp"] = last_enduser_ts

            convs.update_one(
                {"conversationId": conversation_id},
                {
                    "$setOnInsert": {"conversationId": conversation_id},
                    "$set": set_fields,
                },
                upsert=True,
            )
            upserted_convs += 1
        except errors.PyMongoError:
            pass

        bulk = []
        for e in entries:
            mid = e.get("identifier") or ""
            sort_ts = _extract_sort_timestamp(e)
            doc = {
                "_id": f"{conversation_id}:{mid}" if mid else f"{conversation_id}:{sort_ts}",
                "conversationId": conversation_id,
                "identifier": mid,
                "entryType": e.get("entryType"),
                "messageText": _extract_message_text(e),
                "attachmentCount": _extract_attachment_count(e),
                **_extract_menu_fields(e),
                "clientTimestamp": e.get("clientTimestamp"),
                "serverReceivedTimestamp": e.get("serverReceivedTimestamp"),
                "transcriptedTimestamp": e.get("transcriptedTimestamp"),
                "sender": e.get("sender"),
                "recipients": e.get("recipients"),
                "relatedRecords": e.get("relatedRecords"),
                "sortTimestamp": sort_ts,
                "rawEntry": e,
            }
            bulk.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))

        if bulk:
            try:
                result = msgs.bulk_write(bulk, ordered=False)
                inserted += result.upserted_count + result.modified_count
            except errors.PyMongoError:
                pass

    return {"files": len(files), "messagesInserted": inserted, "conversationsUpserted": upserted_convs}
