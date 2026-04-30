from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from flask import Flask, render_template, request
from pymongo import MongoClient

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


@dataclass
class ViewerConfig:
    mongo_uri: str
    db_name: str
    messages_collection: str = "whatsapp_messages"
    conversations_collection: str = "whatsapp_conversations"


def create_app(cfg: ViewerConfig) -> Flask:
    here = os.path.dirname(__file__)
    templates = os.path.join(here, "templates")
    static = os.path.join(here, "static")
    app = Flask(__name__, template_folder=templates, static_folder=static)
    client = MongoClient(cfg.mongo_uri, appname="sfdc_whatsapp_viewer")
    db = client[cfg.db_name]
    msgs = db[cfg.messages_collection]
    convs = db[cfg.conversations_collection]

    def _fmt_ts_ms(ts: int | None) -> str:
        if not ts:
            return ""
        try:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            if ZoneInfo is not None:
                dt = dt.astimezone(ZoneInfo("America/Sao_Paulo"))
            else:
                dt = dt.astimezone()
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(ts)

    def _pretty_json(value: object) -> str:
        try:
            return json.dumps(value, ensure_ascii=False, indent=2)
        except Exception:
            return str(value)

    app.jinja_env.filters["ts_br"] = _fmt_ts_ms
    app.jinja_env.filters["pretty_json"] = _pretty_json

    @app.get("/")
    def index():
        q = request.args.get("q", "").strip()
        phone = request.args.get("phone", "").strip()
        text = request.args.get("text", "").strip()
        entry_identifier = request.args.get("entry_identifier", "").strip()
        limit = min(int(request.args.get("limit", 100)), 1000)
        query = {}
        if phone:
            query["endUserMessagingPlatformKey"] = {"$regex": phone}
        if q:
            query["conversationId"] = {"$regex": q}
        if text:
            query["lastMessageText"] = {"$regex": text, "$options": "i"}
        if entry_identifier:
            matching_conversation_ids = msgs.distinct(
                "conversationId",
                {"identifier": {"$regex": entry_identifier, "$options": "i"}},
            )
            if q:
                q_lower = q.lower()
                matching_conversation_ids = [
                    cid
                    for cid in matching_conversation_ids
                    if q_lower in str(cid).lower()
                ]
            query["conversationId"] = {"$in": matching_conversation_ids}
        items = list(convs.find(query).sort([
            ("lastMessageTimestamp", -1),
            ("lastEndUserTimestamp", -1),
            ("conversationId", 1),
        ]).limit(limit))
        return render_template(
            "index.html",
            items=items,
            q=q,
            phone=phone,
            text=text,
            entry_identifier=entry_identifier,
            limit=limit,
        )

    @app.get("/conversation/<conversation_id>")
    def conversation(conversation_id: str):
        entry_type = request.args.get("entry_type", "").strip()
        show_events = request.args.get("show_events", "1").strip() != "0"
        query = {"conversationId": conversation_id}
        if entry_type:
            query["entryType"] = entry_type
        elif not show_events:
            query["entryType"] = "Message"

        messages = list(msgs.find(query).sort([
            ("sortTimestamp", 1),
            ("_id", 1),
        ]))
        conv = convs.find_one({"conversationId": conversation_id}) or {}
        entry_types = sorted({
            doc.get("entryType")
            for doc in msgs.find({"conversationId": conversation_id}, {"entryType": 1})
            if doc.get("entryType")
        })
        return render_template(
            "chat.html",
            conversation_id=conversation_id,
            conversation=conv,
            messages=messages,
            entry_types=entry_types,
            selected_entry_type=entry_type,
            show_events=show_events,
        )

    return app


def main() -> None:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "sfdc_whatsapp")
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1").lower() in {"1", "true", "yes", "on"}
    cfg = ViewerConfig(mongo_uri=mongo_uri, db_name=db_name)
    app = create_app(cfg)
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
