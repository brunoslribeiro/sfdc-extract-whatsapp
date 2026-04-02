from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from .mongo_utils import MongoConfig, get_client, import_directory


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Importa conversas exportadas (JSON) para MongoDB.")
    p.add_argument("--dir", default="output", help="Diretório base da exportação; procura JSONs em <dir>/json automaticamente")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"), help="URI do MongoDB")
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "sfdc_whatsapp"), help="Nome do banco MongoDB")
    p.add_argument("--messages-col", default=os.getenv("MONGO_MESSAGES_COLLECTION", "whatsapp_messages"), help="Coleção de mensagens")
    p.add_argument("--conversations-col", default=os.getenv("MONGO_CONVERSATIONS_COLLECTION", "whatsapp_conversations"), help="Coleção de conversas")
    p.add_argument("--sessions-csv", default=os.getenv("SESSIONS_CSV", None), help="Caminho para sessions.csv (opcional). Se não informado, o importador tenta descobrir no diretório de logs mais recente.")
    return p.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    cfg = MongoConfig(
        uri=args.mongo_uri,
        db_name=args.mongo_db,
        messages_collection=args.messages_col,
        conversations_collection=args.conversations_col,
    )

    client = get_client(cfg.uri)
    stats = import_directory(cfg, client, Path(args.dir), Path(args.sessions_csv) if args.sessions_csv else None)
    print(f"Import concluído: {stats}")


if __name__ == "__main__":
    main()
