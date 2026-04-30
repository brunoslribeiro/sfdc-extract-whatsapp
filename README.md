# SFDC WhatsApp Extractor

Ferramenta em Python para:

- consultar `Conversation` no Salesforce por janela temporal;
- baixar as mensagens de cada conversa via nova Conversation Data GET API `GET /conversation-entries`;
- salvar os payloads em JSON;
- importar os dados para MongoDB;
- visualizar as conversas em uma interface simples para auditoria.

O projeto foi organizado para uso local, com três pontos de entrada:

- `main.py`: exportação das conversas do Salesforce;
- `mongo_import.py`: carga dos JSONs no MongoDB;
- `viewer.py`: viewer web em Flask.

## Estrutura do projeto

```text
.
|-- main.py
|-- mongo_import.py
|-- viewer.py
|-- requirements.txt
|-- .env.example
`-- src/sfdc_whatsapp_export/
    |-- cli.py
    |-- exporter.py
    |-- salesforce_client.py
    |-- auth.py
    |-- import_mongo_cli.py
    |-- mongo_utils.py
    |-- viewer_app.py
    `-- templates/
```

## Como o fluxo funciona

### 1. Extração no Salesforce

O fluxo principal usa a Conversation Data API. Primeiro, o exportador consulta `Conversation` para descobrir quais conversas foram atualizadas na janela:

- `ConversationIdentifier`;
- `StartTime`;
- `EndTime`;
- `LastModifiedDate`.

Opcionalmente, ele pode consultar `MessagingSession` apenas para enriquecer contato e número.

SOQL base do fluxo principal:

```sql
SELECT Id, StartTime, EndTime, LastModifiedDate, ConversationIdentifier
FROM Conversation
WHERE LastModifiedDate >= <window_start>
  AND LastModifiedDate < <window_end>
```

Depois disso, cada conversa é baixada pela API:

```text
https://api.salesforce.com/platform/engagement/v1.0/conversation-entries?conversationIdentifier={ConversationIdentifier}
```

O projeto preserva o payload completo das entries. Se a org ou o token ainda não suportarem essa API, a CLI pode fazer fallback para o endpoint legado do Connect REST.

Os metadados de menus interativos do WhatsApp (`choices`, `optionItems` e `choicesResponse`) dependem do payload da nova Conversation Data API. No fluxo legado `connect`, esses campos podem não existir no retorno e, nesse caso, o CSV/importador/viewer não conseguem reconstruir as opções de menu a partir do dado legado.

Se a API retornar `nextPageToken`, o cliente consolida todas as páginas em um único payload.

O exportador pode trabalhar com:

- janela relativa via `--days`;
- janela explícita via `--start-datetime` e `--end-datetime`.

Quando necessário, a janela total pode ser quebrada em sub-janelas com `--window-size-minutes`. Isso ajuda a deixar o pipeline mais previsível, facilita backfill histórico e reduz risco operacional em APIs com rate limit mais sensível.

No fluxo legado `connect`, o projeto ainda consegue descobrir conversas via `MessagingSession` filtrando por canal e datas.

No fluxo novo, o projeto aproveita principalmente:

- `ConversationIdentifier`;
- `StartTime`;
- `EndTime`;
- `LastModifiedDate`.

Se o enriquecimento opcional estiver habilitado, ele também aproveita de `MessagingSession`:

- `Conversation.ConversationIdentifier`;
- `MessagingEndUser.Name`;
- `MessagingEndUser.MessagingPlatformKey`.

Os `ConversationIdentifier` são deduplicados antes do download.

### 2. Persistência local

Para cada conversa, o projeto grava:

- JSONs por conversa em `output/json/run_YYYYmmdd_HHMMSS/<ConversationIdentifier>.json`;
- opcionalmente um NDJSON agregado em `output/json/run_YYYYmmdd_HHMMSS/all_conversations.ndjson`;
- CSVs agregados em `output/csv/run_YYYYmmdd_HHMMSS/`;
- estado consolidado e stages de execução em `output/state/`;
- logs da execução em `output/logs/run_YYYYmmdd_HHMMSS/`.

### 3. Importação para MongoDB

O importador lê todos os JSONs exportados e popula duas coleções:

- `whatsapp_messages`: mensagens individuais;
- `whatsapp_conversations`: metadados por conversa.

Quando existe `sessions.csv`, o importador enriquece a conversa com:

- nome do contato;
- número/chave da plataforma (`MessagingPlatformKey`).

### 4. Visualização

O viewer em Flask lista as conversas e abre um chat com layout simples estilo WhatsApp. A busca suporta:

- `conversationId`;
- número do contato (`endUserMessagingPlatformKey`).

## Requisitos

- Python 3.10+
- MongoDB, se você quiser usar importação e viewer

Instalação:

```bash
pip install -r requirements.txt
```

Dependências principais:

- `requests`
- `python-dotenv`
- `tqdm`
- `pymongo`
- `Flask`

## Configuração

Crie um `.env` a partir do `.env.example`.

### Opção A: token pronto

```env
SF_INSTANCE_URL=https://example.my.salesforce.com
SF_ACCESS_TOKEN=SEU_TOKEN
```

### Opção B: OAuth Client Credentials

```env
SF_AUTH_URL=https://example.my.salesforce.com/services/oauth2/token
SF_CLIENT_ID=SEU_CONNECTED_APP_CLIENT_ID
SF_CLIENT_SECRET=SEU_CONNECTED_APP_CLIENT_SECRET
SF_INSTANCE_URL=https://example.my.salesforce.com
```

Campos opcionais:

```env
SF_SCOPE=...
SF_AUDIENCE=...
SF_CONVERSATION_API_BASE_URL=https://api.salesforce.com/platform/engagement/v1.0
MONGO_URI=mongodb://localhost:27017
MONGO_DB=sfdc_whatsapp
MONGO_MESSAGES_COLLECTION=whatsapp_messages
MONGO_CONVERSATIONS_COLLECTION=whatsapp_conversations
```

Observação:

- se `SF_INSTANCE_URL` não for informado, a CLI tenta derivá-lo de `SF_AUTH_URL`;
- o projeto não implementa login interativo do Salesforce, apenas consome token pronto ou client credentials.

## Uso com Docker

O projeto agora inclui:

- `Dockerfile`: imagem única da aplicação;
- `docker-compose.yml`: orquestração de `mongo`, `viewer`, `exporter` e `importer`.

### Arquitetura no Docker

- `mongo`: banco MongoDB persistido em volume Docker;
- `viewer`: aplicação Flask publicada em `http://127.0.0.1:5000`;
- `exporter`: job sob demanda para buscar conversas no Salesforce;
- `importer`: job sob demanda para carregar os JSONs no MongoDB.

Os arquivos exportados ficam montados na pasta local `./output`, para continuarem acessíveis fora do container.

Por padrão, o MongoDB não publica porta no host. Isso evita conflito com instalações locais já usando `27017`. Dentro do Docker Compose, a aplicação acessa o banco por `mongodb://mongo:27017`.

Os serviços `exporter` e `importer` foram configurados com `entrypoint`, então os parâmetros podem ser passados diretamente após o nome do serviço no `docker compose run`.

### Passo a passo de configuração

1. Crie o arquivo `.env`:

```bash
copy .env.example .env
```

Se estiver no PowerShell:

```powershell
Copy-Item .env.example .env
```

2. Edite o `.env` com suas credenciais Salesforce.

Mínimo para client credentials:

```env
SF_AUTH_URL=https://example.my.salesforce.com/services/oauth2/token
SF_CLIENT_ID=SEU_CONNECTED_APP_CLIENT_ID
SF_CLIENT_SECRET=SEU_CONNECTED_APP_CLIENT_SECRET
SF_INSTANCE_URL=https://example.my.salesforce.com
MONGO_DB=sfdc_whatsapp
```

3. Gere a imagem:

```bash
docker compose build
```

4. Suba os serviços persistentes:

```bash
docker compose up -d mongo viewer
```

5. Acesse o viewer:

```text
http://127.0.0.1:5000/
```

Nesse momento o Mongo e o viewer já estarão ativos, mas ainda sem dados importados.

### Passo a passo de operação

1. Execute a exportação:

```bash
docker compose run --rm exporter --days 1 --record-limit 1000 --no-legacy-fallback
```

Esses comandos de Docker já estão em modo estrito da nova API. Se você alterar os resumos das conversas ou reexportar dados, rode o `importer` novamente antes de abrir o viewer.

2. Importe os arquivos gerados para o MongoDB:

```bash
docker compose run --rm importer --dir /app/output
```

3. Acesse o viewer:

```text
http://127.0.0.1:5000/
```

4. Quando terminar:

```bash
docker compose down
```

Se quiser remover também o volume do Mongo:

```bash
docker compose down -v
```

### Comandos úteis com Docker

Exportar com NDJSON:

```bash
docker compose run --rm exporter --days 1 --record-limit 1000 --ndjson --no-legacy-fallback
```

Exportar com CSV agregado:

```bash
docker compose run --rm exporter --days 1 --record-limit 1000 --entries-csv all_conversations.csv --no-legacy-fallback
```

Importar informando manualmente o `sessions.csv`:

```bash
docker compose run --rm importer --dir /app/output --sessions-csv /app/output/logs/run_YYYYmmdd_HHMMSS/sessions.csv
```

Ver logs do viewer:

```bash
docker compose logs -f viewer
```

Abrir shell dentro da imagem:

```bash
docker compose run --rm exporter bash
```

Observações importantes:

- dentro do Docker, o `MONGO_URI` é sobrescrito para `mongodb://mongo:27017`;
- dentro do Docker, o viewer sobe com `HOST=0.0.0.0` para a porta publicada funcionar no host;
- fora do Docker, o padrão continua sendo `mongodb://localhost:27017`;
- `exporter` e `importer` são jobs pontuais, por isso o uso recomendado é `docker compose run --rm`.
- se você quiser acessar o Mongo do host, pode publicar uma porta alternativa no `docker-compose.yml`, por exemplo `27018:27017`.

Sempre que voce reexportar ou alterar o modelo de resumo das conversas, rode o `importer` novamente antes de abrir o viewer.

## Uso

### Exportar conversas

Exemplo com `.env`:

```bash
python main.py --days 1 --record-limit 1000 --no-legacy-fallback
```

Exemplo com janela explícita:

```bash
python main.py --start-datetime "2026-04-01T00:00:00Z" --end-datetime "2026-04-02T00:00:00Z" --record-limit 1000 --no-legacy-fallback
```

Exemplo com janela explícita quebrada em blocos de 1 hora:

```bash
python main.py --start-datetime "2026-04-01T00:00:00Z" --end-datetime "2026-04-02T00:00:00Z" --window-size-minutes 60 --record-limit 1000 --no-legacy-fallback
```

Exemplo passando client credentials por parâmetro:

```bash
python main.py ^
  --auth-url "https://example.my.salesforce.com/services/oauth2/token" ^
  --client-id "XXX" ^
  --client-secret "YYY" ^
  --days 1 ^
  --record-limit 1000 ^
  --no-legacy-fallback
```

Exemplo com token pronto:

```bash
python main.py ^
  --instance-url "https://example.my.salesforce.com" ^
  --access-token "SEU_TOKEN" ^
  --days 1 ^
  --record-limit 1000 ^
  --no-legacy-fallback
```

### Comandos da nova API

Exportação usando explicitamente a Conversation Data API:

```bash
python main.py --days 1 --record-limit 1000 --no-legacy-fallback
```

Exportação usando a Conversation Data API com enriquecimento opcional via `MessagingSession`:

```bash
python main.py --days 1 --record-limit 1000 --with-session-enrichment --dump-sessions sessions.csv --no-legacy-fallback
```

Exportação usando a Conversation Data API com CSV agregado por entry:

```bash
python main.py --days 1 --record-limit 1000 --entries-csv all_conversations.csv --no-legacy-fallback
```

Exportação usando apenas a API antiga via Connect:

```bash
python main.py --legacy-only --channel "WhatsApp SAS" --days 1 --record-limit 1000
```

Exportação usando apenas a API antiga via Connect com janela explícita:

```bash
python main.py --legacy-only --channel "WhatsApp SAS" --start-datetime "2026-04-07T00:00:00Z" --end-datetime "2026-04-07T01:00:00Z" --record-limit 1000
```

Exportação usando apenas a API antiga via Connect, mas descobrindo as conversas por `Conversation`:

```bash
python main.py --legacy-only --legacy-discovery conversation --start-datetime "2026-04-07T00:00:00Z" --end-datetime "2026-04-07T01:00:00Z" --record-limit 1000
```

Exportação usando janela explícita com CSV agregado por entry:

```bash
python main.py --start-datetime "2026-04-01T00:00:00Z" --end-datetime "2026-04-02T00:00:00Z" --window-size-minutes 60 --record-limit 1000 --entries-csv all_conversations.csv --no-legacy-fallback
```

### Comandos de exportação em CSV

CSV agregado por entry, usando a pasta gerenciada `output/csv/`:

```bash
python main.py --days 1 --record-limit 1000 --entries-csv all_conversations.csv --no-legacy-fallback
```

CSV agregado por entry com enriquecimento opcional de sessões:

```bash
python main.py --days 1 --record-limit 1000 --entries-csv all_conversations.csv --with-session-enrichment --dump-sessions sessions.csv --no-legacy-fallback
```

CSV agregado por entry usando apenas a API antiga via Connect com janela explícita:

```bash
python main.py --legacy-only --channel "WhatsApp SAS" --start-datetime "2026-04-07T00:00:00Z" --end-datetime "2026-04-07T01:00:00Z" --record-limit 1000 --entries-csv all_conversations.csv
```

CSV agregado por entry usando a API antiga via Connect com discovery por `Conversation`:

```bash
python main.py --legacy-only --legacy-discovery conversation --start-datetime "2026-04-07T00:00:00Z" --end-datetime "2026-04-07T01:00:00Z" --record-limit 1000 --entries-csv all_conversations.csv
```

No Docker:

```bash
docker compose run --rm exporter --days 1 --record-limit 1000 --entries-csv all_conversations.csv --no-legacy-fallback
```

Parâmetros relevantes:

- `--channel`: valor de `ChannelName`, usado no fluxo legado com `MessagingSession`;
- `--days`: janela relativa quando `--start-datetime` e `--end-datetime` não forem informados;
- `--start-datetime`: início da janela em ISO-8601 com timezone;
- `--end-datetime`: fim da janela em ISO-8601 com timezone;
- `--window-size-minutes`: divide a janela total em blocos menores de discovery;
- a janela resolvida também é enviada ao endpoint de entries como `startTimestamp` e `endTimestamp`, evitando baixar mensagens fora do recorte;
- `--api-version`: versão da API Salesforce, default `62.0`;
- `--out`: diretório de saída;
- `--entries-api`: `conversation-data` (default) ou `connect`;
- `--legacy-only`: atalho para forçar somente a API antiga via `connect`;
- `--legacy-discovery`: no modo legado, escolhe discovery por `messaging-session` (default) ou `conversation`;
- `--conversation-api-base-url`: base da Conversation Data API. Default: `https://api.salesforce.com/platform/engagement/v1.0`;
- `--record-limit`: quantidade máxima de entries por chamada, normalmente de `1` a `1000`;
- `--max-requests-per-minute`: limita a cadência das chamadas para respeitar rate limit, default `90`;
- `--ndjson`: gera `all_conversations.ndjson`;
- `--entries-csv`: gera CSV agregado por entry;
- `--with-session-enrichment`: na Conversation Data API, consulta `MessagingSession` só para enriquecer contato;
- `--dump-sessions`: gera uma cópia explícita do CSV de sessões consultadas para enriquecimento;
- `--log-root`: muda a raiz dos logs;
- `--state-dir`: muda a raiz do estado consolidado e dos stages por execução;
- `--no-logs`: desabilita a pasta de logs por execução;
- `--no-legacy-fallback`: falha imediatamente se a Conversation Data API não estiver disponível, sem tentar o endpoint legado;
- `--created-only`: afeta apenas o fluxo legado com `MessagingSession`;
- `--use-systemmodstamp`: afeta apenas o fluxo legado com `MessagingSession`.

Observação sobre o legado:

- `--channel` só é filtro real quando o legado usa `--legacy-discovery messaging-session`;
- com `--legacy-discovery conversation`, o download continua via `connect`, mas a discovery passa a usar `Conversation`, então `--channel` não é aplicado como filtro efetivo.

## Saída da exportação

Exemplo de estrutura:

```text
output/
|-- json/
|   `-- run_YYYYmmdd_HHMMSS/
|       |-- 0NW....json
|       |-- 0NW....json
|       `-- all_conversations.ndjson
|-- csv/
|   `-- run_YYYYmmdd_HHMMSS/
|       |-- all_conversations.csv
|       `-- sessions.csv
|-- state/
|   |-- latest_state.json
|   `-- run_YYYYmmdd_HHMMSS/
|       |-- seen_conversations.json
|       |-- skipped_conversations.json
|       |-- windows.json
|       |-- downloaded_conversations.json
|       |-- failed_conversations.json
|       `-- summary.json
`-- logs/
    `-- run_YYYYmmdd_HHMMSS/
        |-- sessions.csv
        |-- identifiers.txt
        |-- params.json
        |-- errors.csv
        `-- summary.json
```

Arquivos gerados:

- `output/json/run_YYYYmmdd_HHMMSS/<ConversationIdentifier>.json`: payload bruto por conversa;
- `output/json/run_YYYYmmdd_HHMMSS/all_conversations.ndjson`: uma linha por entry, se habilitado;
- `output/csv/run_YYYYmmdd_HHMMSS/all_conversations.csv`: um registro por entry, se habilitado;
- `output/csv/run_YYYYmmdd_HHMMSS/sessions.csv`: cópia opcional das `MessagingSession` consultadas para enriquecimento;
- `output/state/latest_state.json`: estado consolidado por `ConversationIdentifier`, usado para pular conversas sem mudança;
- `output/state/run_YYYYmmdd_HHMMSS/`: stage da execução com listas de vistas, ignoradas, janelas executadas, baixadas e falhas;
- `logs/run_YYYYmmdd_HHMMSS/identifiers.txt`: lista única de `ConversationIdentifier`;
- `logs/run_YYYYmmdd_HHMMSS/params.json`: parâmetros e métricas da execução, incluindo `recordLimit`, `startTimestamp`, `endTimestamp`, `windowStart`, `windowEnd` e `windowCount`;
- `logs/run_YYYYmmdd_HHMMSS/errors.csv`: falhas por conversa durante o download;
- `logs/run_YYYYmmdd_HHMMSS/summary.json`: resumo da execução.

O estado incremental funciona assim:

- se a conversa não existe em `latest_state.json`, ela entra na fila;
- se `LastModifiedDate` for maior do que o salvo no estado, ela entra na fila;
- se `LastModifiedDate` for igual ou menor, ela é ignorada;
- se o download falhar, a conversa não é atualizada no estado e volta a aparecer em execuções futuras.

Na prática, isso significa:

- a primeira execução tende a baixar tudo o que estiver na janela consultada;
- as execuções seguintes baixam só conversas novas ou alteradas;
- o stage da execução mostra exatamente o que foi visto, ignorado, baixado e o que falhou;
- o `errors.csv` e `failed_conversations.json` permitem localizar facilmente conversas que não entraram no estado por causa de erro.

## Importar para MongoDB

Uso básico:

```bash
python mongo_import.py --dir output --mongo-uri "mongodb://localhost:27017" --mongo-db sfdc_whatsapp
```

Informando manualmente o `sessions.csv`:

```bash
python mongo_import.py ^
  --dir output ^
  --mongo-uri "mongodb://localhost:27017" ^
  --mongo-db sfdc_whatsapp ^
  --sessions-csv "output\\logs\\run_YYYYmmdd_HHMMSS\\sessions.csv"
```

Se `--sessions-csv` não for informado, o importador tenta descobrir automaticamente o `sessions.csv` mais recente em `output/logs/run_*/`.

Se `--dir output` for informado, o importador procura automaticamente os JSONs em `output/json/`.

### Modelo gravado no Mongo

`whatsapp_conversations`:

- `conversationId`
- `messagesCount`
- `firstMessageTimestamp`
- `lastMessageTimestamp`
- `lastMessageText`
- `lastMessageType`
- `lastSenderRole`
- `endUserName`
- `endUserMessagingPlatformKey`
- `lastEndUserTimestamp`
- `participants`
- `entryTypes`
- `hasAttachments`
- `hasTemplates`

`whatsapp_messages`:

- `_id`
- `conversationId`
- `identifier`
- `entryType`
- `messageText`
- `attachmentCount`
- `clientTimestamp`
- `serverReceivedTimestamp`
- `transcriptedTimestamp`
- `sender`
- `recipients`
- `relatedRecords`
- `sortTimestamp`
- `rawEntry`

Índices criados automaticamente:

- mensagens por `conversationId + sortTimestamp`;
- mensagens por `identifier`;
- conversas por `conversationId`;
- conversas por `endUserMessagingPlatformKey`;
- conversas por `lastEndUserTimestamp`.

Se voce quiser refletir novos campos de resumo no viewer, reimporte os JSONs:

```bash
python mongo_import.py --dir output --mongo-uri "mongodb://localhost:27017" --mongo-db sfdc_whatsapp
```

No Docker:

```bash
docker compose run --rm importer --dir /app/output
```

### Colunas do CSV agregado

O CSV agregado é pensado para importação em banco e BI:

- colunas textuais e colunas JSON são normalizadas em linha única, sem quebras de linha;
- o separador do CSV é `;`;
- colunas textuais saem sempre entre aspas duplas;
- campos de menu interativo do WhatsApp só são preenchidos quando a conversa foi baixada pela nova Conversation Data API, pois o endpoint legado `connect` pode não retornar `choices`/`choicesResponse`;
- os timestamps originais continuam no CSV;
- colunas auxiliares em UTC também são geradas:
  - `clientTimestampUtc`
  - `serverReceivedTimestampUtc`
  - `transcriptedTimestampUtc`
  - `sortTimestampUtc`

## Viewer web

Suba a aplicação:

```bash
python viewer.py
```

Acesse:

```text
http://127.0.0.1:5000/
```

Comportamento do viewer:

- lista conversas em ordem decrescente de `lastMessageTimestamp`;
- busca por `conversationId`;
- busca por texto do ultimo evento (`lastMessageText`);
- busca por número/chave de plataforma;
- destaca `entryTypes`, templates e anexos na listagem;
- renderiza mensagens ordenadas por `sortTimestamp`;
- permite filtrar por `entryType`;
- permite ocultar eventos de sistema e focar apenas em `Message`;
- mostra o `rawEntry` formatado para auditoria;
- converte timestamps para `America/Sao_Paulo`.

Variáveis lidas pelo viewer:

- `MONGO_URI`
- `MONGO_DB`
- `MONGO_MESSAGES_COLLECTION`
- `MONGO_CONVERSATIONS_COLLECTION`
- `PORT`

## SOQL base usado pelo projeto

No modo `conversation-data`:

```sql
SELECT Id,
       StartTime,
       EndTime,
       LastModifiedDate,
       ConversationIdentifier
FROM Conversation
WHERE LastModifiedDate = LAST_N_DAYS:<dias>
```

No modo legado `connect`, sem `--created-only`:

```sql
SELECT Id,
       ChannelName,
       Conversation.ConversationIdentifier,
       CreatedDate,
       LastModifiedDate,
       MessagingEndUser.Name,
       MessagingEndUser.MessagingPlatformKey
FROM MessagingSession
WHERE ChannelName = '<canal>'
  AND (
    CreatedDate = LAST_N_DAYS:<dias>
    OR LastModifiedDate = LAST_N_DAYS:<dias>
  )
```

No modo legado, com `--use-systemmodstamp`, `LastModifiedDate` é substituído por `SystemModstamp`.

No modo legado, com `--created-only`, o filtro usa apenas `CreatedDate`.

## Limitações e observações

- O projeto depende de acesso válido à API do Salesforce e permissões para consultar `Conversation` e a Conversation Data API. Se você habilitar enriquecimento opcional, também precisa acesso a `MessagingSession`.
- A extração grava um arquivo JSON por conversa; volumes grandes podem gerar muitos arquivos.
- O importador ignora duplicidades de mensagens no Mongo por meio de `_id` derivado de conversa e mensagem.
- Se o payload vier sem `identifier`, o fallback do `_id` usa timestamp.
- Por padrão, a base da Conversation Data API é `https://api.salesforce.com/platform/engagement/v1.0`. Se sua org exigir outra base, ajuste `--conversation-api-base-url`.
- O viewer é uma interface simples de auditoria local, não uma aplicação pronta para produção.
- Há alguns textos com encoding inconsistente no código fonte, mas isso não impede o funcionamento principal.

## Fluxo recomendado

```bash
pip install -r requirements.txt
python main.py --days 1 --record-limit 1000 --no-legacy-fallback
python mongo_import.py --dir output --mongo-uri "mongodb://localhost:27017" --mongo-db sfdc_whatsapp
python viewer.py
```

## Fluxo recomendado com Docker

```bash
docker compose build
docker compose up -d mongo viewer
docker compose run --rm exporter --days 1 --record-limit 1000 --no-legacy-fallback
docker compose run --rm importer --dir /app/output
```
