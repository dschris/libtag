# LibTag

A local AI agent that scans your media library, intelligently renames files, and removes duplicates. Runs as a Docker container on Unraid.

## Features

- **Smart Renaming** — Uses a local LLM (Ollama) to analyze filenames and suggest clean, descriptive names
- **De-duplication** — Fast xxHash content hashing identifies identical files
- **Full Auto Mode** — Scans, renames, and deduplicates without manual approval
- **Web Dashboard** — Monitor progress, browse files, review duplicates, undo renames
- **Safe by Default** — Duplicates moved to staging (never deleted), full undo history

## Quick Start

```bash
# 1. Clone and configure
git clone <repo-url> && cd libtag
cp .env.example .env

# 2. Edit docker-compose.yml to set your media share path
# 3. Start
docker compose up -d

# 4. Pull the LLM model
docker exec libtag-ollama ollama pull llama3.1:8b

# 5. Open dashboard
open http://your-server:8080
```

See [SETUP.md](SETUP.md) for the full deployment guide.

## Architecture

```
Media Share → Scanner → Hasher → Dedup → LLM Renamer
                ↓         ↓        ↓         ↓
              SQLite DB ← ← ← ← ← ← ← ← ← ←
                ↓
           Web Dashboard (FastAPI + HTMX)
```

## License

MIT
# libtag
