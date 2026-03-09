# LibTag Setup Guide

Complete guide to deploying LibTag on Unraid from scratch.

## Prerequisites

- Unraid 6.12+ with Docker enabled
- A media share (e.g. `/mnt/user/media`)
- SSH or terminal access to your Unraid server

---

## Option A: Docker Compose (Recommended)

### 1. Clone the Repository

```bash
# SSH into your Unraid server
ssh root@your-unraid-ip

# Create a directory for LibTag
mkdir -p /mnt/user/appdata/libtag
cd /mnt/user/appdata/libtag

# Clone or copy the project files
# If using git:
git clone <your-repo-url> .
# Or copy the files manually via SMB/SCP
```

### 2. Configure Environment

```bash
# Copy the example env file
cp .env.example .env

# Edit to match your setup
nano .env
```

Key settings to adjust:
| Variable | Default | Description |
|---|---|---|
| `LIBTAG_MEDIA_PATH` | `/media` | Path inside container (match docker-compose volume) |
| `LIBTAG_OLLAMA_URL` | `http://ollama:11434` | Ollama API endpoint |
| `LIBTAG_OLLAMA_MODEL` | `llama3.1:8b` | Ollama model to use |
| `LIBTAG_AUTO_MODE` | `true` | Auto-rename without approval |
| `LIBTAG_SCAN_EXTENSIONS` | `.mkv,.mp4,...` | File types to process |

### 3. Adjust Volume Mounts

Edit `docker-compose.yml` and change the media volume to your actual share path:

```yaml
volumes:
  - /mnt/user/media:/media        # ← Change left side to your share
```

### 4. Enable GPU (Optional)

If you have an NVIDIA GPU and want faster LLM inference, uncomment the GPU section in `docker-compose.yml`:

```yaml
deploy:
  resources:
    reservations:
      devices:
        - driver: nvidia
          count: all
          capabilities: [gpu]
```

**Note:** You need the Nvidia-Driver plugin installed on Unraid for GPU passthrough.

### 5. Start the Stack

```bash
docker compose up -d
```

### 6. Pull the Ollama Model

The Ollama container starts empty. You need to pull the model:

```bash
docker exec libtag-ollama ollama pull llama3.1:8b
```

This downloads ~4.7GB. Wait for it to complete before starting the pipeline.

### 7. Access the Dashboard

Open `http://your-unraid-ip:8080` in your browser.

---

## Option B: Using Existing Ollama Instance

If you already have Ollama running on your Unraid server (e.g. from another project), you can point LibTag at it instead of starting a new container.

### 1. Remove the Ollama Service

Edit `docker-compose.yml`:
- Remove the entire `ollama:` service block
- Remove `depends_on: - ollama`
- Remove the `ollama-models:` volume

### 2. Set the Ollama URL

If Ollama is running directly on the host:
```bash
LIBTAG_OLLAMA_URL=http://host.docker.internal:11434
```

If Ollama is in another Docker container on the same bridge network:
```bash
LIBTAG_OLLAMA_URL=http://ollama-container-name:11434
```

### 3. Verify Connectivity

```bash
docker exec libtag curl -s http://your-ollama-url:11434/api/tags
```

You should see a JSON response listing available models.

---

## Ollama Setup from Scratch

If you need to set up Ollama independently on Unraid:

### Install via Community Applications

1. Go to **Apps** in the Unraid web UI
2. Search for **"Ollama"**
3. Install the `ollama/ollama` container
4. Configure:
   - **Port:** `11434`
   - **Data path:** `/mnt/user/appdata/ollama`
   - **GPU:** Enable if you have an NVIDIA GPU

### Install via Command Line

```bash
docker run -d \
  --name ollama \
  --restart unless-stopped \
  -p 11434:11434 \
  -v /mnt/user/appdata/ollama:/root/.ollama \
  ollama/ollama:latest
```

With NVIDIA GPU:
```bash
docker run -d \
  --name ollama \
  --restart unless-stopped \
  --gpus all \
  -p 11434:11434 \
  -v /mnt/user/appdata/ollama:/root/.ollama \
  ollama/ollama:latest
```

### Pull a Model

```bash
docker exec ollama ollama pull llama3.1:8b
```

**Recommended models:**
| Model | Size | Speed | Quality |
|---|---|---|---|
| `llama3.1:8b` | ~4.7GB | Fast | Good for renaming |
| `llama3.1:70b` | ~40GB | Slow | Better accuracy |
| `mistral:7b` | ~4.1GB | Fast | Good alternative |
| `gemma2:9b` | ~5.4GB | Fast | Strong at structured output |

### Verify It Works

```bash
curl http://localhost:11434/api/generate -d '{
  "model": "llama3.1:8b",
  "prompt": "Hello",
  "stream": false
}'
```

---

## Environment Variable Reference

All variables use the `LIBTAG_` prefix.

| Variable | Default | Description |
|---|---|---|
| `LIBTAG_MEDIA_PATH` | `/media` | Root directory to scan |
| `LIBTAG_OLLAMA_URL` | `http://localhost:11434` | Ollama API URL |
| `LIBTAG_OLLAMA_MODEL` | `llama3.1:8b` | Model for renaming |
| `LIBTAG_DB_PATH` | `/data/libtag.db` | SQLite database path |
| `LIBTAG_AUTO_MODE` | `true` | Rename automatically |
| `LIBTAG_SCAN_EXTENSIONS` | `.mkv,.mp4,.avi,...` | Extensions to scan |
| `LIBTAG_SCAN_BATCH_SIZE` | `100` | Files per DB batch |
| `LIBTAG_RENAME_BATCH_SIZE` | `10` | Files per LLM request |
| `LIBTAG_WORKERS` | `2` | Concurrent hash workers |
| `LIBTAG_USE_PARTIAL_HASH` | `true` | Fast partial hashing |
| `LIBTAG_HASH_CHUNK_SIZE` | `65536` | Read buffer size (bytes) |
| `LIBTAG_DUPLICATES_DIR` | `_duplicates` | Staging folder name |
| `LIBTAG_HOST` | `0.0.0.0` | Server bind address |
| `LIBTAG_PORT` | `8080` | Server port |

---

## Troubleshooting

### "Connection refused" to Ollama

- Verify Ollama is running: `docker ps | grep ollama`
- Check the URL matches: `docker exec libtag curl -s http://ollama:11434/api/tags`
- If using host Ollama, use `http://host.docker.internal:11434`

### Pipeline stalls at "renaming"

- Check Ollama logs: `docker logs libtag-ollama`
- Model may not be pulled: `docker exec libtag-ollama ollama list`
- Try a smaller model: set `LIBTAG_OLLAMA_MODEL=mistral:7b`

### "Permission denied" errors

- Ensure the container user has read/write access to the media share
- On Unraid, shares typically use `nobody:users` (UID 99, GID 100)
- Add to docker-compose: `user: "99:100"`

### Database locked

- Only run one pipeline at a time
- If stuck, stop the container and restart: `docker compose restart libtag`

### Undo a rename

- Go to the **Activity** page in the dashboard
- Click **↩ Undo** next to any rename entry
- The file will be moved back to its original name and location
