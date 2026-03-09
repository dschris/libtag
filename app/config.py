"""Application configuration via environment variables."""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """All configuration is read from environment variables or .env file."""

    # Media scanning
    media_path: str = "/media"
    scan_extensions: str = ".mkv,.mp4,.avi,.mov,.ts,.flac,.mp3,.wav,.aac,.m4a,.m4v,.wmv,.flv,.webm,.ogg,.opus,.srt,.sub,.ass,.ssa,.idx"
    scan_batch_size: int = 100

    # Ollama
    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.1:8b"
    rename_batch_size: int = 10  # files per LLM request

    # Hashing
    hash_chunk_size: int = 65536  # 64KB read chunks
    use_partial_hash: bool = True  # fast pass: hash first+last 1MB
    partial_hash_size: int = 1048576  # 1MB

    # Dedup
    duplicates_dir: str = "_duplicates"

    # Server
    host: str = "0.0.0.0"
    port: int = 8080

    # Database
    db_path: str = "/data/libtag.db"

    # Processing
    auto_mode: bool = True
    workers: int = 2  # concurrent hash workers

    model_config = {"env_prefix": "LIBTAG_", "env_file": ".env"}


settings = Settings()
