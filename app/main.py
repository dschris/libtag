"""FastAPI application — entrypoint, routes, and background task orchestration."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.database import Database
from app.scanner import Scanner
from app.hasher import Hasher
from app.renamer import Renamer
from app.smart_dedup import SmartDedup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Globals ──────────────────────────────────────────────────────────
db = Database(settings.db_path)
scanner = Scanner(db)
hasher = Hasher(db)
renamer = Renamer(db)
smart_dedup = SmartDedup(db)

# Track background tasks
_tasks: dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    logger.info("Database connected")
    yield
    # Cancel running tasks
    for name, task in _tasks.items():
        if not task.done():
            task.cancel()
            logger.info(f"Cancelled task: {name}")
    await db.close()
    logger.info("Database closed")


app = FastAPI(title="LibTag", lifespan=lifespan)

# Mount static files and templates
_dir = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(_dir, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(_dir, "templates"))


# ── Utility ──────────────────────────────────────────────────────────

def _format_bytes(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


templates.env.filters["format_bytes"] = _format_bytes


# ── Background task runners ──────────────────────────────────────────

async def _run_full_pipeline():
    """Run the complete scan → hash → dedup → rename pipeline."""
    try:
        logger.info("Pipeline started")
        await db.set_job_state("pipeline_status", "scanning")
        await scanner.scan()

        await db.set_job_state("pipeline_status", "hashing")
        await hasher.hash_pending_files()

        await db.set_job_state("pipeline_status", "deduplicating")
        await hasher.find_and_group_duplicates()
        await hasher.move_duplicates_to_staging()

        await db.set_job_state("pipeline_status", "renaming")
        await renamer.rename_pending_files()

        await db.set_job_state("pipeline_status", "smart_dedup")
        await smart_dedup.find_and_resolve_smart_duplicates()

        await db.set_job_state("pipeline_status", "completed")
        logger.info("Pipeline completed")
    except asyncio.CancelledError:
        logger.info("Pipeline cancelled")
        await db.set_job_state("pipeline_status", "cancelled")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        await db.set_job_state("pipeline_status", f"error: {e}")


def _start_task(name: str, coro):
    """Start a named background task, stopping any existing one."""
    if name in _tasks and not _tasks[name].done():
        _tasks[name].cancel()
    _tasks[name] = asyncio.create_task(coro)


# ── Pages ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    stats = await db.get_stats()
    pipeline_status = await db.get_job_state("pipeline_status") or "idle"
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "pipeline_status": pipeline_status,
        "format_bytes": _format_bytes,
        "settings": settings,
    })


@app.get("/activity", response_class=HTMLResponse)
async def activity_page(request: Request):
    history = await db.get_rename_history(limit=200)
    return templates.TemplateResponse("activity.html", {
        "request": request,
        "history": history,
    })


@app.get("/duplicates", response_class=HTMLResponse)
async def duplicates_page(request: Request):
    # Exact hash duplicates
    groups = await db.get_duplicate_groups()
    enriched = []
    for g in groups:
        files = await db.get_files_by_hash(g["hash"])
        enriched.append({"group": g, "files": files})

    # Semantic (smart) duplicates
    smart_groups = await db.get_semantic_duplicate_groups()
    smart_enriched = []
    for sg in smart_groups:
        files = await db.get_files_by_content_title(sg["content_title"], sg.get("media_type"))
        smart_enriched.append({"group": sg, "files": files})

    return templates.TemplateResponse("duplicates.html", {
        "request": request,
        "groups": enriched,
        "smart_groups": smart_enriched,
        "format_bytes": _format_bytes,
    })


@app.get("/files", response_class=HTMLResponse)
async def files_page(request: Request, status: str = None, page: int = 1):
    limit = 100
    offset = (page - 1) * limit
    files = await db.get_all_files(limit=limit, offset=offset, status_filter=status)
    stats = await db.get_stats()
    return templates.TemplateResponse("files.html", {
        "request": request,
        "files": files,
        "status_filter": status,
        "page": page,
        "stats": stats,
    })


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": settings,
    })


# ── HTMX partials ───────────────────────────────────────────────────

@app.get("/partials/stats", response_class=HTMLResponse)
async def stats_partial(request: Request):
    stats = await db.get_stats()
    pipeline_status = await db.get_job_state("pipeline_status") or "idle"
    return templates.TemplateResponse("partials/stats.html", {
        "request": request,
        "stats": stats,
        "pipeline_status": pipeline_status,
        "format_bytes": _format_bytes,
    })


# ── API actions ──────────────────────────────────────────────────────

@app.post("/api/pipeline/start")
async def start_pipeline():
    _start_task("pipeline", _run_full_pipeline())
    return RedirectResponse("/", status_code=303)


@app.post("/api/pipeline/stop")
async def stop_pipeline():
    scanner.stop()
    hasher.stop()
    renamer.stop()
    smart_dedup.stop()
    if "pipeline" in _tasks and not _tasks["pipeline"].done():
        _tasks["pipeline"].cancel()
    await db.set_job_state("pipeline_status", "stopped")
    return RedirectResponse("/", status_code=303)


@app.post("/api/scan/start")
async def start_scan():
    _start_task("scan", scanner.scan())
    return RedirectResponse("/", status_code=303)


@app.post("/api/hash/start")
async def start_hash():
    _start_task("hash", hasher.hash_pending_files())
    return RedirectResponse("/", status_code=303)


@app.post("/api/dedup/start")
async def start_dedup():
    async def _dedup():
        await hasher.find_and_group_duplicates()
        await hasher.move_duplicates_to_staging()
    _start_task("dedup", _dedup())
    return RedirectResponse("/", status_code=303)


@app.post("/api/rename/start")
async def start_rename():
    _start_task("rename", renamer.rename_pending_files())
    return RedirectResponse("/", status_code=303)


@app.post("/api/smart-dedup/start")
async def start_smart_dedup():
    _start_task("smart_dedup", smart_dedup.find_and_resolve_smart_duplicates())
    return RedirectResponse("/", status_code=303)


@app.post("/api/undo/{log_id}")
async def undo_rename(log_id: int):
    entry = await db.undo_rename(log_id)
    if not entry:
        raise HTTPException(404, "Rename log entry not found or already undone")

    # Actually revert the file on disk
    try:
        if os.path.exists(entry["new_path"]):
            os.makedirs(os.path.dirname(entry["old_path"]), exist_ok=True)
            os.rename(entry["new_path"], entry["old_path"])
    except OSError as e:
        logger.error(f"Failed to undo rename on disk: {e}")
        raise HTTPException(500, f"Failed to revert file: {e}")

    return RedirectResponse("/activity", status_code=303)


@app.get("/api/stats")
async def api_stats():
    stats = await db.get_stats()
    pipeline_status = await db.get_job_state("pipeline_status") or "idle"
    return JSONResponse({"stats": stats, "pipeline_status": pipeline_status})
