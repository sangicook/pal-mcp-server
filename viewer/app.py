"""
Consensus Conversation Viewer

Read-only web viewer for browsing PAL MCP Server consensus conversation history.
Opens the SQLite DB in read-only mode so it never conflicts with the MCP server.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

import markdown
from flask import Flask, jsonify, redirect, render_template, url_for
from markupsafe import Markup

app = Flask(__name__)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _db_path() -> str:
    """Resolve DB path using the same logic as storage_backend.py."""
    return os.environ.get("PAL_DB_PATH") or os.path.join(os.path.expanduser("~"), ".pal-mcp", "conversations.db")


def _get_db() -> sqlite3.Connection:
    """Open DB in read-only mode."""
    path = _db_path()
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


_schema_is_normalized: bool | None = None


def _has_normalized_schema(conn: sqlite3.Connection) -> bool:
    """Check if the DB uses the new normalized schema. Cached after first call."""
    global _schema_is_normalized
    if _schema_is_normalized is None:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='threads'")
        _schema_is_normalized = cursor.fetchone() is not None
    return _schema_is_normalized


def _all_threads() -> list[dict]:
    """Return all threads sorted by most recent update."""
    conn = _get_db()
    try:
        if _has_normalized_schema(conn):
            return _all_threads_normalized(conn)
        return _all_threads_legacy(conn)
    finally:
        conn.close()


def _all_threads_normalized(conn: sqlite3.Connection) -> list[dict]:
    """Lightweight query for the list view — thread metadata only, no turn content."""
    rows = conn.execute(
        "SELECT thread_id, tool_name, initial_context, status, created_at, last_updated_at, "
        "(SELECT COUNT(*) FROM turns WHERE thread_id = t.thread_id) as turn_count "
        "FROM threads t ORDER BY last_updated_at DESC"
    ).fetchall()

    thread_ids = [row["thread_id"] for row in rows]

    # Batch-load model names and cost per thread in a single query
    models_by_thread: dict[str, list[str]] = {}
    cost_by_thread: dict[str, float] = {}
    if thread_ids:
        placeholders = ",".join("?" * len(thread_ids))
        agg_rows = conn.execute(
            f"SELECT thread_id, "
            f"GROUP_CONCAT(DISTINCT model_name) as model_names, "
            f"SUM(json_extract(model_metadata, '$.usage.cost')) as total_cost "
            f"FROM turns WHERE thread_id IN ({placeholders}) "
            f"GROUP BY thread_id",
            thread_ids,
        ).fetchall()
        for arow in agg_rows:
            tid = arow["thread_id"]
            if arow["model_names"]:
                models_by_thread[tid] = arow["model_names"].split(",")
            if arow["total_cost"] is not None:
                cost_by_thread[tid] = arow["total_cost"]

    threads = []
    for row in rows:
        tid = row["thread_id"]
        initial_ctx = json.loads(row["initial_context"]) if row["initial_context"] else {}
        # Build a minimal ctx dict with just enough for the list page helpers
        threads.append(
            {
                "thread_id": tid,
                "tool_name": row["tool_name"],
                "initial_context": initial_ctx,
                "created_at": row["created_at"],
                "last_updated_at": row["last_updated_at"],
                "turns": [{"model_name": m} for m in models_by_thread.get(tid, [])],
                "total_cost": cost_by_thread.get(tid),
            }
        )
    return threads


def _all_threads_legacy(conn: sqlite3.Connection) -> list[dict]:
    """Fallback: query legacy conversation_store table."""
    rows = conn.execute("SELECT key, value FROM conversation_store ORDER BY updated_at DESC").fetchall()
    threads = []
    for row in rows:
        if not row["key"].startswith("thread:"):
            continue
        try:
            threads.append(json.loads(row["value"]))
        except (json.JSONDecodeError, TypeError):
            continue
    return threads


def _load_turns_for_thread(conn: sqlite3.Connection, thread_id: str) -> list[dict]:
    """Load all turns with files for a single thread. Uses batched file query."""
    turn_rows = conn.execute(
        "SELECT id, role, content, tool_name, model_provider, model_name, "
        "model_metadata, timestamp FROM turns WHERE thread_id = ? ORDER BY turn_index",
        (thread_id,),
    ).fetchall()

    # Batch-load all files for all turns
    turn_ids = [trow["id"] for trow in turn_rows]
    files_by_turn: dict[int, tuple[list[str], list[str]]] = {}
    if turn_ids:
        placeholders = ",".join("?" * len(turn_ids))
        file_rows = conn.execute(
            f"SELECT turn_id, file_path, file_type FROM turn_files WHERE turn_id IN ({placeholders})",
            turn_ids,
        ).fetchall()
        for frow in file_rows:
            tid = frow["turn_id"]
            if tid not in files_by_turn:
                files_by_turn[tid] = ([], [])
            if frow["file_type"] == "image":
                files_by_turn[tid][1].append(frow["file_path"])
            else:
                files_by_turn[tid][0].append(frow["file_path"])

    turns = []
    for trow in turn_rows:
        files, images = files_by_turn.get(trow["id"], ([], []))
        turns.append(
            {
                "role": trow["role"],
                "content": trow["content"],
                "timestamp": trow["timestamp"],
                "tool_name": trow["tool_name"],
                "model_provider": trow["model_provider"],
                "model_name": trow["model_name"],
                "model_metadata": json.loads(trow["model_metadata"]) if trow["model_metadata"] else None,
                "files": files if files else None,
                "images": images if images else None,
            }
        )
    return turns


def _get_thread(thread_id: str) -> dict | None:
    """Fetch a single thread by ID."""
    conn = _get_db()
    try:
        if _has_normalized_schema(conn):
            return _get_thread_normalized(conn, thread_id)
        return _get_thread_legacy(conn, thread_id)
    finally:
        conn.close()


def _get_thread_normalized(conn: sqlite3.Connection, thread_id: str) -> dict | None:
    """Query normalized tables for a single thread with all turns."""
    row = conn.execute(
        "SELECT thread_id, parent_thread_id, tool_name, initial_context, status, "
        "created_at, last_updated_at FROM threads WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()
    if row is None:
        return None

    return {
        "thread_id": row["thread_id"],
        "parent_thread_id": row["parent_thread_id"],
        "tool_name": row["tool_name"],
        "initial_context": json.loads(row["initial_context"]) if row["initial_context"] else {},
        "created_at": row["created_at"],
        "last_updated_at": row["last_updated_at"],
        "turns": _load_turns_for_thread(conn, thread_id),
    }


def _get_thread_legacy(conn: sqlite3.Connection, thread_id: str) -> dict | None:
    """Fallback: query legacy conversation_store table."""
    row = conn.execute(
        "SELECT value FROM conversation_store WHERE key = ?",
        (f"thread:{thread_id}",),
    ).fetchone()
    if row:
        return json.loads(row["value"])
    return None


# ---------------------------------------------------------------------------
# Content parsing helpers
# ---------------------------------------------------------------------------


def _parse_turn_content(content_str: str) -> dict:
    """Parse a turn's content field (may be JSON or plain text)."""
    try:
        data = json.loads(content_str)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, TypeError):
        pass
    return {"content": content_str}


def _extract_display_text(parsed: dict) -> str:
    """Extract the main display text from parsed turn content."""
    # Primary: the 'content' key holds the markdown response
    if "content" in parsed:
        return str(parsed["content"])
    # Fallback: expert analysis
    if "expert_analysis" in parsed:
        ea = parsed["expert_analysis"]
        if isinstance(ea, dict):
            return ea.get("analysis", str(ea))
        return str(ea)
    return json.dumps(parsed, indent=2)


def _get_step_info(parsed: dict) -> dict | None:
    """Extract step_info from parsed content."""
    return parsed.get("step_info")


def _get_analysis_summary(parsed: dict) -> dict | None:
    """Extract analysis_summary from parsed content."""
    return parsed.get("analysis_summary")


def _find_stance_for_model(initial_context: dict, model_name: str) -> str | None:
    """Find the stance assigned to a model from initial_context.models."""
    models = initial_context.get("models", [])
    if not models:
        # Also check inside 'step' if models is at top level
        models = initial_context.get("initial_request", {}).get("models", [])
    for m in models:
        if isinstance(m, dict) and m.get("model") == model_name:
            return m.get("stance")
    return None


def _get_model_roster(initial_context: dict) -> list[dict]:
    """Get the model roster with stances from initial_context."""
    models = initial_context.get("models", [])
    if not models:
        models = initial_context.get("initial_request", {}).get("models", [])
    return [m for m in models if isinstance(m, dict)]


def _get_proposal_text(ctx: dict) -> str:
    """Extract the original proposal/question text."""
    ic = ctx.get("initial_context", {})
    # The 'step' field in initial_context holds the proposal
    step = ic.get("step", "")
    if step:
        return step
    # Fallback: check first turn content
    turns = ctx.get("turns", [])
    if turns:
        parsed = _parse_turn_content(turns[0].get("content", ""))
        return _extract_display_text(parsed)[:500]
    return "(no proposal text)"


def _get_thread_preview(ctx: dict) -> str:
    """Get a short preview of the thread for listing."""
    text = _get_proposal_text(ctx)
    if len(text) > 200:
        return text[:200] + "..."
    return text


def _get_thread_models(ctx: dict) -> list[str]:
    """Get unique model names from thread turns."""
    seen = set()
    models = []
    for turn in ctx.get("turns", []):
        name = turn.get("model_name")
        if name and name not in seen:
            seen.add(name)
            models.append(name)
    return models


def _provider_for_model(model_name: str, turns: list[dict]) -> str | None:
    """Find provider for a model name by scanning turns."""
    for turn in turns:
        if turn.get("model_name") == model_name:
            return turn.get("model_provider")
    return None


def _get_usage_dict(metadata: dict | None) -> dict | None:
    """Safely extract the usage dict from turn model_metadata."""
    if not metadata or not isinstance(metadata, dict):
        return None
    usage = metadata.get("usage")
    if not usage or not isinstance(usage, dict):
        return None
    return usage


def _extract_turn_cost(metadata: dict | None) -> float | None:
    """Extract cost from a turn's model_metadata."""
    usage = _get_usage_dict(metadata)
    if usage is None:
        return None
    cost = usage.get("cost")
    if cost is None:
        return None
    try:
        return float(cost)
    except (TypeError, ValueError):
        return None


def _extract_turn_tokens(metadata: dict | None) -> dict:
    """Extract token counts from a turn's model_metadata."""
    result = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    usage = _get_usage_dict(metadata)
    if usage is None:
        return result
    for key in result:
        try:
            result[key] = int(usage.get(key, 0) or 0)
        except (TypeError, ValueError):
            pass
    return result


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def _relative_time(iso_str: str) -> str:
    """Convert ISO timestamp to relative time string."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - dt
        seconds = int(delta.total_seconds())
        if seconds < 0:
            return "just now"
        if seconds < 60:
            return f"{seconds}s ago"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m ago"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h ago"
        days = hours // 24
        if days < 30:
            return f"{days}d ago"
        months = days // 30
        if months < 12:
            return f"{months}mo ago"
        years = days // 365
        return f"{years}y ago"
    except (ValueError, TypeError):
        return iso_str


def _format_timestamp(iso_str: str) -> str:
    """Format ISO timestamp for display."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, TypeError):
        return iso_str


# ---------------------------------------------------------------------------
# Jinja filters
# ---------------------------------------------------------------------------


@app.template_filter("md")
def markdown_filter(text: str) -> Markup:
    """Render markdown to HTML."""
    html = markdown.markdown(
        text,
        extensions=["fenced_code", "tables", "nl2br"],
    )
    return Markup(html)


@app.template_filter("fmtcost")
def fmtcost_filter(value) -> str:
    """Format a dollar cost at appropriate precision."""
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    if v < 0.01:
        return f"${v:.4f}"
    if v < 1.00:
        return f"${v:.3f}"
    return f"${v:.2f}"


@app.template_filter("reltime")
def reltime_filter(iso_str: str) -> str:
    return _relative_time(iso_str)


@app.template_filter("fmttime")
def fmttime_filter(iso_str: str) -> str:
    return _format_timestamp(iso_str)


# ---------------------------------------------------------------------------
# Template context helpers
# ---------------------------------------------------------------------------


@app.context_processor
def inject_helpers():
    return {
        "parse_turn_content": _parse_turn_content,
        "extract_display_text": _extract_display_text,
        "get_step_info": _get_step_info,
        "get_analysis_summary": _get_analysis_summary,
        "find_stance_for_model": _find_stance_for_model,
        "provider_for_model": _provider_for_model,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    return redirect(url_for("thread_list"))


@app.route("/threads")
def thread_list():
    try:
        threads = _all_threads()
    except Exception as e:
        threads = []
        app.logger.error(f"Failed to load threads: {e}")

    thread_data = []
    for ctx in threads:
        thread_data.append(
            {
                "thread_id": ctx.get("thread_id", ""),
                "tool_name": ctx.get("tool_name", ""),
                "preview": _get_thread_preview(ctx),
                "models": _get_thread_models(ctx),
                "turn_count": len(ctx.get("turns", [])),
                "created_at": ctx.get("created_at", ""),
                "last_updated_at": ctx.get("last_updated_at", ""),
                "total_cost": ctx.get("total_cost"),
            }
        )
    return render_template("thread_list.html", threads=thread_data, db_path=_db_path())


@app.route("/threads/<thread_id>")
def thread_detail(thread_id: str):
    ctx = _get_thread(thread_id)
    if ctx is None:
        return render_template("thread_list.html", threads=[], error=f"Thread {thread_id} not found"), 404

    initial_context = ctx.get("initial_context", {})
    roster = _get_model_roster(initial_context)
    proposal = _get_proposal_text(ctx)
    turns = ctx.get("turns", [])

    # Enrich turns with parsed content and stance info
    enriched_turns = []
    thread_total_cost = 0.0
    thread_total_tokens = 0
    has_any_cost = False
    for turn in turns:
        parsed = _parse_turn_content(turn.get("content", ""))
        display_text = _extract_display_text(parsed)
        step_info = _get_step_info(parsed)
        analysis_summary = _get_analysis_summary(parsed)
        model_name = turn.get("model_name")
        stance = _find_stance_for_model(initial_context, model_name) if model_name else None

        # Extract work_history from model_metadata
        metadata = turn.get("model_metadata") or {}
        work_history = metadata.get("work_history", [])
        turn_type = metadata.get("turn_type")

        # For consultant_response turns, get stance from metadata
        if not stance and turn_type == "consultant_response":
            stance = metadata.get("stance")

        # Extract cost and tokens
        turn_cost = _extract_turn_cost(metadata)
        turn_tokens = _extract_turn_tokens(metadata)

        if turn_cost is not None:
            thread_total_cost += turn_cost
            has_any_cost = True
        thread_total_tokens += turn_tokens.get("total_tokens", 0)

        enriched_turns.append(
            {
                "role": turn.get("role", ""),
                "display_text": display_text,
                "step_info": step_info,
                "analysis_summary": analysis_summary,
                "model_name": model_name,
                "model_provider": turn.get("model_provider"),
                "stance": stance,
                "turn_type": turn_type,
                "timestamp": turn.get("timestamp", ""),
                "files": turn.get("files") or [],
                "images": turn.get("images") or [],
                "tool_name": turn.get("tool_name"),
                "work_history": work_history,
                "cost": turn_cost,
                "tokens": turn_tokens,
            }
        )

    return render_template(
        "thread_detail.html",
        thread=ctx,
        proposal=proposal,
        roster=roster,
        turns=enriched_turns,
        total_cost=thread_total_cost if has_any_cost else None,
        total_tokens=thread_total_tokens,
    )


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------


@app.route("/api/threads")
def api_threads():
    try:
        threads = _all_threads()
    except Exception:
        return jsonify({"error": "Failed to read database"}), 500

    result = []
    for ctx in threads:
        result.append(
            {
                "thread_id": ctx.get("thread_id", ""),
                "tool_name": ctx.get("tool_name", ""),
                "preview": _get_thread_preview(ctx),
                "models": _get_thread_models(ctx),
                "turn_count": len(ctx.get("turns", [])),
                "created_at": ctx.get("created_at", ""),
                "last_updated_at": ctx.get("last_updated_at", ""),
            }
        )
    return jsonify(result)


@app.route("/api/threads/<thread_id>")
def api_thread_detail(thread_id: str):
    ctx = _get_thread(thread_id)
    if ctx is None:
        return jsonify({"error": "Thread not found"}), 404
    return jsonify(ctx)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db = _db_path()
    print("Consensus Conversation Viewer")
    print(f"Database: {db}")
    if not os.path.exists(db):
        print(f"WARNING: Database not found at {db}")
        print("  Set PAL_DB_PATH env var or run a consensus query first.")
    print("Starting on http://localhost:5001")
    app.run(host="127.0.0.1", port=5001, debug=True)
