"""Shopfront Commerce API — synthetic REST source system.

Serves `customers` and `promotions` with the properties a real vendor API has
and a toy one does not:

  * bearer-token auth
  * cursor pagination with a hard page-size cap
  * an ``updated_since`` watermark filter
  * deliberate intermittent 500s, so the extractor must implement retry

The extractor built in Phase 4 has to handle all four. An API that always
succeeds and returns everything in one page teaches nothing.
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timezone
from typing import Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]
BEARER_TOKEN = os.environ["API_BEARER_TOKEN"]
ERROR_RATE = float(os.environ.get("API_ERROR_RATE", "0.0"))

MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 100

app = FastAPI(title="Shopfront Commerce API", version="1.0.0")
security = HTTPBearer()

ENTITIES = {
    "customers": ("commerce.customers", "customer_id"),
    "promotions": ("commerce.promotions", "promotion_id"),
}


def authorise(creds: HTTPAuthorizationCredentials = Depends(security)) -> None:
    if creds.credentials != BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="invalid bearer token")


def maybe_fail() -> None:
    """Inject transient server errors so retry logic is exercised for real."""
    if ERROR_RATE and random.random() < ERROR_RATE:
        raise HTTPException(status_code=500, detail="synthetic upstream error")


@app.get("/health")
def health() -> dict[str, str]:
    """Unauthenticated so Docker's healthcheck does not need the token."""
    return {"status": "ok"}


@app.get("/api/v1/{entity}")
def list_entity(
    entity: str,
    updated_since: str | None = Query(None, description="ISO-8601 watermark"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    _: None = Depends(authorise),
) -> dict[str, Any]:
    if entity not in ENTITIES:
        raise HTTPException(status_code=404, detail=f"unknown entity '{entity}'")
    maybe_fail()

    table, pk = ENTITIES[entity]

    where, params = "", []
    if updated_since:
        try:
            watermark = datetime.fromisoformat(updated_since)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="updated_since must be ISO-8601"
            ) from None
        where = "WHERE updated_at > %s"
        params.append(watermark)

    offset = (page - 1) * page_size

    # Ordering by (updated_at, pk) rather than updated_at alone gives a total
    # order. Without the tiebreaker, rows sharing a timestamp can appear on two
    # pages or on none — a silent, intermittent data-loss bug.
    sql = (
        f"SELECT * FROM {table} {where} "
        f"ORDER BY updated_at, {pk} LIMIT %s OFFSET %s"
    )

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [*params, page_size + 1, offset])
            rows = cur.fetchall()
            cur.execute(f"SELECT count(*) AS n FROM {table} {where}", params)
            total = cur.fetchone()["n"]

    has_more = len(rows) > page_size
    rows = rows[:page_size]

    return {
        "data": rows,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "returned": len(rows),
            "total": total,
            "has_more": has_more,
            "next_page": page + 1 if has_more else None,
        },
        "meta": {
            "entity": entity,
            "updated_since": updated_since,
            "served_at": datetime.now(timezone.utc).isoformat(),
        },
    }
