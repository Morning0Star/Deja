"""Postgres plumbing shared by the gallery apps. Used only when DEJA_PG_DSN is set."""

import os

DSN = os.environ.get("DEJA_PG_DSN")


async def pool():
    import asyncpg

    return await asyncpg.create_pool(DSN, min_size=1, max_size=10)
