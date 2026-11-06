"""Race gallery: small FastAPI apps with real asyncio race conditions, used as Déjà's acceptance tests.

Each app has a buggy default and a `safe=True` variant (same code with a lock) so later milestones can record both
failing and passing runs. Storage is in-memory by default, or Postgres when DEJA_PG_DSN is set.
"""
