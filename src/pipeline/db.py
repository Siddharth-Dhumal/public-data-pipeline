from __future__ import annotations
from contextlib import contextmanager
from typing import Iterator, Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from pipeline.config import settings

engine: Engine = create_engine(
    settings.db_url,
    future=True,
    pool_pre_ping=True,
    pool_recycle=280,
    echo=False,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
    class_=Session,
)

Base = declarative_base()

def safe_session(existing_session: Optional[Session] = None) -> Iterator[Session]:
    if existing_session is not None:
        yield existing_session
        return

    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()