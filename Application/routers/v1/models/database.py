import psycopg2
from contextlib import contextmanager
from typing import Callable
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, registry, scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool
from fastapi import status, HTTPException

from Application.config import settings

mapper_registry = registry()
Base = mapper_registry.generate_base()

class Database:
    def __init__(
        self,
        db_url: str = f'postgresql+psycopg2://{settings.db_user}:@{settings.db_url}:{settings.db_port}/{settings.db_name}'
    ) -> None:
        self._engine = create_engine(
            url=db_url,
            poolclass=QueuePool,
            pool_size=5,
            pool_timeout=3600,
            pool_recycle=3600,
            pool_use_lifo=True,
            pool_pre_ping=True,
            max_overflow=0,
            future=True,
            echo=True,
            echo_pool=False,
            connect_args={'application_name': 'user_subscribes'}
        )

        self._session_factory = scoped_session(
            sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine,
            ),
        )

    @property
    def engine(self):
        return self._engine

    @contextmanager
    def connection(self) -> Callable[..., Connection]:
        connection: Connection = self._session_factory()
        try:
            yield connection
        except Exception as e:
            print(str(e))
            connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
                )
        finally:
            connection.close()

    @contextmanager
    def session(self) -> Callable[..., Session]:
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
                )
        finally:
            session.close()
            print(session.get_bind().pool.status())

db = Database()