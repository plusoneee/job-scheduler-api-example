from typing import List, Optional, Type, TypeVar
from sqlalchemy import delete, insert, inspect, select, update
from Application.routers.v1.models.database import db

ModelClass_T = TypeVar('ModelClass_T')
ModelObj_T = TypeVar('ModelObj_T')

class BaseRepository:
    def base_add(
        self,
        model: ModelObj_T
    ) -> dict:
        with db.session() as session:
            session.add(model)
            session.commit()
            session.refresh(model)
            return model.as_dict()

    def base_get(
        self,
        model: ModelClass_T,
        **kwargs
    ) -> Optional[dict]:
        with db.session() as session:
            item = session.get(model, kwargs)
            if item:
                return item.as_dict()

    def base_query(
        self,
        model: ModelClass_T,
        filters: set = set()
    ) -> Optional[List[dict]]:
        stmt = select(model).where(*filters)
        with db.session() as session:
            result = session.execute(stmt)
            return [item.as_dict() for item in result.scalars().all()]

    def base_delete(
        self,
        model: ModelClass_T,
        **kwargs
    ) -> Optional[dict]:
        with db.session() as session:
            item = session.get(model, kwargs)
            if item:
                data = item.as_dict()
                session.delete(item)
                session.commit()
                return data

    def base_query_delete(
        self,
        model: ModelClass_T,
        filters: set = set()
    ) -> Optional[List[dict]]:
        select_stmt = select(model).where(*filters)
        delete_stmt = delete(model).where(*filters).execution_options(
            synchronize_session="fetch"
        )
        with db.session() as session:
            select_result = session.execute(select_stmt)
            session.execute(delete_stmt)
            session.commit()
            return [item.as_dict() for item in select_result.scalars().all()]

    def base_distinct(
        self,
        model: ModelClass_T,
        distinct_args: set = set(),
        order_args: set = set(),
    ):
        stmt = select(model).distinct(*distinct_args).order_by(*order_args)
        with db.session() as session:
            result = session.execute(stmt).columns(model)
            return [item.as_dict() for item in result.scalars().all()]
