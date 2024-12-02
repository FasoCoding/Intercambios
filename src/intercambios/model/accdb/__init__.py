from typing import Optional

import sqlalchemy
from intercambios.model.accdb import models
from intercambios.model.accdb import queries

class Querier:
    def __init__(self, conn: sqlalchemy.engine.Connection):
        self._conn = conn

    def get_user(self, *, collection_id: int, property_id) -> Optional[models.Costo_Marginal]:
        row = self._conn.execute(sqlalchemy.text(queries.CMG_DATA), {"c_id": collection_id, "p_id": property_id}).first()
        if row is None:
            return None
        return models.User(
            id=row[0],
            name=row[1],
            email=row[2],
        )