import pandas as pd
import sqlalchemy as sa


def load_sql_data(sql: str,
                  cnx: sa.engine) -> pd.DataFrame:
    