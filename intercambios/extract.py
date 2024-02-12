import importlib.resources as sql_resources

from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import (
    engine,
    create_engine,
)

import polars as pl

class  DataExtractor:
    nodes: pl.DataFrame
    gen: pl.DataFrame
    cmg: pl.DataFrame
    topo: pl.DataFrame
    path_prg: Path
    path_topo: Path

    def __init__(self, path_prg: Path, path_topo: Path):
        self.path_prg = path_prg
        self.path_topo = path_topo
        
    def extract_data(self) -> None:
        """Inicia proceso de extracción de datos.
        """
        with create_prg_engine(self.path_prg).connect() as conn:
            sql = sql_resources.files("intercambios.sql")
            self.nodes = get_access_data((sql / "gen_node.sql").read_text(), conn)
            self.gen = get_access_data((sql / "gen_data.sql").read_text(), conn)
            self.cmg = get_access_data((sql / "cmg_data.sql").read_text(), conn)
        
        self.topo = get_topo(self.path_topo)

def create_prg_engine(path_prg: Path) -> engine.Engine:
    """Creates a SQLAlchemy engine for a Microsoft Access database.

    This function takes a path to a Microsoft Access database file and returns a SQLAlchemy engine
    that can be used to interact with the database.

    Args:
        path_prg (Path): A pathlib.Path object representing the path to the .mdb or .accdb file.

    Raises:
        ValueError: If the provided path does not exist.

    Returns:
        engine.Engine: A SQLAlchemy engine object.
    """
    connection_string = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={path_prg.as_posix()};"
        r"ExtendedAnsiSQL=1;"
    )
    connection_url = engine.URL.create(
        "access+pyodbc", query={"odbc_connect": connection_string}
    )

    return create_engine(connection_url)


def get_access_data(sql_str: str, prg_engine: engine.Engine) -> pl.DataFrame:
    """Wrapper function to read data from a Microsoft Access database.

    Args:
        sql_str (str): sql query to be executed.
        prg_engine (engine.Engine): SQLAlchemy engine object.

    Raises:
        f: SQLAlchemyError if connection to database fails.

    Returns:
        pl.DataFrame: A polars DataFrame with the results of the query.
    """
    try:
        return pl.read_database(query=sql_str, connection=prg_engine)
    except SQLAlchemyError as e:
        raise f"Error: {e}"


def get_topo(path_topo: Path) -> pl.DataFrame:
    """Extracts the system topology from an Excel file on Antecedentes.

    Returns:
        pl.DataFrame: A polars DataFrame with the topology of the system.
    """
    return pl.read_excel(
        source=path_topo.absolute(),
        sheet_name="Lineas",
        xlsx2csv_options={"skip_empty_lines": True},
        read_csv_options={"new_columns": ["Nodo", "Central"]},
    )
