import polars as pl
import networkx as nx
import datetime as dt

from dataclasses import dataclass
from typing import Protocol

@dataclass
class DataSchema:
    dates: dt.datetime
    curt: float

class DataExtractor(Protocol):
    nodes: pl.DataFrame
    gen: pl.DataFrame
    cmg: pl.DataFrame
    topo: pl.DataFrame

class DataProcessor:
    data: pl.LazyFrame
    topo: pl.DataFrame
    curt_data: pl.DataFrame
    min_date: dt.datetime
    max_date: dt.datetime

    def __init__(self, data_extractor: DataExtractor):
        self.data = _join_data(data_extractor)
        self.topo = data_extractor.topo
        self.min_date = data_extractor.cmg.select("datetime").min().item()
        self.max_date = data_extractor.cmg.select("datetime").max().item()
    
    def process_intercambios(self):
        self.curt_data = _process_intercambios(self.data, self.topo, self.min_date, self.max_date)
    
    def show_results(self) -> pl.DataFrame:
        """Generates a summary of the results for the prorate calculation.

        Args:
            df (pl.LazyFrame): Complete dataset with the prorate calculation.

        Returns:
            pl.DataFrame:  Summary of the results for the prorate calculation.
        """
        return (
            self.curt_data
            .filter(
                pl.col("datetime").ge(self.min_date + dt.timedelta(days=1)),
                pl.col("datetime").lt(self.min_date + dt.timedelta(days=2))
            )
        )


def _pivot_gen(df: pl.DataFrame) -> pl.DataFrame:
    """Pivot the generation data to a wide format and filter out the banned generators and the PMGDs.

    Args:
        df (pl.DataFrame): Generation data.

    Returns:
        pl.LazyFrame: Data in wide format with the banned generators and PMGDs filtered out.
    """
    return (
        df
        .pivot(
            values="value",
            columns="property",
            index=["generator", "datetime"])
        .select(["generator","datetime","Capacity Curtailed"])
    )


def _join_data(data_extractor: DataExtractor) -> pl.LazyFrame:
    gen_pivot = _pivot_gen(data_extractor.gen)
    return (
        data_extractor.cmg
        .join(
            data_extractor.nodes,
            on="node",
            how="left")
        .join(
            gen_pivot,
            on=["generator", "datetime"],
            how="left"
        )
        .fill_null(0)
        .group_by(["node", "datetime"])
        .agg(
            pl.col("cmg").first().alias("cmg"),
            pl.col("Capacity Curtailed").sum().alias("curt"),
        )
        .lazy()
    )


def hour_curtailment(data: pl.DataFrame, nodes: list[str]) -> float:
    return (
        data
        .filter(
            pl.col("node").is_in(nodes)
        )
        .group_by("datetime")
        .agg(pl.col("curt").sum().alias("curt"))
        .select("curt")
        .item()
    )

def get_hour_data(data:pl.DataFrame, date: dt.datetime) -> pl.DataFrame:
    return data.filter(pl.col("datetime")==date)

def get_hours(data:pl.DataFrame, node:str) -> pl.DataFrame:
    return data.filter(pl.col("node")==node,pl.col("cmg").le(0)).select("datetime").sort("datetime")

def check_cmg(data: pl.DataFrame, nodo: str) -> bool:
    if data.filter(pl.col("node")==nodo).select("cmg").is_empty():
        return True
    return data.filter(pl.col("node")==nodo).select("cmg").item() <= 0

def get_curt_nodes(G: nx.Graph, start_node: str, data: pl.DataFrame) -> list[str]:
    curt_nodes = []
    
    # Verificamos que el nodo inicial esté en el grafo
    if start_node not in G:
        print(f"El nodo inicial {start_node} no se encuentra en el grafo.")
        return curt_nodes
    
    # Realizamos un recorrido BFS para explorar los nodos conectados
    checked_list = set()  
    buffer_list = [start_node]
    
    while buffer_list:
        current_node = buffer_list.pop(0) 
        if current_node not in checked_list:
            checked_list.add(current_node)  
            # Verificamos el valor del nodo actual
            if check_cmg(data, current_node):
                curt_nodes.append(current_node)
            
            # Añadimos los nodos adyacentes no visitados al buffer_list
            for neighbor in G[current_node]:
                if neighbor not in checked_list:
                    buffer_list.append(neighbor)
    
    return curt_nodes


def _process_intercambios(
    data: pl.LazyFrame,
    topo: pl.DataFrame,
    min_date: dt.datetime,
    max_date: dt.datetime
) -> pl.DataFrame:

    Graph = nx.from_pandas_edgelist(topo.to_pandas(), 'Nodo', 'Central')
    curt_data = []

    for date in get_hours(data.collect(), "Andes220").iter_rows():
        hour_data = get_hour_data(data.collect(), *date)
        nodes = get_curt_nodes(Graph, "Andes220", hour_data)
        curt = hour_curtailment(hour_data, nodes)
        curt_data.append(DataSchema(*date, curt))
    
    df_curt = pl.DataFrame(
        [{'datetime': data.dates, 'curt': data.curt} for data in curt_data]
    )

    return (
        pl.DataFrame()
        .select(
            pl.datetime_range(
                start=min_date,
                end=max_date,
                interval="1h"
            ).alias("datetime"),
        )
        .join(df_curt, on="datetime", how="left")
        .with_columns(
            pl.when(pl.col("curt").gt(80))
            .then(80)
            .otherwise(pl.col("curt"))
            .alias("curt")
        )
        .fill_null(0)
    )




