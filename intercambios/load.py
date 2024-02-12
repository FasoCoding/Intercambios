from pathlib import Path
from typing import Protocol

import polars as pl


class DataProcessor(Protocol):
    curt_data: pl.DataFrame

class DataLoader:
    path_prg: Path

    def __init__(self, path_prg: Path):
        self.path_prg = path_prg
    
    def load_data(self, data_processor: DataProcessor) -> None:
        """Inicia proceso de carga de datos.
        """
        data_processor.curt_data.write_csv(self.path_prg.as_posix())
        