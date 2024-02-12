import typer

from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.console import Console
from rich.table import Table

from typing_extensions import Annotated

from intercambios.extract import DataExtractor
from intercambios.transform import DataProcessor
from intercambios.load import DataLoader

app = typer.Typer()
console = Console()

PATH_ACCDB_INPUT = r"Datos/Model PRGdia_Full_Definitivo Solution/Model PRGdia_Full_Definitivo Solution.accdb"
TOPOLOGY_PATH = r"Antecedentes/topologia.xlsx"
PATH_CSV_OUTPUT = r"Antecedentes/Curtailment_Andes.csv"

def check_path(path_prg: str) -> Path:
    """Check validity of input path

    Args:
        path_prg (str): path to the PRG folder model.

    Returns:
        Path: root path to the daily PRG.
    """
    temp_path = Path(path_prg)
    temp_path = temp_path.parent.parent
    if not temp_path.exists():
        raise ValueError(f"Path: {temp_path.as_posix()} does not exists.")
    elif not temp_path.joinpath(PATH_ACCDB_INPUT).exists():
        raise ValueError(f"Input ACCDB: {temp_path.joinpath(PATH_ACCDB_INPUT).as_posix()} does not exists.")
    elif not temp_path.joinpath(TOPOLOGY_PATH).exists():
        raise ValueError(f"Topology file: {temp_path.joinpath(TOPOLOGY_PATH).as_posix()} does not exists.")
    else:
        console.print(f"[bold green]Path to PRG folder[/bold green]: :boom: {temp_path.as_posix()}")
    return temp_path


# entrada de la aplicación.
# revisar documentación de Typer para más información sobre como manejar argumentos y opciones.
@app.command()
def main(path_prg: Annotated[str, typer.Argument(help="Path to the PRG folder")]):
    """Prorate ERV calculation for the PRG model.
    """
    
    path_prg = check_path(path_prg)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Extracting data...", total=None)
        data_extractor = DataExtractor(path_prg.joinpath(PATH_ACCDB_INPUT), path_prg.joinpath(TOPOLOGY_PATH))
        data_extractor.extract_data()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Processing data...", total=None)
        data_processor = DataProcessor(data_extractor)
        data_processor.process_prorrata()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        progress.add_task("Loading data...", total=None)
        data_loader = DataLoader(path_prg.joinpath(PATH_CSV_OUTPUT))
        data_loader.load_data(data_processor)

    print("\n") #saltar una linea para que se vea mejor la salida.
    table = Table("fecha-hora", "Curtailment", title="Resultados prorrata ERV")
    for row in data_processor.show_results().iter_rows():
        table.add_row(row[0].strftime('%Y-%m-%d %H:%M'),format(row[1],".1f"))
    console.print(table)

    # TODO: add results with graphs

if __name__ == "__main__":
    main(path_prg=r"C:\Users\felipe.bastidas\PyProyectos\prg_20240128\Datos\Model PRGdia_Full_Definitivo Solution")
