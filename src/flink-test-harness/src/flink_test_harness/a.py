import typer
from rich import print

app = typer.Typer(no_args_is_help=True)

@app.command()
def main(name: str, lastname: str = "", formal: bool = False):
    """
    Say hi to NAME, optionally with a --lastname.

    If --formal is used, say hi very formally.
    """
    if formal:
        print(f"Good day Ms. {name} {lastname}.")
    else:
        print(f"Hello {name} {lastname}")
        print("[bold red]Alert![/bold red] [green]Portal gun[/green] shooting! :boom:")

@app.command()
def repeat(count: int = typer.Option(1, help="Number of times to repeat")):
    """Repeats a message."""
    for i in range(count):
        typer.echo(f"Executing command2, iteration {i + 1}")

if __name__ == "__main__":
    app()