import typer
from rich import print

app = typer.Typer()

@app.command()
def create(username: str):
    """
    Create a new user with USERNAME.
    """
    print(f"Creating user: {username}")

@app.command()
def greet(name: str, lastname: str = "", formal: bool = False):
    """
    Say hi to NAME, optionally with a --lastname.

    If --formal is used, say hi very formally.
    """
    if formal:
        print(f"Good day Ms. {name} {lastname}.")
    else:
        print(f"Hello {name} {lastname}")
        print("[bold red]Alert![/bold red] [green]Portal gun[/green] shooting! :boom:")


if __name__ == "__main__":
    app()