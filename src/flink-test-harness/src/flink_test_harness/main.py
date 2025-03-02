import typer
from commands import command1, command2

app = typer.Typer(no_args_is_help=True)

app.add_typer(command1.app.greet, name="greet")
app.add_typer(command1.app.create, name="create")
app.add_typer(command2.app, name="repeat")

if __name__ == "__main__":
    app()