import typer

app = typer.Typer()

@app.command()
def repeat(count: int = typer.Option(1, help="Number of times to repeat")):
    """Repeats a message."""
    for i in range(count):
        typer.echo(f"Executing command2, iteration {i + 1}")

if __name__ == "__main__":
    app()