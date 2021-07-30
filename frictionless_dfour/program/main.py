import typer
from typing import Optional
from .. import config

# Program

program = typer.Typer()


def version(value: bool):
    if value:
        typer.echo(config.VERSION)
        raise typer.Exit()


@program.callback()
def version(
    version: Optional[bool] = typer.Option(None, "--version", callback=version),
):
    """Interact with dfour from the command line."""
    pass


# @program.command(
#   name="login",
#   help="Store login credentials in a config file. Alternatively use --username/-u, --password/-p and --endpoint/-e options.",
#   short_help="Store login credentials",
#   )
# def program_login():
#   """
#   Login to dfour via the command line.
#   """
#   home_dir = os.path.expanduser("~")
#   conf_dir = ".config/dfour"
#   conf_file = "config.json"
#   # Check for existing config.json file
#   if os.path.exists(f"{home_dir}/{conf_dir}/{conf_file}"):
#     typer.secho("Login successful.", fg=typer.colors.GREEN, bold=True)

#   else:
#     is_stdin = True
#     typer.secho("No login credentials found.", err=True, fg=typer.colors.YELLOW, bold=True)
#     typer.secho(f"Creating empty credentials in {home_dir}/{conf_dir}/{conf_file}")
#     typer.secho("")
#     endpoint = "https://sandbox.dfour.space"
#     endpoint_input = input(f"Enter dfour endpoint [{endpoint}]: ")
#     if endpoint_input != "":
#       endpoint = endpoint_input
#     username = ""
#     password = ""
#     while username == "":
#       username = input("Enter your dfour username: ")
#     while password == "":
#       password = getpass.getpass("Enter your dfour password: ")

#     credentials = {
#       "endpoint": endpoint,
#       "username": username,
#       "password": password,
#     }

#     if not os.path.exists(f"{home_dir}/{conf_dir}"):
#       os.mkdir(f"{home_dir}/{conf_dir}")

#     with open(f"{home_dir}/{conf_dir}/{conf_file}","w") as f:
#       json.dump(credentials, f)

#     typer.secho("Login successful.", fg=typer.colors.GREEN, bold=True)
#     raise typer.Exit()
