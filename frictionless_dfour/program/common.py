from typer import Argument, Option


# Source

folder = Argument(
    default=None,
    help="folder [default: stdin]",
)

workspace = Argument(default=None, help="workspace hash [default: stdin]")


# Options

username = Option(
    None,
    "--username",
    "-u",
    help="dfour username as a string",
)

password = Option(
    None,
    "--password",
    "-p",
    help="dfour password as a string",
)

endpoint = Option(
    "https://sandbox.dfour.space",
    "--endpoint",
    "-e",
    help="dfour endpoint as a string",
)

dry = Option(
    False,
    help="dry run only, prints changes",
)

noninteractive = Option(False, "-y", help="run without prompts")

credentials = Option(
    None,
    "--credentials",
    "-c",
    help="path to custom credentials file",
)

# Formats

yaml = Option(False, help="Return in YAML format")

json = Option(False, help="Return in JSON format")

csv = Option(False, help="Return in CSV format")
