import os
import json as js
import yaml as ym
from dictdiffer import diff
from slugify import slugify
import hashlib
import typer
import datetime
import time
import pytz
from tzlocal import get_localzone
import requests
import pathlib
from . import common
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import base64
from frictionless import Package, system
from ..dfour import DfourDialect
from .main import program

workspace = typer.Typer()
gmt = pytz.timezone("GMT")  # server timezone -> should be inferred from request
local_tz = get_localzone()  # current systems timezone

# Classes


class DateTimeEncoder(js.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime.datetime):
            return str(z)
        else:
            return super().default(z)


@program.command(
    name="workspace",
    help="Status and sync of workspace and local folder",
    no_args_is_help=True,
)
def program_workspace(
    workspace: str = common.workspace,
    folder: str = common.folder,
    dry: bool = common.dry,
    noninteractive: bool = common.noninteractive,
    username: str = common.username,
    password: str = common.password,
    endpoint: str = common.endpoint,
    yaml: bool = common.yaml,
    json: bool = common.json,
    csv: bool = common.csv,
):
    """
    Show workspace overview.
    """

    credentials = dict(
        username=username if username is not None else os.getenv("DFOUR_USERNAME"),
        password=password if password is not None else os.getenv("DFOUR_PASSWORD"),
    )

    if os.path.exists(f"{folder}/dfour.yaml"):
        with open(f"{folder}/dfour.yaml") as config_file:
            ws_config = ym.safe_load(config_file)
        config_data = ws_config

    else:
        config_data = {}
        config_data[workspace] = dict(endpoint=endpoint, snapshots=[])

        typer.secho(f"Found no dfour.yaml in {folder}.")

        if not noninteractive:
            typer.promt(f"Create {folder}/dfour.yaml?")

        with open(f"{folder}/dfour.yaml", "w") as config_file:
            ym.dump(config_data, config_file)

    endpoint = (
        config_data[workspace]["endpoint"]
        if "endpoint" in config_data[workspace].keys()
        else endpoint
    )

    local_data = get_local_data(folder, config_data, workspace, noninteractive)
    remote_data = get_remote_data(endpoint, workspace)

    merged = local_data["snapshots"].copy()
    merged.update(remote_data["snapshots"])
    data_diff = diff(remote_data["snapshots"], local_data["snapshots"])

    changes = []
    for v in data_diff:
        change_type = v[0]
        key = v[1]
        change = v[2]

        # TODO: handle metadata updates of `topic` and `bfsNumber`

        if key.endswith("hash") or change_type == "add" or change_type == "remove":
            if key.endswith("hash"):
                snap_name = key.split(".")[0]
                path = local_data["snapshots"][snap_name]["datafile"]
                snap_hash = remote_data["snapshots"][snap_name]["pk"]

                local_date = local_data["snapshots"][snap_name]["last_modified"]
                remote_date = remote_data["snapshots"][snap_name]["last_modified"]

                if local_date < remote_date:
                    options = dict(
                        type="download-replace",
                        source=snap_hash,
                        target=path,
                        topic=remote_data["snapshots"][snap_name]["topic"],
                        bfsNumber=remote_data["snapshots"][snap_name]["bfsNumber"],
                    )

                elif local_date > remote_date:
                    options = dict(
                        type="upload-replace",
                        source=path,
                        target=snap_hash,
                        topic=local_data["snapshots"][snap_name]["topic"],
                        bfsNumber=local_data["snapshots"][snap_name]["bfsNumber"],
                    )

                if options:
                    to_apply = {
                        "name": snap_name,
                        **options,
                        "local_date": local_date,
                        "remote_date": remote_date,
                    }
                    changes.append(to_apply)
            else:
                change_type = "download" if change_type == "remove" else "upload"

                for sub in change:
                    snap_name = sub[0]
                    path = f"{folder}/{snap_name}.json"
                    snap_hash = (
                        remote_data["snapshots"][snap_name]["pk"]
                        if change_type == "download"
                        else None
                    )

                    local_date = (
                        local_data["snapshots"][snap_name]["last_modified"]
                        if snap_name in local_data["snapshots"]
                        else None
                    )
                    remote_date = (
                        remote_data["snapshots"][snap_name]["last_modified"]
                        if snap_name in remote_data["snapshots"]
                        else None
                    )

                    if change_type == "download":
                        options = dict(
                            type=change_type,
                            source=snap_hash,
                            target=path,
                            topic=remote_data["snapshots"][snap_name]["topic"],
                            bfsNumber=remote_data["snapshots"][snap_name]["bfsNumber"],
                        )
                    elif change_type == "upload":
                        options = dict(
                            type=change_type,
                            source=local_data["snapshots"][snap_name]["datafile"],
                            target="",
                            topic=local_data["snapshots"][snap_name]["topic"],
                            bfsNumber=local_data["snapshots"][snap_name]["bfsNumber"],
                        )

                    to_apply = {
                        "name": snap_name,
                        **options,
                        "local_date": local_date,
                        "remote_date": remote_date,
                    }
                    changes.append(to_apply)

    # snaps = compile_snapshots(endpoint, workspace, data,folder)

    if not yaml and not json and not csv and len(changes) > 0:
        typer.secho(f"{len(merged)} snapshot(s) found. Changes:")
        typer.secho(js.dumps(changes, cls=DateTimeEncoder, indent=4))

    if len(changes) > 0 and not dry:
        if not noninteractive:
            typer.confirm("Do you want to apply these changes?", abort=True)
        typer.secho("Processing")

        process_changes(changes, folder, endpoint, workspace, credentials)
    else:
        typer.secho(f"\n{len(merged)} snapshot(s) found. No changes detected.\n")


# Helpers


def get_local_data(folder, config_data_raw, workspace, noninteractive):
    local_snaps = {"folder": folder, "snapshots": {}}

    config_data = config_data_raw[workspace]["snapshots"]

    for snap_file in [
        f for f in os.listdir(folder) if not f.startswith(".") and f.endswith(".json")
    ]:
        with open(f"{folder}/{snap_file}") as file_:
            fname = pathlib.Path(f"{folder}/{snap_file}")
            mtime = local_tz.localize(
                datetime.datetime.fromtimestamp(fname.stat().st_mtime)
            )
            mtime = mtime.replace(microsecond=0)
            f_data = js.load(file_)

            snap_name = resolve_name(f_data)

            if snap_name not in [k for k, v in config_data.items()]:
                typer.secho(
                    f"\"{f_data['name']}\" is not in {folder}/dfour.yaml for the workspace.",
                    fg=typer.colors.RED,
                )
                if not noninteractive:
                    topic = None
                    bfsNumber = None
                    while not topic:
                        topic = typer.prompt(
                            f"Whats the topic for {snap_name}? [e.g. Structure]",
                        )
                    while not bfsNumber:
                        bfsNumber = typer.prompt(
                            f"Whats the bfsNumber for {snap_name}? [e.g. 273]",
                        )

                    config_data[snap_name] = dict(topic=topic, bfsNumber=int(bfsNumber))
                    config_data_raw[workspace]["snapshots"] = config_data

                    with open(f"{folder}/dfour.yaml", "w") as config_file:
                        ym.dump(config_data_raw, config_file)

            local_snap = {
                "name": snap_name,
                "pk": "",
                "topic": config_data[snap_name]["topic"]
                if snap_name in config_data.keys()
                and "topic" in config_data[snap_name].keys()
                else None,
                "title": f_data["title"] if "title" in f_data.keys() else None,
                "bfsNumber": config_data[snap_name]["bfsNumber"]
                if snap_name in config_data.keys()
                and "bfsNumber" in config_data[snap_name].keys()
                else None,
                "datafile": f"{folder}/{snap_file}",
                "last_modified": mtime,
                "hash": hashlib.sha256(
                    js.dumps(f_data, separators=(",", ":"), sort_keys=True).encode(
                        "utf-8"
                    )
                ).hexdigest(),
            }

            local_snaps["snapshots"][snap_name] = local_snap

    return local_snaps


def get_remote_data(endpoint, workspace):

    remote_snaps = {"hash": "", "snapshots": {}}

    baseUrl = get_endpoint_url(endpoint)

    transport = RequestsHTTPTransport(url=baseUrl)
    client = Client(transport=transport, fetch_schema_from_transport=True)

    params = {
        "wshash": base64.b64encode(
            ":".join(["WorkspaceNode", workspace]).encode("utf-8")
        ).decode("ascii")
    }

    query = gql(
        """
    query snapshotsInWorkspace($wshash: ID!) {
      workspace(id: $wshash) {
        title
        description
        snapshots {
          pk
          topic
          title
          municipality {
            bfsNumber
          }
          datafile
          data
        }
      }
    }
    """
    )

    try:
        result = client.execute(query, variable_values=params)
        result = result["workspace"]
    except Exception as e:
        raise ValueError(
            f"GraphQL API query for {baseUrl} failed.\nParams: {params}\nError: {e}"
        )

    remote_snaps["hash"] = workspace

    if result:
        for snap in result["snapshots"]:
            path = f'{endpoint}/media/{snap["datafile"]}'

            r = requests.get(path)
            mtime = gmt.localize(
                datetime.datetime.strptime(
                    r.headers["last-modified"], "%a, %d %b %Y %H:%M:%S %Z"
                )
            )
            mtime = mtime.replace(tzinfo=gmt)

            try:
                name = resolve_name(snap["data"])

                remote_snap = {
                    "name": name,
                    "pk": snap["pk"],
                    "topic": snap["topic"],
                    "title": snap["title"],
                    "bfsNumber": snap["municipality"]["bfsNumber"],
                    "datafile": f'{endpoint}/media/{snap["datafile"]}',
                    "last_modified": mtime,
                    "hash": hashlib.sha256(
                        js.dumps(
                            snap["data"], separators=(",", ":"), sort_keys=True
                        ).encode("utf-8")
                    ).hexdigest(),
                }
                remote_snaps["snapshots"][name] = remote_snap

            except Exception as e:
                raise ValueError(f"Extraction failed.\nError: {e}")

    return remote_snaps


def get_endpoint_url(endpoint):
    return f"{endpoint}/graphql/"


def process_changes(changes, folder, endpoint, workspace, credentials):
    for change in changes:
        if change["type"] == "download" or change["type"] == "download-replace":
            modTime = time.mktime(
                change["remote_date"].astimezone(local_tz).timetuple()
            )

            storage = system.create_storage(
                "dfour", endpoint, dialect=DfourDialect(snapshotHash=change["source"])
            )
            pkg = storage.read_package()

            with open(change["target"], "w") as output_file:
                js.dump(pkg, output_file, indent=4)
                with open(f"{folder}/dfour.yaml", "r") as config_read:
                    config_read = ym.safe_load(config_read)
                    config_read[workspace]["snapshots"][change["name"]] = dict(
                        topic=change["topic"], bfsNumber=change["bfsNumber"]
                    )
                    with open(f"{folder}/dfour.yaml", "w") as config_write:
                        ym.dump(config_read, config_write)
            os.utime(change["target"], (modTime, modTime))

        elif change["type"] == "upload" or change["type"] == "upload-replace":
            storage = system.create_storage(
                "dfour",
                endpoint,
                dialect=DfourDialect(
                    snapshotHash=change["target"] if change["target"] != "" else None,
                    workspaceHash=workspace,
                    bfsMunicipality=change["bfsNumber"],
                    snapshotTopic=change["topic"],
                    username=credentials["username"],
                    password=credentials["password"],
                ),
            )
            pkg = Package(change["source"])
            storage.write_package(pkg.to_copy(), force=True)


def resolve_name(data):
    name = data["name"] if "name" in data.keys() else slugify(data["title"])
    return name
