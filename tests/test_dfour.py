# import pytest
from frictionless_dfour.dfour import DfourDialect
from frictionless import Package, system
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import base64


def test_dfour_storage_types(dfour_url, dfour_dialect):

    # Upload fresh package
    source = Package("data/perimeter.json")
    storage = system.create_storage("dfour", dfour_url, dialect=dfour_dialect)
    storage.write_package(source.to_copy(), force=True)

    # Download again and check content
    transport = RequestsHTTPTransport(url=f"{dfour_url}/graphql/")
    client = Client(transport=transport, fetch_schema_from_transport=True)

    params = {
        "wshash": base64.b64encode(
            ":".join(["WorkspaceNode", dfour_dialect.workspaceHash]).encode("utf-8")
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
            f"GraphQL API query for {dfour_url} failed.\nParams: {params}\nError: {e}"
        )

    if result:
        for snap in result["snapshots"]:
            if snap["data"]["name"] == source.name:
                pk = snap["pk"]

    target_dialect = DfourDialect(**dfour_dialect)
    target_dialect["snapshotHash"] = pk
    storage = system.create_storage("dfour", dfour_url, dialect=target_dialect)
    target = storage.read_package()

    # Assert metadata
    assert target.resource_names == ["sample-perimeter", "map-background"]
    assert target["views"] == [
        {
            "name": "map",
            "specType": "gemeindescanSnapshot",
            "spec": {
                "title": "Demo Sample Perimeter",
                "description": "Feel free to download this snapshot, edit it and re-upload your own version. Or use the Spatial Data Package Export QGIS-Plugin.",
                "bounds": [
                    "geo:47.449718246737376,8.656607950126903",
                    "geo:47.548002806825494,8.8099413291546",
                ],
                "legend": [],
            },
            "resources": ["map-background", "sample-perimeter"],
        }
    ]
