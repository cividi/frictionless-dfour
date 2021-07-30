import os
import json
import base64
import requests
import urllib.parse
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from frictionless import (
    Plugin,
    Dialect,
    Storage,
    Metadata,
    Package,
    errors,
)
from frictionless.exception import FrictionlessException


# Plugin


class DfourPlugin(Plugin):
    """Plugin for dfour
    API      | Usage
    -------- | --------
    Public   | `from frictionless_dfour import DfourPlugin`
    """

    code = "dfour"
    status = "experimental"

    def create_dialect(self, resource, *, descriptor):
        if resource.format == "dfour":
            return DfourDialect(descriptor)

    def create_storage(self, name, source, **options):
        if name == "dfour":
            return DfourStorage(source, **options)


# Dialect


class DfourDialect(Dialect):
    """Dfour dialect representation
    API      | Usage
    -------- | --------
    Public   | `from frictionless_dfour import DfourDialect`
    Parameters:
        descriptor? (str|dict): descriptor
        snapshotHash? (str): snapshotHash
        workspaceHash? (str): workspaceHash
        credentials? (dict): credentials
    Raises:
        FrictionlessException: raise any error that occurs during the process
    """

    def __init__(
        self,
        descriptor=None,
        *,
        snapshotHash=None,
        workspaceHash=None,
        username=None,
        password=None,
        snapshotTopic=None,
        bfsMunicipality=None,
    ):
        self.setinitial("snapshotHash", snapshotHash)
        self.setinitial("workspaceHash", workspaceHash)
        self.setinitial("username", username)
        self.setinitial("password", password)
        self.setinitial("snapshotTopic", snapshotTopic)
        self.setinitial("bfsMunicipality", bfsMunicipality)
        super().__init__(descriptor)

    @Metadata.property
    def snapshotHash(self):
        return self.get("snapshotHash")

    @Metadata.property
    def workspaceHash(self):
        return self.get("workspaceHash")

    @Metadata.property
    def username(self):
        return self.get("username")

    @Metadata.property
    def password(self):
        return self.get("password")

    @Metadata.property
    def snapshotTopic(self):
        return self.get("snapshotTopic")

    @Metadata.property
    def bfsMunicipality(self):
        return self.get("bfsMunicipality")

    # Metadata

    metadata_profile = {  # type: ignore
        "type": "object",
        # "required": [],
        "additionalProperties": False,
        "properties": {
            "snapshotHash": {"type": "string"},
            "workspaceHash": {"type": "string"},
            "username": {"type": "string"},
            "password": {"type": "string"},
            "snapshotTopic": {"type": "string"},
            "bfsMunicipality": {"type": "number"},
        },
    }


# Storage


class DfourStorage(Storage):
    """Dfour storage implementation
    Parameters:
        url (string): dfour instance url e.g. "https://sandbox.dfour.space"
        snapshotHash (string): dfour snapshot hash
        workspaceHash (string): dfour workspace hash
        credentials? (dict): dictionary with login credentials, e.g. { "username": "<YOURUSERNAME>", "password": "<YOURPASSWORD>" }

    API      | Usage
    -------- | --------
    Public   | `from frictionless_dfour import DfourStorage`
    """

    def __init__(self, source, *, dialect=None):
        dialect = dialect or DfourDialect()
        self.__url = source.rstrip("/")
        self.__endpoint = f"{self.__url}/graphql/"
        self.__snapshotHash = dialect.snapshotHash
        self.__workspaceHash = dialect.workspaceHash
        self.__username = dialect.username
        self.__password = dialect.password
        self.__bfsMuniciaplity = dialect.bfsMunicipality
        self.__snapshotTopic = dialect.snapshotTopic
        self.__sessionid = None
        self.__workspaceSnapshots = [item["title"] for item in list(self)]
        self.__dialect = dialect

    def __iter__(self):
        snapshotsInWorkspace = []

        if self.__workspaceHash:
            query = gql(
                """
                query getsnapshotsinworkspace($wshash: ID!) {
                    workspace(id:$wshash){
                        snapshots{
                            pk
                            title
                        }
                    }
                }
                """
            )

            params = {"wshash": self.__dfour_id(self.__workspaceHash, False)}

            results = self.__make_dfour_request(query, params)
            if results["workspace"]["snapshots"]:
                snapshotsInWorkspace = results["workspace"]["snapshots"]

        return iter(snapshotsInWorkspace)

    # Read
    def read_package(self, **options):
        # Provide a GraphQL query
        query = gql(
            """
            query getsnapshot($hash: ID!) {
                snapshot(id: $hash) {
                    data
                }
            }
            """
        )

        params = {"hash": self.__dfour_id(self.__snapshotHash)}

        result = self.__make_dfour_request(query, params)

        if result["snapshot"]:
            pkg = Package(descriptor=result["snapshot"]["data"])
            # for res in pkg.resources:
            #     if res["mediatype"] == "application/geo+json":
            return pkg

        note = (
            f'Snapshot with hash "{self.__snapshotHash}" on {self.__url} doesn\'t exist'
        )
        raise FrictionlessException(errors.StorageError(note=note))

    def read_resource(self, name):
        pkg = self.read_package()
        return pkg.get_resource(name)

    # Write

    def write_package(self, package, *, force, **options):
        if self.__workspaceHash and self.__username and self.__password:
            self.__dfour_login()
            if self.__sessionid:
                # Check existing
                if self.__snapshotHash:
                    pk = self.__snapshotHash
                elif (
                    not self.__snapshotHash
                    and package.title in self.__workspaceSnapshots
                ):
                    # Snapshot already exists
                    pk = [
                        item["pk"]
                        for item in list(self)
                        if item["title"] == package.title
                    ][0]
                else:
                    if self.__snapshotTopic and self.__bfsMuniciaplity:
                        query = gql(
                            """
                            mutation updatesnapshot($data: SnapshotMutationInput!) {
                                snapshotmutation(input: $data) {
                                    snapshot {
                                        pk
                                    }
                                }
                            }
                            """
                        )

                        params = {
                            "data": {
                                "title": package.title,
                                "topic": self.__snapshotTopic,
                                "bfsNumber": self.__bfsMuniciaplity,
                                "wshash": self.__dfour_id(self.__workspaceHash),
                            }
                        }

                        result = self.__make_dfour_request(
                            query,
                            params,
                            self.__dfour_session.cookies,
                            self.__dfour_session.headers,
                        )

                        if result["snapshotmutation"]["snapshot"]["pk"]:
                            pk = result["snapshotmutation"]["snapshot"]["pk"]
                    else:
                        note = f'Uploading "{package.title}" on {self.__url} requires a municiaplity bfs number and a snapshot topic, set one via the DfourDialect.'
                        raise FrictionlessException(errors.StorageError(note=note))
                self.__upload_file(package, pk)
            else:
                note = f'Uploading "{package.title}" on {self.__url} requires valid login credentials.'
                raise FrictionlessException(errors.StorageError(note=note))
        else:
            note = f'Uploading "{package.title}" on {self.__url} needs a workspace hash and login credentials.'
            raise FrictionlessException(errors.StorageError(note=note))

    # helpers

    def __make_dfour_request(self, query, params, cookies=None, headers=None):
        transport = RequestsHTTPTransport(
            url=self.__endpoint, headers=headers, cookies=cookies
        )
        client = Client(
            transport=transport,
            fetch_schema_from_transport=True,
        )
        return client.execute(query, variable_values=params)

    def __upload_file(self, package, pk):
        uploadUrl = f"{self.__url}/api/v1/snapshots/{pk}/"

        files = [
            (
                "data_file",
                (f"{pk}-{package.name}.json", json.dumps(package), "application/json"),
            )
        ]
        headers = {
            "X-CSRFToken": self.__get_token(),  # CORS-Token from above
        }

        self.__dfour_session.request(
            "PATCH", uploadUrl, headers=headers, files=files
        )  # submit the PATCH request
        # note = f'upload failed. {r.text}'
        # raise FrictionlessException(errors.StorageError(note=note))

    # Internal

    def __get_token(self):
        return self.__dfour_session.cookies["csrftoken"]

    def __dfour_id(self, hash, snapshot=True):
        if snapshot:
            prefix = "SnapshotNode"
        else:
            prefix = "WorkspaceNode"
        return base64.b64encode(f"{prefix}:{hash}".encode("ascii")).decode("ascii")

    def __dfour_session(self):
        self.__dfour_session = requests.session()

    def __dfour_login(self):
        self.__dfour_session()
        self.__dfour_session.get(f"{self.__url}/account/login/")
        if self.__username and self.__password and self.__get_token():
            if self.__username.startswith("env:"):
                username = os.environ.get(self.__username[4:])
            else:
                username = self.__username

            if self.__password.startswith("env:"):
                password = os.environ.get(self.__password[4:])
            else:
                password = self.__password

            headers = {
                "Cookie": "; ".join(
                    [f"csrftoken={self.__get_token()}"]
                ),  # make sure the CORS-Token cookie is set
                "Content-Type": "application/x-www-form-urlencoded",  # set the Content Type to form-urlencoded
                "Referer": f"{self.__url}/account/login/",
            }
            payload = {
                "csrfmiddlewaretoken": self.__get_token(),  # set the the CORS-Token
                "username": username,
                "password": password,
            }

            payload = urllib.parse.urlencode(
                payload
            )  # encode payload as www-form-urlencoded

            self.__dfour_session.post(
                f"{self.__url}/account/login/", data=payload, headers=headers
            )

            if "sessionid" in self.__dfour_session.cookies.keys():
                self.__sessionid = self.__dfour_session.cookies["sessionid"]
            else:
                note = f"Couldn't obtain {self.__url} session. Current cookies: {self.__dfour_session.cookies}, {self.__get_token()} {username} {password}"
                raise FrictionlessException(errors.StorageError(note=note))
