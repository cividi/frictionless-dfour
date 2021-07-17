from frictionless import Plugin, Step, system, errors
from frictionless.errors import FrictionlessException
from .dfour import DfourDialect


# Plugin


class StorageWritePlugin(Plugin):
    code = "storage"
    status = "experimental"

    def create_step(self, descriptor):
        if descriptor.get("code") == "storage":
            return storage_step(descriptor)


# Pipeline Step


class storage_step(Step):
    """Write a package to `storage`"""

    code = "storage"

    def __init__(self, descriptor=None, *, storage=None, endpoint=None, dialect=None):
        self.setinitial("storage", storage)
        self.setinitial("endpoint", endpoint)
        self.setinitial("dialect", dialect)
        super().__init__(descriptor)

    def transform_package(self, package):
        try:
            storage = system.create_storage(
                self.get("storage"),
                self.get("endpoint"),
                dialect=DfourDialect(self.get("dialect")))
            storage.write_package(package.to_copy(), force=False)
        except Exception as e:
            note = f'Couldn\'t upload "{package.title}" on {self.get("endpoint")}: {type(e)} {e}'
            raise FrictionlessException(errors.StepError(note=note))

    # Metadata

    metadata_profile = {  # type: ignore
        "type": "object",
        "required": ["storage", "endpoint", "dialect"],
        "properties": {
            "storage": {"type": "string"},
            "endpoint": {"type": "string"},
            "dialect": {"type": "object"},
        },
    }
