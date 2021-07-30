# import pytest
from frictionless import Package, system


def test_dfour_storage_types(dfour_url, dfour_dialect):

    source = Package("data/types.json")
    source.title = "Upload Test"
    storage = system.create_storage("dfour", dfour_url, dialect=dfour_dialect)
    storage.write_package(source.to_copy(), force=True)
    target = storage.read_package()

    # Assert metadata
    assert target.get_resource("types").schema == source.get_resource("types").schema

    # Assert data
    # assert target.get_resource("types").read_rows() == source.get_resource("types").read_rows()
