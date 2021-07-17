from .dfour import *
from .storage import StorageWritePlugin
from frictionless import system

system.register("storage-write", StorageWritePlugin())
