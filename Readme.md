[![Build](https://img.shields.io/github/workflow/status/cividi/frictionless-dfour/main/main)](https://github.com/cividi/frictionless-dfour/actions)
[![Coverage](https://img.shields.io/codecov/c/github/cividi/frictionless-dfour/main)](https://codecov.io/gh/cividi/frictionless-dfour)
[![Registry](https://img.shields.io/pypi/v/frictionless_dfour.svg)](https://pypi.python.org/pypi/frictionless_dfour)
[![Codebase](https://img.shields.io/badge/github-main-brightgreen)](https://github.com/cividi/frictionless-dfour)

# Frictionless dfour

An extension to add [dfour](https://github.com/cividi/spatial-data-package-platform) storage support in [frictionless-py](https://framework.frictionlessdata.io).

## Guide

### Load the package

#### Release version

```sh
pip install frictionless_dfour
```

#### Dev version

```sh
python3 -V # should be > 3.6

# download project
git clone git@github.com:cividi/frictionless-dfour.git
cd frictionless-dfour

# Load dynamic dev version
make dev # or python3 -m pip install -e .
```

### Read from dfour

```python
from frictionless import system
from pprint import pprint

source = "https://sandbox.dfour.space"
dialect = DfourDialect(snapshot="<SNAPSHOTHASH>", workspace="<WORKSPACEHASH>", credentials={"username":"<YOURUSER>","password":"<YOURPASSWORD>"})

storage = system.create_storage("dfour", source, dialect=dialect)
pkg = storage.read_package()
```

### Write to dfour

```python
from frictionless import system
from pprint import pprint

target = "https://sandbox.dfour.space"
dialect = DfourDialect(snapshot="<SNAPSHOTHASH>", workspace="<WORKSPACEHASH>", credentials={"username":"<YOURUSER>","password":"<YOURPASSWORD>"})

storage = system.create_storage("dfour", target, dialect=dialect)
storage.write_package(pkg.to_copy(), force=True)
```
