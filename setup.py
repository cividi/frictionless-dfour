import os
import io
from setuptools import setup, find_packages


# Helpers


def read(*paths):
    """Read a text file."""
    basedir = os.path.dirname(__file__)
    fullpath = os.path.join(basedir, *paths)
    contents = io.open(fullpath, encoding="utf-8").read().strip()
    return contents


# Prepare


PACKAGE = "frictionless_dfour"
NAME = PACKAGE.replace("_", "-")
TESTS_REQUIRE = [
    "mypy",
    "moto",
    "black",
    "jinja2",
    "pylama",
    "pytest",
    "ipython",
    "pymysql",
    "livemark",
    "psycopg2",
    "pytest-cov",
    "pytest-vcr",
    "pytest-only",
    "oauth2client",
    "requests-mock",
    "python-dotenv",
    "pydoc-markdown",
    "docstring-parser",
]
EXTRAS_REQUIRE = {
    "dev": TESTS_REQUIRE,
}
INSTALL_REQUIRES = [
    "gql",
    "frictionless",
    "dictdiffer",
    "tzlocal==2.1"
]
README = read("README.md")
VERSION = read(PACKAGE, "assets", "VERSION")
PACKAGES = find_packages(exclude=["tests"])
ENTRY_POINTS = {"console_scripts": ["dfour = frictionless_dfour.__main__:program"]}


# Run


setup(
    name=NAME,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require=EXTRAS_REQUIRE,
    entry_points=ENTRY_POINTS,
    zip_safe=False,
    long_description=README,
    long_description_content_type="text/markdown",
    description="Dfour Storage for frictionless-py",
    author="Thorben Westerhuys",
    author_email="thorben@cividi.ch",
    url="https://github.com/cividi/frictionless-dfour",
    license="MIT",
    keywords=[
        "data validation",
        "frictionless data",
        "open data",
        "json schema",
        "json table schema",
        "data package",
        "tabular data package",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)