from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

with open("fabduckdb/_version.py", "r") as file:
    code = file.read()
    exec(code)
    _version = __version__  # type: ignore # noqa

setup(
    name="fabduckdb",
    version=_version,  # type: ignore # noqa
    description="Fabulous DuckDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/paultiq/fabduckdb",
    author="Paul",
    author_email="paul@iqmo.com",
    classifiers=[],
    keywords="DuckDB, Fabulous, Loops, Dynamic",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=["duckdb>=0.8.0", "sqlparse", "jinja2"],
)
