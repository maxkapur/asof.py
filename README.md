# asof.py

Get latest version of a Python package (PyPI, conda) as of a certain date.

## Usage

What versions of Numba were available on October 15, 2025?

```shell
$ asof 2025-10-15 numba
Query: numba (PyPI name)
Conda name: numba · PyPI name: numba
numba v0.63.0b1 published Mon 10/06/25 10:22:54 to https://pypi.org
numba v0.62.1 published Mon 09/29/25 10:46:31 to https://pypi.org
numba v0.62.1 published Mon 09/29/25 11:59:57 to conda-forge
```

By default, matches are based on the PyPI name. You can search by the conda
package name instead:

```shell
$ asof 2019-10-10 web.py --query-type=conda
Query: web.py (conda name)
Conda name: web.py · PyPI name: web-py
web-py v0.40 published Fri 09/27/19 07:44:13 to https://pypi.org
web.py v0.40 published Sun 09/29/19 15:30:21 to conda-forge
```

Or by import name:

```shell
$ asof 2019-10-10 web --query-type=import
Query: web (import name)
Conda name: web.py · PyPI name: web-py
web-py v0.40 published Fri 09/27/19 07:44:13 to https://pypi.org
web.py v0.40 published Sun 09/29/19 15:30:21 to conda-forge
```

There are colors in the terminal output, but I can't show them here :)

## Motivation

I use this to pin dependencies for new projects to the latest versions available
under a
[dependency cooldown](https://blog.yossarian.net/2025/11/21/We-should-all-be-using-dependency-cooldowns)
rule.

## Installation

Easy way:

```shell
pipx install 'git+https://github.com/maxkapur/asof.py'
```

For development:

```shell
python -m venv ./venv
# Activate the venv, then:
python -m pip install --editable .[dev]
```
