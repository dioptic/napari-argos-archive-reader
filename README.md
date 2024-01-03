# napari-argos-archive-reader

[![License MIT](https://img.shields.io/pypi/l/napari-argos-archive-reader.svg?color=green)](https://github.com/dioptic/napari-argos-archive-reader/raw/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/napari-argos-archive-reader.svg?color=green)](https://pypi.org/project/napari-argos-archive-reader)
[![Python Version](https://img.shields.io/pypi/pyversions/napari-argos-archive-reader.svg?color=green)](https://python.org)
[![tests](https://github.com/dioptic/napari-argos-archive-reader/workflows/tests/badge.svg)](https://github.com/dioptic/napari-argos-archive-reader/actions)
[![codecov](https://codecov.io/gh/dioptic/napari-argos-archive-reader/branch/main/graph/badge.svg)](https://codecov.io/gh/dioptic/napari-argos-archive-reader)
[![napari hub](https://img.shields.io/endpoint?url=https://api.napari-hub.org/shields/napari-argos-archive-reader)](https://napari-hub.org/plugins/napari-argos-archive-reader)

A plugin to read Dioptic ARGOS archive files

----------------------------------

This repo contains a reader plugin for [DIOPTIC ARGOS](https://www.dioptic.de/en/argos-en/) Archive files, which
have `.zip` file extension.
Individual ARGOS layers are grouped into napari layer with stacks according to
their illumination, stage XY position and Z-stack information.

The plugin implements delayed reading using `dask.delayed` so that one can quickly
see the contents even for large archives with many layers. Note!: switching to
volume rendering or swapping axes can trigger the loading of all ARGOS layers, which
can take a long time for large archives.

[ARGOS](https://www.dioptic.de/en/argos-en/) is an automated system
for surface inspection according to ISO 10110-7.
<!--
Don't miss the full getting started guide to set up your new package:
https://github.com/napari/cookiecutter-napari-plugin#getting-started

and review the napari docs for plugin developers:
https://napari.org/stable/plugins/index.html
-->

## Installation

You can install `napari-argos-archive-reader` via [pip]:

    pip install napari-argos-archive-reader

To install latest development version :

    pip install git+https://github.com/dioptic/napari-argos-archive-reader.git

## License

Distributed under the terms of the [MIT] license,
"napari-argos-archive-reader" is free and open source software

[MIT]: http://opensource.org/licenses/MIT
[pip]: https://pypi.org/project/pip/
