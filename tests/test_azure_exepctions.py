#!/usr/bin/env python
"""Tests for `azure_datalake_utils.exepctions` package."""

from azure_datalake_utils.exepctions import ArchivoNoEncontrado


def test_ArchivoNoEncontrado():
    """Test para ArchivoNoEncontrado."""
    e = ArchivoNoEncontrado("ruta")
    assert str(e) == "ruta -> La ruta no es se encuentra, verificar que el archivo(s) existan."
