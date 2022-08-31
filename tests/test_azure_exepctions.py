#!/usr/bin/env python
"""Tests for `azure_datalake_utils.exepctions` package."""

from azure_datalake_utils.exepctions import ArchivoNoEncontrado, ExtensionIncorrecta


def test_ArchivoNoEncontrado():
    """Test para ArchivoNoEncontrado."""
    e = ArchivoNoEncontrado("ruta")
    assert str(e) == "ruta -> La ruta no es se encuentra, verificar que el archivo(s) existan."


def test_ExtensionIncorrecta():
    """Test para ArchivoNoEncontrado."""
    e = ExtensionIncorrecta("ruta")
    assert str(e) == "ruta -> La ruta no termina en la extension correcta."
