"""Tests for `azure_datalake_utils.exepctions` package."""
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from azure_datalake_utils.exepctions import ArchivoNoEncontrado, ExtensionIncorrecta


def test_raise_ArchivoNoEncontrado():
    """Test para ArchivoNoEncontrado."""
    e = ArchivoNoEncontrado("ruta")
    assert str(e) == "ruta -> La ruta no es se encuentra, verificar que el archivo(s) existan."


def test_raise_ExtensionIncorrecta():
    """Test para ArchivoNoEncontrado."""
    e = ExtensionIncorrecta("ruta")
    assert str(e) == "ruta -> La ruta no termina en la extension correcta."
