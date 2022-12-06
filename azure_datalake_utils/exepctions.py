"""Exepciones personalizadas."""
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
from azure.core.exceptions import ResourceNotFoundError


class ArchivoNoEncontrado(Exception):
    """Exepction cuando un archivo no es encontrado en el Datalake."""

    def __init__(self, ruta, message="La ruta no es se encuentra, verificar que el archivo(s) existan."):
        """Constructor de la expecion."""
        self.ruta = ruta
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        """Imprimir el mensaje de la exepecion."""
        return f'{self.ruta} -> {self.message}'


class ExtensionIncorrecta(Exception):
    """Exepction cuando la ruta tiene extension incorrecta."""

    def __init__(self, ruta, message="La ruta no termina en la extension correcta."):
        """Constructor de la expecion."""
        self.ruta = ruta
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        """Imprimir el mensaje de la exepecion."""
        return f'{self.ruta} -> {self.message}'


def raiseArchivoNoEncontrado(func):
    """Decorador para hacer triger la exepción `ArchivoNoEncontrado`."""

    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (IndexError, FileNotFoundError, ResourceNotFoundError):
            raise ArchivoNoEncontrado("archivo no encontrado")

    return inner_function
