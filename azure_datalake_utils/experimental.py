"""Funciones experimentales.

Estos metodos no son testeados y se esperan en futuras veriones incorporar esta funcionalidad o
removerla del todo.

"""
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
import asyncio
import tempfile
import uuid
from typing import Any, Optional

import pandas as pd
from adlfs import AzureBlobFileSystem
from azure.identity import InteractiveBrowserCredential


def read_excel_with_client(ruta: str, account_name: str, account_key: str, **kwargs: Optional[Any]):
    """Metodo para leer Excel descargado el archivo a un temporal."""
    fs = AzureBlobFileSystem(account_name=account_name, account_key=account_key)
    # TODO: en el futuro si de verdad se necesita el nombre del archivo, hay usa otro metodo
    # que se mas agnostico que asumir que se puede hacer sin un split.
    nombre_archivo = ruta.split("/")[-1]
    tempfilename = f"{tempfile.gettempdir()}/{str(uuid.uuid4())}_{nombre_archivo}"
    fs.download(ruta, tempfilename)

    return pd.read_excel(tempfilename, **kwargs)


class AioCredentialWrapper:
    """Clase para envolver cualquier credencial de Azure-sdk-for-python.

    Esto se hace mientras en el futuro esta funcionalidad se integra al SDK.

    TODO: verificar como evoluciona este issue: https://github.com/Azure/azure-sdk-for-python/issues/19943.
    """

    def __init__(self, credential: InteractiveBrowserCredential):
        """Constructor de la clase."""
        self._credential = credential

    async def get_token(self, *scopes: str, **kwargs):
        """Metodo para obtener el token."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._credential.get_token(*scopes, **kwargs),
        )
