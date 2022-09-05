"""Main module."""
import platform
from typing import Any, Optional

import pandas as pd
from azure.identity import InteractiveBrowserCredential
from azure.identity.aio import DefaultAzureCredential as AIODefaultAzureCredential

from azure_datalake_utils.exepctions import ArchivoNoEncontrado, ExtensionIncorrecta


class Datalake(object):
    """Clase para representar operaciones de Datalake."""

    def __init__(self, datalake_name: str, tenant_id: str, account_key: Optional[str] = None) -> None:
        """Clase para interactuar con Azure Dalake.

        Args:
            datalake_name: nombre de la cuenta de Azure Datalake Gen2.
            tenant_id: Identificador del tenant, es valor es proporcionado
                por arquitectura de datos, debe conservarse para un
                correcto funcionamiento.
            account_key: key de la cuenta. Por defecto es None y es ignorado

        """
        self.datalake_name = datalake_name

        if account_key is None:

            self.tenant_id = tenant_id
            credentials = InteractiveBrowserCredential(tenant_id=self.tenant_id)
            credentials.authenticate()
            self._credentials = credentials
            # TODO: verificar https://github.com/fsspec/adlfs/issues/270
            # para ver como evoluciona y evitar este condicional.
            if platform.system().lower() != 'windows':
                self.storage_options = {'account_name': self.datalake_name, 'anon': False}
            else:
                self.storage_options = {
                    'account_name': self.datalake_name,
                    'anon': False,
                    'credential': AIODefaultAzureCredential(),
                }

        else:
            self.storage_options = {'account_name': self.datalake_name, 'account_key': account_key}

    @classmethod
    def from_account_key(cls, datalake_name: str, account_key: str):
        """Opcion de inicializar con account key."""
        return cls(datalake_name=datalake_name, account_key=account_key, tenant_id=None)

    def read_csv(self, ruta: str, **kwargs: Optional[Any]) -> pd.DataFrame:
        """Leer un archivo CSV desde la cuenta de datalake.

        Esta función hace una envoltura de [pd.read_csv].
        usar la documentación de la función para determinar parametros adicionales.

        [pd.read_csv]: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

        Args:

            ruta: Ruta a leeder el archivo, debe contener una referencia a un archivo
                `.csv` o `.txt`. Recordar que la ruta debe contener esta estructura:
                `{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.csv`.

            **kwargs: argumentos a pasar a pd.read_csv. El unico argumento que es ignorado
                es storage_options.

        Returns:
            Dataframe con la informacion del la ruta.
        """
        if 'storage_options' in kwargs:
            kwargs.pop('storage_options')

        if not self._verificar_extension(ruta, '.csv', '.txt', '.tsv'):
            raise ExtensionIncorrecta(ruta)

        try:
            df = pd.read_csv(f"az://{ruta}", storage_options=self.storage_options, **kwargs)
        except IndexError:
            raise ArchivoNoEncontrado(ruta)

        return df

    def read_excel(self, ruta: str, **kwargs: Optional[Any]) -> pd.DataFrame:
        """Leer un archivo CSV desde la cuenta de datalake.

        Esta función hace una envoltura de [pd.read_excel].
        Por favor usar la documentación de la función para determinar parametros adicionales.

        [[pd.read_excel]]:(https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)

        Args:
            ruta: Ruta a leeder el archivo, debe contener una referencia a un archivo
                `.xlsx` o `.xls`. Recordar que la ruta debe contener esta estructura:
                `{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.xlsx`.
            **kwargs: argumentos a pasar a pd.read_csv.


        Returns:
            Dataframe con la informacion del la ruta.
        """
        if 'storage_options' in kwargs:
            kwargs.pop('storage_options')

        if 'engine' in kwargs:
            kwargs.pop('storage_options')

        if not self._verificar_extension(ruta, '.xlsx', '.xls'):
            raise ExtensionIncorrecta(ruta)

        try:
            df = pd.read_excel(f"az://{ruta}", engine='openpyxl', storage_options=self.storage_options, **kwargs)
        except IndexError:
            raise ArchivoNoEncontrado(ruta)

        return df

    def write_csv(self, df: pd.DataFrame, ruta, **kwargs: Optional[Any]) -> None:
        """Escribir al archivo."""
        if not self._verificar_extension(ruta, '.csv', '.txt', '.tsv'):
            raise ExtensionIncorrecta(ruta)
        df.to_csv(f"az://{ruta}", storage_options=self.storage_options, **kwargs)

    def write_excel(self, df: pd.DataFrame, ruta, **kwargs: Optional[Any]) -> None:
        """Escribir al archivo al datalake."""
        if not self._verificar_extension(ruta, '.xlsx', '.xls'):
            raise ExtensionIncorrecta(ruta)
        df.to_excel(f"az://{ruta}", storage_options=self.storage_options, **kwargs)

    def _verificar_extension(self, ruta: str, *extensiones):
        """Metodo para verificar extensiones."""
        for ext in extensiones:
            verificar = ruta.endswith(ext)

            if verificar:
                return verificar

        return verificar
