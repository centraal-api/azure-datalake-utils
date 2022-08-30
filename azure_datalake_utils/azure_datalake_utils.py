"""Main module."""

import pandas as pd
from azure.identity import InteractiveBrowserCredential

from azure_datalake_utils.exepctions import ArchivoNoEncontrado


class Datalake(object):
    """Clase para representar operaciones de Datalake."""

    def __init__(self, datalake_name: str, tenant_id: str) -> None:
        """Clase para interactuar con Azure Dalake.

        Args:
            datalake_name: nombre de la cuenta de Azure Datalake Gen2.
            tenant_id: Identificador del tenant, es valor es proporcionado
                por arquitectura de datos, debe conservarse para un
                correcto funcionamiento.

        """
        self.datalake_name = datalake_name
        self.tenant_id = tenant_id
        credentials = InteractiveBrowserCredential(tenant_id=self.tenant_id)
        credentials.authenticate()
        self._credentials = credentials
        self.storage_options = {'account_name': self.datalake_name, 'anon': False}

    def read_csv(self, ruta: str, **kwargs) -> pd.DataFrame:
        """Leer un archivo CSV desde la cuenta de datalake.

        # noqa: E501
        Esta función hace una envoltura de [pandas.read_csv](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html).
        Por favor usar la documentación de la función para determinar parametros adicionales.
        Args:
            ruta: Ruta a leeder el archivo, debe contener una referencia a un archivo
                `.csv` o `.txt`. Recordar que la ruta debe contener esta estructura:
                `{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.csv`.
            **kwargs: argumentos a pasar a pd.read_csv.

        Returns:
            Dataframe con la informacion del la ruta.
        """
        try:
            df = pd.read_csv(f"az://{ruta}", storage_options=self.storage_options, **kwargs)
        except IndexError:
            raise ArchivoNoEncontrado(ruta)

        return df
