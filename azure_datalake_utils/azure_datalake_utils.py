"""Main module."""
import platform
import re
from typing import Any, Dict, Optional, Union, List

import numpy as np
import pandas as pd
import azure_datalake_utils.experimental as exp

from azure.identity import InteractiveBrowserCredential
from azure.identity.aio import DefaultAzureCredential as AIODefaultAzureCredential
from adlfs import AzureBlobFileSystem
from azure_datalake_utils.exepctions import ExtensionIncorrecta, raiseArchivoNoEncontrado
from azure_datalake_utils.partitions import HivePartitiion


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
            self.fs = AzureBlobFileSystem(account_name=self.datalake_name, account_key=account_key)

    @classmethod
    def from_account_key(cls, datalake_name: str, account_key: str):
        """Opcion de inicializar con account key."""
        return cls(datalake_name=datalake_name, account_key=account_key, tenant_id=None)

    @raiseArchivoNoEncontrado
    def read_csv(self, ruta: Union[str, List[str]], **kwargs: Optional[Any]) -> pd.DataFrame:
        """Leer un archivo CSV desde la cuenta de datalake.

        Esta funci??n hace una envoltura de [pd.read_csv].
        usar la documentaci??n de la funci??n para determinar parametros adicionales.

        [pd.read_csv]: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

        Args:

            ruta: Ruta a leeder el archivo, debe contener una referencia a un archivo
                `.csv` o `.txt`. Recordar que la ruta debe contener esta estructura:
                `{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.csv`.
                NUEVO en version 0.5:
                Tambien acepta una lista de archivos terminados en csv. ejemplo:
                ```
                [{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.csv,
                {NOMBRE_CONTENEDOR}/{RUTA2}/{nombre o patron}.csv]
                ```

            **kwargs: argumentos a pasar a pd.read_csv. El unico argumento que es ignorado
                es storage_options.

        Returns:
            Dataframe con la informacion del la ruta.
        """
        if 'storage_options' in kwargs:
            kwargs.pop('storage_options')

        if type(ruta) == str:
            self._verificar_extension(ruta, '.csv', '.txt', '.tsv')
            df = pd.read_csv(f"az://{ruta}", storage_options=self.storage_options, **kwargs)
        else:
            [self._verificar_extension(r, '.csv', '.txt', '.tsv') for r in ruta]
            rutas = [pd.read_csv(f"az://{r}", storage_options=self.storage_options, **kwargs) for r in ruta]
            df = pd.concat(rutas, ignore_index=True)

        return df

    @raiseArchivoNoEncontrado
    def read_excel(self, ruta: str, experimental: bool = False, **kwargs: Optional[Any]) -> pd.DataFrame:
        """Leer un archivo Excel desde la cuenta de datalake.

        Esta funci??n hace una envoltura de [pd.read_excel].
        Por favor usar la documentaci??n de la funci??n para determinar parametros adicionales.

        [[pd.read_excel]]:(https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)

        Args:
            ruta: Ruta a leeder el archivo, debe contener una referencia a un archivo
                `.xlsx` o `.xls`. Recordar que la ruta debe contener esta estructura:
                `{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.xlsx`.
            force_client: Bandera para forzar el uso del cliente. Es flag todavia es experimental
                Y en futuras versiones se va ha eliminar. Solo funciona con un solo archivo de excel.
            **kwargs: argumentos a pasar a pd.read_excel.


        Returns:
            Dataframe con la informacion del la ruta.
        """
        if 'storage_options' in kwargs:
            kwargs.pop('storage_options')

        if 'engine' in kwargs:
            kwargs.pop('engine')

        self._verificar_extension(ruta, '.xlsx', '.xls')

        # TODO: esto es algo temporal y se debe analizar si se puede remover. Esta bandera fue necesario debido a:
        # 1. Si no se usa el cliente para descargar el excel cuando se modifica el archivo de
        # excel y no se ha reiniciado
        # el runtime se provoca el error: `BadZipFile("File is not a zip file")`.
        # 2. Se debe analizar como integrar en windows donde se usa credenciales asociadas al Active directory.
        if experimental:
            df = exp.read_excel_with_client(ruta, self.datalake_name, self.storage_options['account_key'], **kwargs)
            return df
        ###############

        df = pd.read_excel(f"az://{ruta}", engine='openpyxl', storage_options=self.storage_options, **kwargs)

        return df

    @raiseArchivoNoEncontrado
    def read_json(self, ruta: str, **kwargs: Optional[Any]) -> pd.DataFrame:
        """Leer un archivo Json desde la cuenta de datalake.

        Esta funci??n hace una envoltura de [pd.read_json].
        Por favor usar la documentaci??n de la funci??n para determinar parametros adicionales.

        [[pd.read_json]]:(https://pandas.pydata.org/docs/reference/api/pandas.read_json.html)

        Args:
            ruta: Ruta a leeder el archivo, debe contener una referencia a un archivo
                `.json` . Recordar que la ruta debe contener esta estructura:
                `{NOMBRE_CONTENEDOR}/{RUTA}/{nombre o patron}.json`.
            **kwargs: argumentos a pasar a pd.read_json.


        Returns:
            Dataframe con la informacion del la ruta.
        """
        if 'storage_options' in kwargs:
            kwargs.pop('storage_options')

        self._verificar_extension(ruta, '.json')

        df = pd.read_json(f"az://{ruta}", storage_options=self.storage_options, **kwargs)

        return df

    def read_csv_with_partition(
        self,
        ruta: str,
        partition_cols: Dict[str, List[str]] = None,
        partition_exclusion: Dict[str, List[str]] = None,
        partition_inclusion: Dict[str, List[str]] = None,
        last_modified_last_level: bool = True,
        **kwargs: Optional[Any],
    ) -> pd.DataFrame:
        """Leer un archivo CSV desde la cuenta de datalake con particiones Hive.

        Una partici??n tipo Hive son archivos almacenados de la forma
        /ruta/to/archivo/particion_1=1/particion2=2/archivo_con_info.extension.

        **IMPORTANTE**: por el momento el funcionamiento de esta funci??n es solo soportada
        si el objeto fue creado mediante `from_account_key`.

        Esta funci??n hace una envoltura de `read_csv` que asu vez usa [pd.read_csv].
        usar la documentaci??n de la funci??n para determinar parametros adicionales.

        [pd.read_csv]: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

        Args:

            ruta: Ruta a leeder el archivo, esta ruta debe contener archivos guardados siguiendo
            la convecci??n de la partici??n Hive. Ejemplo si la estructura de los archivos es:

            - contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv
            - contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv
            - contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv

            `ruta` debe ser `contenedor/ruta/al/archivo/`.

            partition_cols: Definir que particionews incluir, se asume que se sabe a priori la
                estructura. Si se pasa `None`, se activa el descubrimiento automatico de las particiones.
            partition_exclusion: Definir que particiones excluir, es m??s util cuando no se ha definido
                `partition_cols=None`.
            partition_inclusion: Definir la particiones a incluir, es m??s util cuando no se ha definido
                `partition_cols=None`.
            last_modified_last_level: Define si el ultimo nivel no se tiene en cuenta para particiones y
                se carga el archivo que fue modificado de manera m??s reciente.
            **kwargs: argumentos a pasar a pd.read_csv. El unico argumento que es ignorado
                es storage_options.

        Returns:
            Dataframe con la informacion del la ruta.
        """
        if not ruta.endswith("/"):
            raise ValueError("ruta debe finalizar en /")

        particiones = HivePartitiion(
            ruta=ruta,
            partition_cols=partition_cols,
            partition_exclusion=partition_exclusion,
            partition_inclusion=partition_inclusion,
            last_modified_last_level=last_modified_last_level,
            fs=self.fs,
        )
        list_of_files = particiones.get_partition_list()
        list_of_dfs = []
        for path_, particion in zip(list_of_files, particiones.get_partition_files()):
            particiones_cols = particion[1]
            df = self.read_csv(path_, **kwargs).assign(**particiones_cols)
            list_of_dfs.append(df)

        return pd.concat(list_of_dfs, ignore_index=True)

    def write_csv(self, df: pd.DataFrame, ruta, **kwargs: Optional[Any]) -> None:
        """Escribir al archivo."""
        if not self._verificar_extension(ruta, '.csv', '.txt', '.tsv'):
            raise ExtensionIncorrecta(ruta)

        sep = kwargs.get('sep', ',')
        df_to_write = self._limpiar_df_cols_str(df, sep)
        df_to_write.to_csv(f"az://{ruta}", storage_options=self.storage_options, **kwargs)

    def write_excel(self, df: pd.DataFrame, ruta, **kwargs: Optional[Any]) -> None:
        """Escribir al archivo al datalake."""
        if not self._verificar_extension(ruta, '.xlsx', '.xls'):
            raise ExtensionIncorrecta(ruta)
        df.to_excel(f"az://{ruta}", storage_options=self.storage_options, **kwargs)

    def write_json(self, df: pd.DataFrame, ruta, **kwargs: Optional[Any]) -> None:
        """Escribir al archivo al datalake."""
        if not self._verificar_extension(ruta, '.json'):
            raise ExtensionIncorrecta(ruta)
        df.to_json(f"az://{ruta}", storage_options=self.storage_options, **kwargs)

    def _verificar_extension(self, ruta: str, *extensiones):
        """Metodo para verificar extensiones."""
        for ext in extensiones:
            verificar = ruta.endswith(ext)
            if verificar:
                return True
        raise ExtensionIncorrecta(ruta)

    def _limpiar_df_cols_str(self, df: pd.DataFrame, sep: str = ",") -> pd.DataFrame:
        """Limpia las columnas string del dataframe."""
        types = df.dtypes
        string_columns = list(types[types == np.array([object()]).dtype].index)
        esc_sep = re.escape(sep)
        df_res = df.copy()
        df_res[string_columns] = (
            df[string_columns]
            .replace(esc_sep, " ", regex=True)
            .replace("\r", " ", regex=True)
            .replace("\n", " ", regex=True)
        )
        return df_res
