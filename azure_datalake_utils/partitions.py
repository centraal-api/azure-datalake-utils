"""Utilidades para particiones."""
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
import itertools
import logging
from typing import Dict, List, Tuple

from adlfs import AzureBlobFileSystem


class HivePartitiion:
    """Clase para tratar con la particiones tipo hive.

    Una partición tipo hive son archivos que son almacenados de la forma
    /ruta/to/archivo/particion_1=1/particion2=2/archivo_con_info.extension Esta
    Clase ayuda a descubrir los archivos que se quieren leer y poder cargar
    las columnas como particiones.

    Tambien incluye una opción para filtrar las particiones que se quieren cargar,
    de igual manera se puede especificar  una partición especifica.

    Otra opción tambien puede ser usada para leer el archivo mas reciente en el ultimo nivel de
    la partición.

    **NOTA**: solo descubre un arhivo por carperta, es decir, si hay multiples archivos
    en el nivel más bajo, solo garantiza leer uno solo.

    Args:
        ruta: ruta donde se encuentran los archivos almancenados.

    Attributes:
        ruta: atributo con la ruta a descubir.
        partition_cols: dict con las particiones a leer de manera especifica.
            Al pasar esta opción implica que hay NO hay inferencia de columnas.
        partition_exclusion: diccionarios con valores a filtrar en la particiónes.
        partition_inclusion: diccionarios con valores a incluir en la particiónes.
        last_modidfied_deeper_level: si se debe filtrar para obtener el mas reciente
            en el nivel más profundo.
    """

    def __init__(
        self,
        ruta: str,
        partition_cols: Dict[str, List[str]] = None,
        partition_exclusion: Dict[str, List[str]] = None,
        partition_inclusion: Dict[str, List[str]] = None,
        last_modified_last_level: bool = False,
        fs: AzureBlobFileSystem = None,
    ) -> None:
        """Constructor."""
        self.ruta = ruta
        self.container = ruta.split("/")[0]
        self.path_blob = ruta.replace(f"{self.container}/", "")
        self.partition_files: List[Tuple[str, Dict[str, str]]] = [{}]
        self.partition_cols = partition_cols
        self.partition_exclusion = partition_exclusion
        self.partition_inclusion = partition_inclusion
        self.last_modified_last_level = last_modified_last_level

        self.fs = fs
        self._make_partitions()

    def get_partition_files(self):
        """Getter para partition_files."""
        return self.partition_files

    def get_partition_list(self) -> List[str]:
        """Convierte a una lista de archivos a leer."""
        path_and_file = [(file_path_[0], self.dict_to_path(file_path_[1])) for file_path_ in self.partition_files]

        return [f'{self.ruta}{file_path_[1]}/{file_path_[0]}' for file_path_ in path_and_file]

    def dict_to_path(self, dict_) -> str:
        """Convierte diccionario a path."""
        return '/'.join('{}={}'.format(*p) for p in dict_.items())

    def _make_partitions(self) -> None:
        """Metodo para descubrir las paritciones."""
        if self.partition_cols is not None:
            self._make_partitions_using_partition_cols()
        else:
            self._discover()

        if self.partition_exclusion is not None:
            logging.debug("filtrando particiones por exclusion.")
            for name, value_to_exclude in self.partition_exclusion.items():
                try:
                    self.partition_files = [
                        partition for partition in self.partition_files if partition[1][name] not in value_to_exclude
                    ]
                except KeyError:
                    raise KeyError(f'{name} NO existe en las particiones!.')

        if self.partition_inclusion is not None:
            logging.debug("filtrando particiones por inclusion.")
            for name, value_to_exclude in self.partition_inclusion.items():
                try:
                    self.partition_files = [
                        partition for partition in self.partition_files if partition[1][name] in value_to_exclude
                    ]
                except KeyError:
                    raise KeyError(f'{name} NO existe en las particiones!.')

    def _make_partitions_using_partition_cols(self) -> None:
        """Metodo para constuir particiones con la partition_cols y asi evitar descubrirlas."""
        combinations = itertools.product(*(self.partition_cols[name] for name in self.partition_cols))

        partitions = []
        for combination in combinations:
            part = {k: v for k, v in zip(self.partition_cols.keys(), combination)}
            # TODO: Si es la mejor manera de hacer paths? a priori si por que se
            # esta trabajando con el fs de Datalake y no hay lios de diferentes
            # delimitadores.
            partition_path = "/".join([f"{k}={v}" for k, v in part.items()])

            if self.last_modified_last_level:
                files = self.fs.find(f"{self.ruta}{partition_path}/", detail=True)
                files = {k: v['last_modified'] for k, v in files.items()}
                logging.debug(f"archivos encontrados {files}")
                selected = sorted(files.items(), key=lambda x: x[1], reverse=True)[0][0]
                logging.debug(f"archivo seleccionado {selected}")
                file_name = "/".join(selected.split("/")[-2:])
                partitions.append((file_name, part))
            else:
                try:
                    details = self.fs.listdir(f"{self.ruta}{partition_path}/")
                except FileNotFoundError:
                    logging.debug(f"{part} No tiene archivos")
                    continue
                if len(details) < 1:
                    logging.debug(f"{part} No tiene archivos")
                    continue
                file_name = details[0]['name'].split("/")[-1]
                partitions.append((file_name, part))

        self.partition_files = partitions

    def _discover(self) -> None:
        """Inferir particiones a partir del esquema.

        NOTA: El metodo al descubrir particiones, todas las dejas en string.
        #TODO: verificar si vale la pena tratar de inferir formatos.
        """
        partition_files_ = []
        partition_files_as_dict = {}
        list_of_files = self.fs.find(self.ruta, detail=True)
        list_of_files = {k: v['last_modified'] for k, v in list_of_files.items()}
        list_of_files = dict(sorted(list_of_files.items(), key=lambda x: x[1], reverse=True))

        for completepath, last_modified_loop in list_of_files.items():
            path_to_discover = completepath.split(self.ruta)[-1]
            split_path = path_to_discover.split("/")
            file_name = split_path[-1]

            if self.last_modified_last_level:
                partitions = tuple(split_path[0 : len(split_path) - 2])
                last_level = split_path[-2]

                if partitions in partition_files_as_dict:
                    last_modified_current = list_of_files[completepath]
                    # si la fecha es mayor, la particion es mas reciente, entonces se reemplaza.
                    if last_modified_loop > last_modified_current:
                        partition_files_as_dict[partitions] = f"{last_level}/{file_name}"

                else:
                    partition_files_as_dict[partitions] = f"{last_level}/{file_name}"

            else:

                partitions = split_path[0 : len(split_path) - 1]
                parts = {part.split("=")[0]: part.split("=")[1] for part in partitions if "=" in part}
                partition_files_.append((file_name, parts))

        if self.last_modified_last_level:
            # Ajustar el formato.
            partition_files_ = [
                (name, {part.split("=")[0]: part.split("=")[1] for part in parts if "=" in part})
                for parts, name in partition_files_as_dict.items()
            ]

        self.partition_files = partition_files_
