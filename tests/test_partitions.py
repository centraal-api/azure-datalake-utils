"""Tests for `azure_datalake_utils.partitions` package."""
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
from unittest.mock import patch, create_autospec
from typing import List, Dict
from azure_datalake_utils.partitions import HivePartitiion
from azure.storage import blob


def list_side_effect_to_test(*args) -> List[Dict]:
    """Funcion para controlar los side effects de fs.

    Simula una estructura de archivos de la siguiente manera:
    - contenedor/ruta/al/archivo/year=2022/month=10/
        archivo.csv
    - contenedor/ruta/al/archivo/year=2023/month=10/
        archivo.csv
    - contenedor/ruta/al/archivo/year=2022/month=11/
        archivo.csv
    - contenedor/ruta/al/archivo/year=2022/month=1/
        archivo1.csv
        archivo2.csv

    """
    if args[0] == "contenedor/ruta/al/archivo/year=2022/month=10/":
        return [{"name": "contenedor/ruta/al/archivo/year=2022/month=10/archivo.csv"}]
    elif args[0] == "contenedor/ruta/al/archivo/year=2023/month=10/":
        return [{"name": "contenedor/ruta/al/archivo/year=2023/month=10/archivo.csv"}]
    elif args[0] == "contenedor/ruta/al/archivo/year=2022/month=11/":
        return [{"name": "contenedor/ruta/al/archivo/year=2022/month=11/archivo.csv"}]
    elif args[0] == "contenedor/ruta/al/archivo/year=2022/month=11/":
        return []
    elif args[0] == "contenedor/ruta/al/archivo/year=2022/month=1/":
        return [
            {"name": "contenedor/ruta/al/archivo/year=2022/month=1/archivo1.csv"},
            {"name": "contenedor/ruta/al/archivo/year=2022/month=1/archivo2.csv"},
        ]
    else:
        raise FileNotFoundError


def list_side_effect_to_test_deeper_level(*args) -> List[Dict]:
    """Funcion para controlar los side effects de fs.

    Simula una estructura de archivos de la siguiente manera:
    - contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv
    - contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv
    - contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv

    """
    if args[0] == "contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv":
        return [{"name": "contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv"}]
    elif args[0] == "contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv":
        return [{"name": "contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv"}]
    elif args[0] == "contenedor/ruta/al/archivo/year=2022/month=11/":
        return [{"name": "contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv"}]
    else:
        raise FileNotFoundError


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_no_filter_deeper_level_return_correct_list(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': [2022, 2023], 'month': [10]}, fs=fs_mock
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': 2022, 'month': 10}),
        ("archivo.csv", {'year': 2023, 'month': 10}),
    ]

    assert hive.container == "contenedor"
    assert hive.path_blob == "ruta/al/archivo/"


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_no_filter_deeper_level_return_valid_files(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': [2022], 'month': [10, 11, 1]}, fs=fs_mock
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': 2022, 'month': 10}),
        ("archivo.csv", {'year': 2022, 'month': 11}),
        ("archivo1.csv", {'year': 2022, 'month': 1}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_no_filter_deeper_level_return_existing_files(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': [2022, 2024], 'month': [10, 11]}, fs=fs_mock
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': 2022, 'month': 10}),
        ("archivo.csv", {'year': 2022, 'month': 11}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__get_last_modified_should_return_last_modified(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test_deeper_level

    mock_blob = create_autospec(blob)
    mock_client = mock_blob.BlobServiceClient("https://fake", "key")
    mock_container = mock_blob.ContainerClient("https://fake", "key")
    mock_container.list_blobs.return_value = 
    mock_client.get_container_client.return_value = mock_container

    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': [2022, 2024], 'month': [10, 11]}, fs=fs_mock
    )

    hive._get_last_modified('', mock_client)
