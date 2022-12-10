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
import datetime

import pytest

from unittest.mock import patch
from typing import List, Dict
from azure_datalake_utils.partitions import HivePartitiion


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


def list_side_effect_to_test_last_level(*args, **kwargs) -> Dict:
    """Funcion para controlar los side effects de fs.

    Simula una estructura de archivos de la siguiente manera:
    - contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv
    - contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv
    - contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv

    """
    if args[0] == "contenedor/ruta/al/archivo/year=2022/month=10/":
        return {
            'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv': {
                'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
            },
            'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv': {
                'last_modified': datetime.datetime(2022, 1, 2, 12, 30, 00, tzinfo=datetime.timezone.utc)
            },
        }
    elif args[0] == "contenedor/ruta/al/archivo/year=2022/month=11/":
        return {
            'contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv': {
                'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
            },
        }
    else:
        return {}


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_no_filter_deeper_level_return_correct_list(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': ['2022', '2023'], 'month': ['10']}, fs=fs_mock
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': '2022', 'month': '10'}),
        ("archivo.csv", {'year': '2023', 'month': '10'}),
    ]

    assert hive.container == "contenedor"
    assert hive.path_blob == "ruta/al/archivo/"


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_and_partition_exclusion_should_return_filtered_partition(fs_mock):
    """Test para verificar discover con exlcusion de particiones."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/",
        partition_cols={'year': ['2022'], 'month': ['10', '11', '1']},
        fs=fs_mock,
        partition_exclusion={'month': ['11', '1']},
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': '2022', 'month': '10'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_and_partition_exclusion_should_rasie_expection(fs_mock):
    """Test para verificar discover con exlcusion de particiones."""
    fs_mock.listdir.side_effect = list_side_effect_to_test

    with pytest.raises(KeyError):

        HivePartitiion(
            ruta="contenedor/ruta/al/archivo/",
            partition_cols={'year': ['2022'], 'month': ['10', '11', '1']},
            fs=fs_mock,
            partition_exclusion={'month': ['11', '1'], 'product': ['foo']},
        )


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_no_last_modified_last_level_return_valid_files(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': ['2022'], 'month': ['10', '11', '1']}, fs=fs_mock
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': '2022', 'month': '10'}),
        ("archivo.csv", {'year': '2022', 'month': '11'}),
        ("archivo1.csv", {'year': '2022', 'month': '1'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_no_last_modified_last_level_return_existing_files(fs_mock):
    """Test para verificar discover."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': ['2022', '2024'], 'month': ['10', '11']}, fs=fs_mock
    )
    assert hive.partition_files == [
        ("archivo.csv", {'year': '2022', 'month': '10'}),
        ("archivo.csv", {'year': '2022', 'month': '11'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test__make_partitions_using_partition_cols_filter_last_modified_last_level_return_valid_files(fs_mock):
    """Test para verificar discover."""
    fs_mock.find.side_effect = list_side_effect_to_test_last_level

    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/",
        partition_cols={'year': ['2022'], 'month': ['10', '11']},
        fs=fs_mock,
        last_modified_last_level=True,
    )

    assert hive.partition_files == [
        ("load_date=2022-01-02/archivo.csv", {'year': '2022', 'month': '10'}),
        ("load_date=2022-01-01/archivo.csv", {'year': '2022', 'month': '11'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test___discover_should_discover_partitions(fs_mock):
    """Test para verificar discover."""
    fs_mock.find.return_value = {
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 2, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
    }

    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/",
        fs=fs_mock,
        last_modified_last_level=False,
    )

    assert hive.partition_files == [
        ("archivo.csv", {'year': '2022', 'month': '10', 'load_date': '2022-01-02'}),
        ("archivo.csv", {'year': '2022', 'month': '10', 'load_date': '2022-01-01'}),
        ("archivo.csv", {'year': '2022', 'month': '11', 'load_date': '2022-01-01'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test___discover_with_filter_last_modified_last_level_should_discover_partitions(fs_mock):
    """Test para verificar discover."""
    fs_mock.find.return_value = {
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 2, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
    }

    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/",
        fs=fs_mock,
        last_modified_last_level=True,
    )

    assert hive.partition_files == [
        ("load_date=2022-01-02/archivo.csv", {'year': '2022', 'month': '10'}),
        ("load_date=2022-01-01/archivo.csv", {'year': '2022', 'month': '11'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test___discover_with_filter_last_modified_last_level_and_partition_exclusion_should_discover_partitions(fs_mock):
    """Test para verificar discover."""
    fs_mock.find.return_value = {
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 2, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
    }

    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/",
        fs=fs_mock,
        last_modified_last_level=True,
        partition_exclusion={'month': ['10']},
    )

    assert hive.partition_files == [
        ("load_date=2022-01-01/archivo.csv", {'year': '2022', 'month': '11'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test___discover_with_filter_last_modified_last_level_and_partition_inclusion_should_discover_partitions(fs_mock):
    """Test para verificar discover."""
    fs_mock.find.return_value = {
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=10/load_date=2022-01-02/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 2, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
        'contenedor/ruta/al/archivo/year=2022/month=11/load_date=2022-01-01/archivo.csv': {
            'last_modified': datetime.datetime(2022, 1, 1, 12, 30, 00, tzinfo=datetime.timezone.utc)
        },
    }

    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/",
        fs=fs_mock,
        last_modified_last_level=True,
        partition_inclusion={'month': ['10']},
    )

    assert hive.partition_files == [
        ("load_date=2022-01-02/archivo.csv", {'year': '2022', 'month': '10'}),
    ]


@patch("azure_datalake_utils.partitions.AzureBlobFileSystem", autospec=True)
def test_get_partitiion_list_should_return_list_files(fs_mock):
    """Test para verificar get_partition_list."""
    fs_mock.listdir.side_effect = list_side_effect_to_test
    hive = HivePartitiion(
        ruta="contenedor/ruta/al/archivo/", partition_cols={'year': ['2022', '2024'], 'month': ['10', '11']}, fs=fs_mock
    )

    partition_list = hive.get_partition_list()

    assert partition_list == [
        'contenedor/ruta/al/archivo/year=2022/month=10/archivo.csv',
        'contenedor/ruta/al/archivo/year=2022/month=11/archivo.csv',
    ]
