"""Suite de test para clase principal."""
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
import platform
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from azure.identity import AuthenticationRecord
from azure.identity.aio import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

from azure_datalake_utils import Datalake
from azure_datalake_utils.exepctions import ExtensionIncorrecta

fake_record = AuthenticationRecord("tenant-id", "client-id", "localhost", "object.tenant", "username")


@pytest.fixture
def dl_account() -> Datalake:
    """DL instancia incilizada."""
    return Datalake.from_account_key('name', 'key')


@pytest.fixture
def test_df() -> pd.DataFrame:
    """DF para test."""
    return pd.DataFrame({"foo_id": [1, 2, 3]})


@pytest.fixture
def test_str_df() -> pd.DataFrame:
    """DF para test."""
    return pd.DataFrame({"foo_str": ['bar\n', 'foo,', 'bar|'], "bar_str": ["bar", "foo|", "bar\r"], "foo": [1, 2, 3]})


def test_datalake_should_init_properly():
    """Test de inicializacion de Datalake."""
    with patch('azure.identity.InteractiveBrowserCredential.authenticate', return_value=fake_record):
        dl = Datalake('name', 'tenant')
        if platform.system().lower() != 'windows':
            assert dl.storage_options == {'account_name': 'name', 'anon': False}
        else:
            assert dl.storage_options['account_name'] == 'name'
            assert not dl.storage_options['anon']
            assert isinstance(dl.storage_options['credential'], DefaultAzureCredential)


def test_datalake_should_init_from_account_key():
    """Test para inicializacion del datalake con key account."""
    dl = Datalake.from_account_key('name', 'key')
    assert dl.storage_options == {'account_name': 'name', 'account_key': 'key'}


def test_verificar_extension_should_check_extensions():
    """Test de verificar de Datalake."""
    with patch('azure.identity.InteractiveBrowserCredential.authenticate', return_value=fake_record):
        dl = Datalake('name', 'tenant')
        assert dl._verificar_extension('a/b/c/foo.csv', '.csv', '.tsv')
        assert dl._verificar_extension('a/b/c/foo.tsv', '.csv', '.tsv', '.txt')
        assert not dl._verificar_extension('a/b/c/foo.xlsx', '.csv', '.tsv', '.txt')


def test_read_csv_should_raise_extension_incorrecta(dl_account: Datalake):
    """Test de verificar raise de ExtensionIncorrecta."""
    with pytest.raises(ExtensionIncorrecta):
        dl_account.read_csv('contenedor/foo/bar.text')


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_csv")
def test_read_csv_should_return_dataframe(read_mock: Mock, dl_account: Datalake, test_df: pd.DataFrame):
    """Test read_csv."""
    read_mock.return_value = test_df
    df = dl_account.read_csv("path/to/file.csv")
    read_mock.assert_called_once()
    pd.testing.assert_frame_equal(df, test_df)


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_json")
def test_read_json_should_return_dataframe(read_mock: Mock, dl_account: Datalake, test_df: pd.DataFrame):
    """Test read_json."""
    read_mock.return_value = test_df
    df = dl_account.read_json("path/to/file.json")
    read_mock.assert_called_once()
    pd.testing.assert_frame_equal(df, test_df)


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_excel")
def test_read_excel_should_return_dataframe(read_mock: Mock, dl_account: Datalake, test_df: pd.DataFrame):
    """Test read_json."""
    read_mock.return_value = test_df
    df = dl_account.read_excel("path/to/file.xlsx")
    read_mock.assert_called_once()
    pd.testing.assert_frame_equal(df, test_df)


def test_limpiar_df_cols_str_should_clean_no_special_chars(dl_account: Datalake, test_str_df: pd.DataFrame):
    """Test para limpiar el DF."""
    # valores originales:
    # "foo_str": ['bar\n', 'foo,', 'bar|']
    # "bar_str": ["bar", "foo|", "bar\r"]
    # Test con `,`
    df_clean = dl_account._limpiar_df_cols_str(test_str_df, ',')
    assert df_clean['foo_str'].values.tolist() == ['bar ', 'foo ', 'bar|']
    assert df_clean['bar_str'].values.tolist() == ['bar', 'foo|', 'bar ']
    assert df_clean['foo'].values.tolist() == [1, 2, 3]


def test_limpiar_df_cols_str_should_clean_special_chars(dl_account: Datalake, test_str_df: pd.DataFrame):
    """Test para limpiar el DF."""
    # Test con `|`
    df_clean = dl_account._limpiar_df_cols_str(test_str_df, '|')
    assert df_clean['foo_str'].values.tolist() == ['bar ', 'foo,', 'bar ']
    assert df_clean['bar_str'].values.tolist() == ['bar', 'foo ', 'bar ']
    assert df_clean['foo'].values.tolist() == [1, 2, 3]


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_csv", autospec=True)
def test_read_excel_should_raise_ArchivNoEncontrado(read_mock: Mock, dl_account: Datalake):
    """Test para verificar que la expecion es levantanda."""
    read_mock.side_effect = FileNotFoundError
    with pytest.raises(ExtensionIncorrecta):
        dl_account.read_csv('contenedor/foo/bar.text')


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_csv", autospec=True)
def test_read_excel_should_raise_ArchivNoEncontrado_with_azure_error(read_mock: Mock, dl_account: Datalake):
    """Test para verificar que la expecion es levantanda."""
    read_mock.side_effect = ResourceNotFoundError
    with pytest.raises(ExtensionIncorrecta):
        dl_account.read_csv('contenedor/foo/bar.text')
