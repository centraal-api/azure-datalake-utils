"""Suite de test para clase principal."""
import platform
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from azure.identity import AuthenticationRecord
from azure.identity.aio import DefaultAzureCredential

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


def test_Datalake():
    """Test de inicializacion de Datalake."""
    with patch('azure.identity.InteractiveBrowserCredential.authenticate', return_value=fake_record):
        dl = Datalake('name', 'tenant')
        if platform.system().lower() != 'windows':
            assert dl.storage_options == {'account_name': 'name', 'anon': False}
        else:
            assert dl.storage_options['account_name'] == 'name'
            assert not dl.storage_options['anon']
            assert isinstance(dl.storage_options['credential'], DefaultAzureCredential)


def test_Datalake_from_account_key():
    """Test para inicializacion del datalake con key account."""
    dl = Datalake.from_account_key('name', 'key')
    assert dl.storage_options == {'account_name': 'name', 'account_key': 'key'}


def test_Datalake_verificar_extension():
    """Test de verificar de Datalake."""
    with patch('azure.identity.InteractiveBrowserCredential.authenticate', return_value=fake_record):
        dl = Datalake('name', 'tenant')
        assert dl._verificar_extension('a/b/c/foo.csv', '.csv', '.tsv')
        assert dl._verificar_extension('a/b/c/foo.tsv', '.csv', '.tsv', '.txt')
        assert not dl._verificar_extension('a/b/c/foo.xlsx', '.csv', '.tsv', '.txt')


def test_Datalake_read_csv_extension_incorrecta():
    """Test de verificar raise de ExtensionIncorrecta."""
    with patch('azure.identity.InteractiveBrowserCredential.authenticate', return_value=fake_record):
        dl = Datalake('name', 'tenant')
        with pytest.raises(ExtensionIncorrecta):
            dl.read_csv('contenedor/foo/bar.text')


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_csv")
def test_read_cv(read_mock: Mock, dl_account: Datalake, test_df: pd.DataFrame):
    """Test read_csv."""
    read_mock.return_value = test_df
    df = dl_account.read_csv("path/to/file.csv")
    read_mock.assert_called_once()
    pd.testing.assert_frame_equal(df, test_df)


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_json")
def test_read_json(read_mock: Mock, dl_account: Datalake, test_df: pd.DataFrame):
    """Test read_json."""
    read_mock.return_value = test_df
    df = dl_account.read_json("path/to/file.json")
    read_mock.assert_called_once()
    pd.testing.assert_frame_equal(df, test_df)


@patch("azure_datalake_utils.azure_datalake_utils.pd.read_excel")
def test_read_excel(read_mock: Mock, dl_account: Datalake, test_df: pd.DataFrame):
    """Test read_json."""
    read_mock.return_value = test_df
    df = dl_account.read_excel("path/to/file.xlsx")
    read_mock.assert_called_once()
    pd.testing.assert_frame_equal(df, test_df)


def test_limpiar_df_cols_str_no_special(dl_account: Datalake, test_str_df: pd.DataFrame):
    """Test para limpiar el DF."""
    # valores originales:
    # "foo_str": ['bar\n', 'foo,', 'bar|']
    # "bar_str": ["bar", "foo|", "bar\r"]
    # Test con `,`
    df_clean = dl_account._limpiar_df_cols_str(test_str_df, ',')
    assert df_clean['foo_str'].values.tolist() == ['bar ', 'foo ', 'bar|']
    assert df_clean['bar_str'].values.tolist() == ['bar', 'foo|', 'bar ']
    assert df_clean['foo'].values.tolist() == [1, 2, 3]


def test_limpiar_df_cols_str_special(dl_account: Datalake, test_str_df: pd.DataFrame):
    """Test para limpiar el DF."""
    # Test con `|`
    df_clean = dl_account._limpiar_df_cols_str(test_str_df, '|')
    assert df_clean['foo_str'].values.tolist() == ['bar ', 'foo,', 'bar ']
    assert df_clean['bar_str'].values.tolist() == ['bar', 'foo ', 'bar ']
    assert df_clean['foo'].values.tolist() == [1, 2, 3]
