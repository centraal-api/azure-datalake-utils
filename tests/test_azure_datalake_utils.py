"""Suite de test para clase principal."""
import platform
from unittest.mock import patch

import pytest
from azure.identity import AuthenticationRecord
from azure.identity.aio import DefaultAzureCredential

from azure_datalake_utils import Datalake
from azure_datalake_utils.exepctions import ExtensionIncorrecta

fake_record = AuthenticationRecord("tenant-id", "client-id", "localhost", "object.tenant", "username")


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
