"""Suite de test para clase principal."""
from unittest.mock import patch

import pytest
from azure.identity import AuthenticationRecord

from azure_datalake_utils import Datalake
from azure_datalake_utils.exepctions import ExtensionIncorrecta

fake_record = AuthenticationRecord("tenant-id", "client-id", "localhost", "object.tenant", "username")


def test_Datalake():
    """Test de inicializacion de Datalake."""
    with patch('azure.identity.InteractiveBrowserCredential.authenticate', return_value=fake_record):
        dl = Datalake('name', 'tenant')
        assert dl.storage_options == {'account_name': 'name', 'anon': False}


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
