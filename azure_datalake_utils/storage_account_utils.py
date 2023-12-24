"""Utilidades relacionadas con la cuenta de almacenamiento."""
import datetime

import pandas as pd
from adlfs import AzureBlobFileSystem
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from typing_extensions import Literal


def create_url_sas_token(
    ruta: str,
    fs: AzureBlobFileSystem,
    expiration_duration: int,
    unit: Literal["day", "hour", "minute", 'second'] = "hour",
    ip: str = None,
) -> str:
    """Crea un url con sas token."""
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + pd.Timedelta(value=expiration_duration, unit=unit)
    contenedor = ruta.split("/")[0]
    path_file = "/".join(ruta.split("/")[1:])
    blob_client = fs.service_client.get_blob_client(contenedor, path_file)

    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=fs.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time,
        ip=ip,
    )

    return f"{blob_client.url}?{sas_token}"
