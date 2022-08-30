"""Main module."""

class Datalake(object):

    def __init__(self, datalake_name: str) -> None:
        """Clase para interactuar con Azure Dalake.

        Args:
            datalake_name: nombre de la cuenta de Azure Datalake Gen2.

        """
        self.datalake_name = datalake_name
        