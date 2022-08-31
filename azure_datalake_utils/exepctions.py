"""Exepciones personalizadas."""


class ArchivoNoEncontrado(Exception):
    """Exepction cuando un archivo no es encontrado en el Datalake."""

    def __init__(self, ruta, message="La ruta no es se encuentra, verificar que el archivo(s) existan."):
        """Constructor de la expecion."""
        self.ruta = ruta
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        """Imprimir el mensaje de la exepecion."""
        return f'{self.ruta} -> {self.message}'


class ExtensionIncorrecta(Exception):
    """Exepction cuando la ruta tiene extension incorrecta."""

    def __init__(self, ruta, message="La ruta no termina en la extension correcta."):
        """Constructor de la expecion."""
        self.ruta = ruta
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        """Imprimir el mensaje de la exepecion."""
        return f'{self.ruta} -> {self.message}'
