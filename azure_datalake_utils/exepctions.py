
class ArchivoNoEncontrado(Exception):
    """Exepction cuando un archivo no es encontrado en el Datalake."""


    def __init__(self, ruta, message="La ruta no es se encuentra, verificar que el archivo(s) existan."):
        self.ruta = ruta
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.ruta} -> {self.message}'
    
