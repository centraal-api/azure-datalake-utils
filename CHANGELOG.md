# Changelog

## 0.5.5 - 2023-02-15

### Fixed

- Correción  de test unitarios.

## 0.5.4 - 2023-02-15

### Fixed

- Adicionar `storage_options["default_fill_cache"] = False` para asegurar no usar el cache, cuando la opción   `fsspec_cache=False` es usada.

## 0.5.3 - 2023-02-08

### Fixed

- version pip en poetry lock

## 0.5.1 - 2023-02-08

### Added

- se adiciona el parametro `fsspec_cache`. Se recomienda usar `fsspec_cache=False` cuando se encuentran en ambientes serverless. Para mas detalles:
    - https://github.com/fsspec/adlfs/issues/391
    - https://github.com/Azure/azure-sdk-for-python/issues/28312


## 0.5.0 - 2022-12-10

### Added
- `read_csv_with_partition` para inferir y leer archivos con particiones tipo `hive`. Solo se habilita cuando se usa el account key.
- `read_csv` ahora soporta listas de archivos como inputs.

## 0.4.0 - 2022-12-06

### Added

- `flag=True` en `dl.read_excel` Para dar un workaround para la siguiente situación:
    - Se observa un comportamiento extraño bajo estas condiciones:
    - Se importa la libreria en un sesion de python.
    - Se abre el archivo de excel y se reemplaza en el datalake.
    - Usando la misma sesión de python, se trata de leer el archivo con `dl.read_excel("hacebanalitica-user-servicio/configuracion_valores_salesforce.xlsx")`
    - Una exepción `BadZipFile: File is not a zip file` es disparada.
    - Este flag es experimental y es una solución rapida (esperamos que temporal) para entornos donde reiniciar python de manera periodica no es una opción (ejemplo en Azure Functions). Se espera explorar más y incorporar una mejor solución.


## 0.3.2 - 2022-09-10

### Fixed

- Bug en la lectura de excel.


## 0.3.1 - 2022-09-05

### Added

- Limpieza de columnas antes de escribir csv.


## 0.3.0 - 2022-09-05

### Added

- Soporte python 3.10
- Fix para credenciales en Windows
- Escritura y lectura de Json

## 0.2.0 - 2022-08-31

### Added

- Primera version util con lectura de excel y csv
- Adicionar ejemplos de como hacer testing


## 0.1.1 - 2022-08-30

### Added

- inlcuir primera utilidad

## 0.0.1 (2022-08-30)

* First release on PyPI.
