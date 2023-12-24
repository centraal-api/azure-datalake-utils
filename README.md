# Azure Datalake Utils


[![pypi](https://img.shields.io/pypi/v/azure-datalake-utils.svg)](https://pypi.org/project/azure-datalake-utils/)
[![python](https://img.shields.io/pypi/pyversions/azure-datalake-utils.svg)](https://pypi.org/project/azure-datalake-utils/)
[![Build Status](https://github.com/centraal-api/azure-datalake-utils/actions/workflows/dev.yml/badge.svg)](https://github.com/centraal-api/azure-datalake-utils/actions/workflows/dev.yml)
[![codecov](https://codecov.io/gh/centraal-api/azure-datalake-utils/branch/main/graphs/badge.svg)](https://codecov.io/github/centraal-api/azure-datalake-utils)



Utilidades para interactuar con Azure Datalake.

El objetivo es evitar que personas denominadas cientificos ciudadanos tengan que interactuar con librerias, que no son, totalmente relacionadas con el analisis de datos.

La hipotesis detras de este pensamiento es que se puede lograr incrementar la adopción de estas herramientas si se facilitan y simplifica la interacción de pandas con la lectura del datalake.

* Documentation: <https://centraal-api.github.io/azure-datalake-utils>
* GitHub: <https://github.com/centraal-api/azure-datalake-utils>
* PyPI: <https://pypi.org/project/azure-datalake-utils/>
* Free software: Apache-2.0


## Features

* Control de autenticación directamente con el Directorio activo de Azure.
* Lectura de archivos csv, excel, json y parquet de una forma más concisa.
* Creación de token SAS para generar URL a un path especifico.


## Publicar nueva version

Seguir [checklist del template orginal](https://waynerv.github.io/cookiecutter-pypackage/pypi_release_checklist/).


## Credits

La librería es creada y mantenida por [Centraal Studio](https://centraal.studio/).
 
Centraal Studio Agredece la alianza con [Haceb](https://www.haceb.com/), cuyos retos internos  de democratizar el acceso a información han motivado la creación de esta librería.

//

This package is created and mantained by [Centraal Studio](https://centraal.studio/).

Centraal Studio appreciate the alliance with [Haceb](https://www.haceb.com/), which internal efforts to democratize the access of company information has motivated the creation of the library.

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [waynerv/cookiecutter-pypackage](https://github.com/waynerv/cookiecutter-pypackage) project template.
