# Test-Data-Engineer

## Método de Ejecución

Para la ejecución del scritp en un entorno local, se utilizaron las siguientes sentencias.

**python "Test Data Engineer.py" --add_field 20210708 --path_config config_a.json**
**python "Test Data Engineer.py" --path_config config_b.json**

### Parámetros
* --add_field: Valores asociados a los campos adicionales. Dichos campos deben indicarse en el archivo de configuración
* --path_config: Ruta del archivo de configuración

## Pregunta: 
**Qué peculiaridades tiene el modo de escritura Overwrite, en data frames particionados, para versiones de Spark previas a 2.3? Qué cambió a partir de dicha versión?**

## Respuesta:
En versiones anteriores a la 2.3, al momento de escribir un archivo particionado por un determinado campo utilizando el modo "Overwrite" en una ruta 
en particular, Spark reemplazaba todas las particiones existentes en dicha ruta, por la nueva partición. Sin embargo, en versiones posteriores a la 2.3,
es posible modificar el comportamiento del modo "Overwrite" alterando solo las particiones del mismo valor o agregando nuevas en caso de que el valor
no exista.