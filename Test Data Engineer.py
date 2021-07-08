# coding: utf-8

#import findspark # Sólo para ejecutar en ambiente local
#findspark.init() # Sólo para ejecutar en ambiente local 

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DateType, IntegerType, StringType, TimestampType
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import json
import os
import numpy as np
import sys
import argparse

parser = argparse.ArgumentParser(description="Test Data Engineer")
parser.add_argument('--add_fields',metavar="Valores asignados a los campos adicionales",nargs='+',help="Valores asignados a los campos adicionales",default=None,required=False)
parser.add_argument('--path_config',metavar="Archivo de configuración .json",nargs='?',help="Archivo de configuración",default=os.path.abspath("config_a.json"),required=False)

args = parser.parse_args()

spark = SparkSession\
        .builder\
        .appName(name="Test Data Engineer")\
        .getOrCreate()
# Configuración necesaria para que el modo overwrite solo sobrescriba los valores de las particiones indicadas
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Método utilizado para validar el archivo de configuración en relación al archivo de origen
def validate_origin(origin,**kwargs):
	# Input
	# origin: Diccionario representativo del archivo de origen.
	# delimiters: Array de caracteres delimitadores válidos. ej. delimiters=[',',';','']
	# formats: Array de formatos de archivos válidos. ej. formats=['csv','text']
	# exts: Array de extensiones de archivos válidas. ej exts=['csv','txt']
	# 
	# Output
	# path: string de ruta del archivo origen
	# has_header: boolean de header
	# file_format: formato de archivo
	# has_delimiter: boolean delimitador
	# sep: string de separador
	# columns: Diccionario de columnas
	# add_field: Diccionario de columnas adicionales
    try:
        # Se evalúa que el archivo presente los atributos necesarios para el proceso ETL
        ext = origin.get('ext')
        file_format = origin.get('format')
        delimiter = origin.get('delimiter')
        basepath = origin.get('basepath')
        filename = origin.get('filename')
        has_header = origin.get('has_header',True)
        has_delimiter = origin.get('has_delimiter',True)
        has_add_column = origin.get('has_add_column',False)
        columns = origin.get('columns')
        add_column = origin.get('add_column')

    except KeyError as e:
        raise Exception("ERROR: El atributo de configuracion {} es inválido".format(e))
        
    try:
        for key,value in kwargs.items():
            if key=='exts' and ext not in value:
                raise Exception("El atributo 'ext' no contiene un valor válido")
            if key=='formats' and file_format not in value:
                raise Exception("El atributo 'format' no contiene un valor válido")
            if key=='delimiters' and delimiter not in value:
                raise Exception("El atributo 'delimiter' no contiene un valor válido")
                
        path = ".".join((os.path.join(basepath,filename),ext))	
        sep = delimiter if has_delimiter else ''
        add_field = add_column if has_add_column else []
        
        if not os.path.exists(os.path.abspath(path)):
            raise Exception("ERROR: No se pudo encontrar el archivo de entrada {}".format(path))
            
        if not args.add_fields and len(add_field) > 0:
            raise Exception("ERROR: Debe ingresar {} valor/es para la/s columna/s adicional/es".format(len(add_field)))
        
        if args.add_fields and len(add_field) != len(args.add_fields):
            raise Exception("ERROR: Campos adicionales {} - Parámetros ingresados {}".format(len(add_field),len(args.add_fields)))
            
    except Exception as e:
        sys.exit(e)
    return path, has_header, file_format, has_delimiter, sep, columns, has_add_column, add_field

# Método utilizado para validar el archivo de configuración en relación al archivo de salida
def validate_target(target):
	# Input
	# target: Diccionario representativo del archivo de salida.
	#
	# Output
	# path: string de ruta del archivo de salida
	# partitions: Array con los campos utilizandos para particionar el archivo
	# mode: string Modo de escritura
	# repartition: Cantidad de archivos de particion
	# compression: tipo de compresion del archivo

    try:
        # Se evalúa que el archivo presente los atributos necesarios para el proceso ETL
        basepath = target.get('basepath')
        filename = target.get('filename')
        partitions = target.get('partitions')
        mode = target.get('mode')
        repartition = target.get('repartition')
        compression = target.get('compression')
        path = os.path.join(basepath,filename)  
                    
    except KeyError as e:
        sys.exit("ERROR: El atributo de configuracion {} es inválido".format(e))
    
    try:
        if compression not in ['brotli', 'uncompressed', 'lz4', 'gzip', 'lzo', 'snappy', 'none', 'zstd']:
            raise Exception("ERROR: {} no es un valor válido para el atributo 'compression'".format(compression))
            
        if mode not in ['append', 'overwrite', 'ignore', 'error', 'errorifexists']:
            raise Exception("ERROR: {} no es un valor válido para el atributo 'mode'".format(mode))
            
        if not os.path.exists(os.path.abspath(basepath)):
            raise Exception("ERROR: No se pudo encontrar la ruta del archivo de salida {}".format(basepath))
        
    except Exception as e:
        sys.exit(e)
    
    return path, partitions, mode, repartition, compression

# Método encargado de formatear las columnas según el tipo de dato declarado en el archivo de configuración
def formatter_field(name,_type,fmt=None):
	# Input
	# name: string con el nombre de la columna a formatear
	# _type: string con el tipo de la columna a formatear
    # fmt: string con el formato de la columna.
	#
	# Output
	# function: Función de transformación
    if _type == 'date':
        return F.to_date(F.col(name),fmt)
    if _type == 'datetime':
        return to_datetime(F.col(name),fmt)
    if _type == 'timestamp':
        return F.to_timestamp(F.col(name),fmt)
    
    return F.col(name).cast(_type)

# Método encargado de agregar columna adicional al dataframe
def get_new_field(value,_type,fmt=None):
	# Input
	# value: string con el valor de la columna adiciona
	# _type: string con el tipo de la columna adicional
    # fmt: string con el formato de la columna.
	#
	# Output
	# function: Función de transformación
    if _type == 'date':
        return F.to_date(F.lit(value),fmt)
    if _type == 'datetime':
        return to_datetime(F.lit(value),fmt)
    if _type == 'timestamp':
        return F.to_timestamp(F.lit(value),fmt)
    
    return F.lit(value).cast(_type)
        
# Método encargado de parsear archivos dependiendo de su estructura
def parser_columns(row, delimited=True, sep=','):
    # Input: 
    # row: string representativo de cada fila de un archivo
    # delimited: boolean delimitador
    # sep: string de separación
    #
    # Output:
    # Array con el string parseado
    
    if delimited:
        return row.split(sep)
    else:
        try:
            width_max = np.cumsum([int(w.get('width')) for w in _columns])
            width_min = [int(w.get('width')) for w in _columns]
            dist = np.abs(width_min - width_max)
            return [row[a:b].strip() for a,b in zip(dist,width_max)]
        
        except KeyError as e:
            sys.exit("ERROR: El atributo de configuracion {} es inválido".format(e))
            
        except Exception as e:
            sys.exit(e)

# ----------------------------------------------------------INICIO ETL---------------------------------------------------------#
# Lectura segura del archivo de configuración
try:
    with open(os.path.abspath(args.path_config)) as file:
        config = json.load(file)
except Exception as e:
    sys.exit(e)

_origin = config.get('origin')
_target = config.get('target')

# Validación de la configuracion asociada al archivo de origen
_path_in, _has_header, _format, _has_delimiter,_sep, _columns, _has_add_column, _add_field = validate_origin(origin=_origin)
_path_out, _partitions,_mode, _repartition, _compression = validate_target(target=_target)

# Procedemos a la lectura del archivo origen
try:
    header = None
    if _has_header:
        header = spark.sparkContext.textFile(_path_in)\
                .first()
        
    dataset = spark.sparkContext.textFile(_path_in)\
                .filter(lambda x: x != header )\
                .map(lambda x: parser_columns(x, _has_delimiter,_sep))\
                .toDF([col.get('name') for col in _columns])   

except KeyError as e:
    sys.exit("ERROR: El atributo de configuración {} es inválido".format(e))
except Exception as e:
    sys.exit(e)

# Formateamos el archivo de origen.
for column in _columns:
    dataset = dataset.withColumn(column.get('name'),formatter_field(column.get('name'),column.get('type'),column.get('format_in')))

# Agregamos el campo adicional, si es que presenta.
if _has_add_column:
    for column, val in zip(_add_field,args.add_fields):
        dataset = dataset.withColumn(column.get('name'),get_new_field(val,column.get('type'),column.get('format_in')))

# Eliminamos los registros cuyos valores en el atributo de partición se encuentran en nulo
try:
    dataset = dataset.dropna(subset=_partitions)
except Exception as e:
    sys.exit("ERROR: Los atributos de particionado no se encuentran como campos del archivo de entrada")
    
# Muestra de archivo final, previo a la ingesta.
dataset.show()

# Ingesta de archivo Parquet en ruta destino

dataset.write\
    .partitionBy(_partitions)\
    .mode(_mode)\
    .option('compression',_compression)\
    .parquet(_path_out)
