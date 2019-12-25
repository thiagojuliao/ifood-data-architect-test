# coding: UTF-8

###############################################
#   Importando pacotes & definindo funções    #
###############################################

from datetime import *
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

###############################
#   Definição de variáveis    #
###############################

# Caminho de origem da raw order
origem_raw = "hdfs://localhost:9000/ifood-raw-order/"

# Caminho de destino da trusted items
destino_trusted = "hdfs://localhost:9000/ifood-trusted-items/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-trusted-items-dev").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima do tamanho das partições ao ler os arquivos de entrada
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições
spark.conf.set("spark.default.parallelism", 100) # Define a quantidade de tasks a serem executadas em paralelo

################################
#   Etapas do Processamento    #
################################

# Lê a raw order
#rawDF = spark.read.parquet(origem_raw).filter(col("dt") == "2019-01-01").select("order_id", "items", "dt")
rawDF = spark.read.parquet(origem_raw).select("order_id", "items", "dt")

# Explodindo a coluna items
expDF = rawDF \
    .withColumn("items", explode_outer("items"))
    
# Definimos um map para cada elemento do array afim de transformá-los em colunas para facilitar a leitura
# Cria o map de colunas para o array items
map_colunas = \
[
    "order_id",
    "items",
    expr("items.name as item_name"),
    expr("items.addition.currency as item_currency"),
    expr("items.addition.value/100 as item_addition_value"),
    expr("items.discount.value/100 as item_discount_value"),
    expr("items.quantity as item_quantity"),
    expr("items.sequence as item_sequence"),
    expr("items.unitPrice.value/100 as item_unit_price"),
    expr("items.externalId as item_external_id"),
    expr("items.totalValue.value/100 as item_total_value"),
    expr("items.customerNote as item_customer_note"),
    expr("items.garnishItems as item_garnish_list"),
    expr("items.integrationId as item_integration_id"),
    expr("items.totalAddition.value/100 as item_total_addition_value"),
    expr("items.totalDiscount.value/100 as total_discount_value"),
    "dt"
]

# Aplica o map no dataframe
mapDF = expDF.select(map_colunas)

# Explodindo a coluna item_garnish_list e definindo um novo map para suas colunas
# Explode o array item_garnish_list
expDF_ = mapDF \
    .withColumn("item_garnish_list", explode_outer("item_garnish_list"))
    
# Cria o novo map
map_colunas_ = \
[
    "order_id",
    "item_name",
    "item_currency",
    "item_addition_value",
    "item_discount_value",
    "item_quantity",
    "item_sequence",
    "item_unit_price",
    "item_external_id",
    "item_total_value",
    "item_customer_note",
    expr("item_garnish_list.name as garnish_name"),
    expr("item_garnish_list.addition.value/100 as garnish_addition_value"),
    expr("item_garnish_list.discount.value/100 as garnish_discount_value"),
    expr("item_garnish_list.quantity as garnish_quantity"),
    expr("item_garnish_list.sequence as garnish_sequence"),
    expr("item_garnish_list.unitPrice.value/100 as garnish_unit_price"),
    expr("item_garnish_list.categoryId as garnish_category_id"),
    expr("item_garnish_list.externalId as garnish_external_id"),
    expr("item_garnish_list.totalValue.value/100 as garnish_total_value"),
    expr("item_garnish_list.categoryName as garnish_category_name"),
    expr("item_garnish_list.integrationId as garnish_integration_id"),
    "item_integration_id",
    "item_total_addition_value",
    "total_discount_value",
    expr("current_date as dt_proc"),
    "dt"
]

# Aplica o novo map no dataframe
mapDF_ = expDF_.select(map_colunas_)

# Grava a base trusted items
mapDF_ \
    .repartition(int(spark.conf.get("spark.default.parallelism")), "dt") \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .option("compression", "snappy") \
    .format("parquet") \
    .save(destino_trusted)