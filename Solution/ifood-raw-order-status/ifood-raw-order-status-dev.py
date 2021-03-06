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

# Definição do schema json
schema = StructType(
    [
        StructField("created_at", StringType()),
        StructField("order_id", StringType()),
        StructField("status_id", StringType()),
        StructField("value", StringType())
    ])
    
# Caminho de origem da pouso order status
origem_pouso = "hdfs://localhost:9000/ifood-landing-order-status/full-load/status.json.gz"

# Caminho de destino da raw order status
destino_raw = "hdfs://localhost:9000/ifood-raw-order-status/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-raw-order-status-dev").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima de bytes em uma partição ao ler os arquivos de entrada (Entre 100MB e 200MB é o ideal)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições 

################################
#   Etapas do Processamento    #
################################

# Lê a pouso de origem
pousoDF = spark.read.schema(schema).json(origem_pouso).persist(StorageLevel.DISK_ONLY)
print("> Volumetria da pouso orders: {}".format(pousoDF.count()))

# Adiciona a coluna dt que definirá nossa partição
pousoDF = pousoDF \
    .withColumn("dt", expr("min(created_at) over(partition by order_id order by order_id)")) \
    .withColumn("dt", col("dt").cast(DateType()))
    
# Cria a tabela raw
rawDF = pousoDF \
    .withColumn("dt_proc", current_date()) \
    .dropDuplicates()
    
rawDF.write.partitionBy("dt").mode("overwrite").option("compression", "snappy").format("parquet").save(destino_raw)

# Validação
# Leitura da base final
rawDF_ = spark.read.parquet(destino_raw + "/*/*.parquet")

# Validação Volumétrica
print("> Volumetria de saída equivale-se a de entrada ? --> {}".format(pousoDF.dropDuplicates().count() == rawDF_.count()))

# Validação do Schema
schema = pousoDF.schema
schema_ = rawDF_.drop("dt_proc").schema
colunas = pousoDF.columns
colunas_ = rawDF_.drop("dt_proc").columns
colunas.sort()
colunas_.sort()
print("> O schema de saída equivale-se ao de entrada ? ---> {}".format(schema == schema_ or colunas == colunas_))