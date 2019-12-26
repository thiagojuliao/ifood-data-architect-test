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

# Referência de processamento
ref00 = str(datetime.today() - timedelta(days=2))[0:10]
ref01 = str(datetime.today() - timedelta(days=1))[0:10]

# Caminho de origem da pouso order status
origem_pouso = "hdfs://localhost:9000/ifood-landing-order-status/dt={}".format(ref01)

# Caminho de origem da raw order status
origem_raw = "hdfs://localhost:9000/ifood-raw-order-status/dt={}".format(ref00)

# Caminho de destino da raw order status
destino_raw = "hdfs://localhost:9000/ifood-raw-order-status/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-raw-order-status-prod").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima de bytes em uma partição ao ler os arquivos de entrada (Entre 100MB e 200MB é o ideal)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições  

################################
#   Etapas do Processamento    #
################################

# Lê a pouso de origem
pousoDF = spark.read.schema(schema).json(origem_pouso)

# Lê a raw order
rawDF = spark.read.parquet(origem_raw).drop("dt", "dt_proc").persist(StorageLevel.DISK_ONLY)
print("> Volumetria da raw order de dois dias atrás: {}".format(rawDF.count()))

# Empilha as duas bases a fim de considerarmos atualizações de status de pedidos de até dois dias atrás e atualizarmos nossas partições
unionDF = pousoDF.union(rawDF)

# Adiciona a coluna dt que definirá nossa partição
unionDF_ = unionDF \
    .withColumn("dt", expr("min(created_at) over(partition by order_id order by order_id)")) \
    .withColumn("dt", col("dt").cast(DateType()))
   
# Cria a tabela raw
rawDF_ = unionDF_ \
    .withColumn("dt_proc", current_date()) \
    .dropDuplicates()
    
rawDF_.write.partitionBy("dt").mode("overwrite").option("compression", "snappy").format("parquet").save(destino_raw)

# Validação
# Leitura da base final
rawDF_ = spark.read.parquet(destino_raw).filter(col("dt") == lit(ref))

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