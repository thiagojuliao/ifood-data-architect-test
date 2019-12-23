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

# Definindo o schema json
schema = StructType(
    [
        StructField("cpf", StringType()),
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("delivery_address_city", StringType()),
        StructField("delivery_address_country", StringType()),
        StructField("delivery_address_district", StringType()),
        StructField("delivery_address_external_id", StringType()),
        StructField("delivery_address_latitude", StringType()),
        StructField("delivery_address_longitude", StringType()),
        StructField("delivery_address_state", StringType()),
        StructField("delivery_address_zip_code", StringType()),
        StructField("items", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_latitude", StringType()),
        StructField("merchant_longitude", StringType()),
        StructField("merchant_timezone", StringType()),
        StructField("order_created_at", StringType()),
        StructField("order_id", StringType()),
        StructField("order_scheduled", BooleanType()),
        StructField("order_scheduled_date", StringType()),
        StructField("order_total_amount", DoubleType()),
        StructField("origin_platform", StringType())
    ])

# Referência de processamento
ref = str(datetime.today() - timedelta(days=1))[0:10]

# Caminho de origem da pouso order
origem_pouso = "s3://ifood-landing-order/dt={}/*.json".format(ref)

# Caminho de destino do incremental
destino_incremental = "s3://ifood-raw-order/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-raw-order-prod").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima de bytes em uma partição ao ler os arquivos de entrada (Entre 100MB e 200MB é o ideal)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições  

################################
#   Etapas do Processamento    #
################################

# Lê a pouso de origem
pousoDF = spark.read.schema(schema).json(origem_pouso)

# Cria a tabela raw
rawDF = pousoDF \
    .withColumn("dt_proc", current_date()) \
    .withColumn("dt", col("order_created_at").cast(DateType()))

rawDF.write.partitionBy("dt").mode("overwrite").option("compression", "snappy").format("parquet").save(destino_raw)

# Validação
# Leitura da base final
rawDF_ = spark.read.parquet(destino_raw).filter(col("dt") == lit(ref))

# Validação Volumétrica
print("> Volumetria de saída equivale-se a de entrada ? --> {}".format(pousoDF.count() == rawDF_.count()))

# Validação do Schema
schema = pousoDF.schema
schema_ = rawDF_.drop("dt", "dt_proc").schema
colunas = pousoDF.columns
colunas_ = rawDF_.drop("dt", "dt_proc").columns
colunas.sort()
colunas_.sort()
print("> O schema de saída equivale-se ao de entrada ? ---> {}".format(schema == schema_ or colunas == colunas_))