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

# Definição do Schema de garnishItems que é um Array dentro de Items
schema_value_currency = StructType([StructField("value", StringType()), StructField("currency", StringType())])
schema_garnish = StructType(
    [
        StructField("name", StringType()),
        StructField("addition", schema_value_currency),
        StructField("discount", schema_value_currency),
        StructField("quantity", DoubleType()),
        StructField("sequence", IntegerType()),
        StructField("unitPrice", schema_value_currency),
        StructField("categoryId", StringType()),
        StructField("externalId", StringType()),
        StructField("totalValue", schema_value_currency),
        StructField("categoryName", StringType()),
        StructField("integrationId", StringType())
    ])

# Definição do Schema do Array de Items
schema_items = StructType(
    [
        StructField("name", StringType()),
        StructField("addition", schema_value_currency),
        StructField("discount", schema_value_currency),
        StructField("quantity", DoubleType()),
        StructField("sequence", IntegerType()),
        StructField("unitPrice", schema_value_currency),
        StructField("externalId", StringType()),
        StructField("totalValue", schema_value_currency),
        StructField("customerNote", StringType()),
        StructField("garnishItems", ArrayType(schema_garnish)),
        StructField("integrationId", StringType()),
        StructField("totalAddition", schema_value_currency),
        StructField("totalDiscount", schema_value_currency)
    ])

# Definação do schema json
schema_json = StructType(
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

# Caminho de origem da pouso order
origem_pouso = "hdfs://localhost:9000/ifood-landing-order/full-load/order.json.gz"

# Caminho de destino da raw order
destino_raw = "hdfs://localhost:9000/ifood-raw-order/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-raw-order-dev").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima de bytes em uma partição ao ler os arquivos de entrada (Entre 100MB e 200MB é o ideal)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições

################################
#   Etapas do Processamento    #
################################

# Lê a pouso de origem
pousoDF = spark.read.schema(schema_json).json(origem_pouso)

# Altera o schema da coluna items para ser reconhecida como um array
pousoDF_ = pousoDF \
    .withColumn("items", from_json("items", ArrayType(schema_items)))
    
# Cria a tabela raw
rawDF = pousoDF_ \
    .withColumn("dt_proc", current_date()) \
    .withColumn("dt", col("order_created_at").cast(DateType())) \
    .repartition(100, "dt")

rawDF.write.partitionBy("dt").mode("overwrite").option("compression", "snappy").format("parquet").save(destino_raw)

# Validação
# Leitura da base final
rawDF_ = spark.read.parquet(destino_raw + "/*/*.parquet")

# Validação Volumétrica
print("> Volumetria de saída equivale-se a de entrada ? --> {}".format(pousoDF.count() == rawDF_.count()))

# Validação do Schema
schema = pousoDF.schema
schema_ = rawDF_.drop("dt_proc").schema
colunas = pousoDF.columns
colunas_ = rawDF_.drop("dt_proc").columns
colunas.sort()
colunas_.sort()
print("> O schema de saída equivale-se ao de entrada ? ---> {}".format(schema == schema_ or colunas == colunas_))