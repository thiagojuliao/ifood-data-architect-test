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

# Referência de processamento
ref = str(datetime.today() - timedelta(days=1))[0:10]

# Caminho de origem da raw order status
origem_raw = "s3://ifood-raw-order-status/dt={}/*.parquet".format(ref)

# Caminho de destino da trusted order status
destino_trusted = "s3://ifood-trusted-order-status/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-trusted-order-status-dev").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima de bytes em uma partição ao ler os arquivos de entrada (Entre 100MB e 200MB é o ideal)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições

################################
#   Etapas do Processamento    #
################################

# Lê a base raw order status
rawDF = spark.read.parquet(origem_raw)

# Pivoteia a tabela a fim de termos os valores dos status de cada ordem em colunas distintas
pivotDF = rawDF \
    .drop("dt_proc") \
    .withColumn("created_at", col("created_at").cast(TimestampType())) \
    .withColumn("created_at", unix_timestamp("created_at")) \
    .groupBy("order_id") \
    .pivot("value", ["REGISTERED", "PLACED", "CONCLUDED", "CANCELLED"]) \
    .min("created_at") \
    .withColumn("registered", to_timestamp("REGISTERED")) \
    .withColumn("placed", to_timestamp("PLACED")) \
    .withColumn("concluded", to_timestamp("CONCLUDED")) \
    .withColumn("cancelled", to_timestamp("CANCELLED")) \
    .withColumn("dt_proc", current_date()) \
    .withColumn("dt", current_date())

# Grava a tabela no diretório final
# 125MB correspondem a aproximadamente 2.400.000 linhas
num_de_linhas = pivotDF.count()
total_de_particoes = num_de_linhas / 2400000

if total_de_particoes == 0:
    total_de_particoes = 1
    
pivotDF \
    .repartition(total_de_particoes) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .option("compression", "snappy") \
    .format("parquet") \
    .save(destino_trusted)

# Validação
# Leitura da base final
trustedDF = spark.read.parquet(destino_trusted) \
    .select(
        "order_id", 
        expr("registered as registered_"), 
        expr("placed as placed_"), 
        expr("concluded as concluded_"), 
        expr("cancelled as cancelled_")) \
    .orderBy("order_id")

# Monta a tabela comparativa
compDF = rawDF \
    .groupBy("order_id", "value") \
    .agg(min("created_at").alias("created_at")) \
    .withColumn("registered", expr("case when value == 'REGISTERED' then created_at else null end")) \
    .withColumn("placed", expr("case when value == 'PLACED' then created_at else null end")) \
    .withColumn("concluded", expr("case when value == 'CONCLUDED' then created_at else null end")) \
    .withColumn("cancelled", expr("case when value == 'CANCELLED' then created_at else null end")) \
    .withColumn("registered", col("registered").cast(TimestampType())) \
    .withColumn("placed", col("placed").cast(TimestampType())) \
    .withColumn("concluded", col("concluded").cast(TimestampType())) \
    .withColumn("cancelled", col("cancelled").cast(TimestampType())) \
    .drop("value") \
    .orderBy("order_id") 

# Compara as variáveis e valida os resultados
validaDF = compDF \
    .join(trustedDF, on=["order_id"], how="left") \
    .withColumn("fl_registered", expr("case when coalesce(registered, registered_, '9999-12-31 00:00:00') = coalesce(registered_, '9999-12-31 00:00:00') then 0 else 1 end")) \
    .withColumn("fl_placed", expr("case when coalesce(placed, placed_, '9999-12-31 00:00:00') = coalesce(placed_, '9999-12-31 00:00:00') then 0 else 1 end")) \
    .withColumn("fl_concluded", expr("case when coalesce(concluded, concluded_, '9999-12-31 00:00:00') = coalesce(concluded_, '9999-12-31 00:00:00') then 0 else 1 end")) \
    .withColumn("fl_cancelled", expr("case when coalesce(cancelled, cancelled_, '9999-12-31 00:00:00')  = coalesce(cancelled_, '9999-12-31 00:00:00') then 0 else 1 end"))

# Faz a contagem de valores desiguais
errosDF = validaDF \
    .withColumn("dummy", lit("1")) \
    .groupBy("dummy") \
    .agg(
        sum("fl_registered").alias("err_registered"),
        sum("fl_placed").alias("err_placed"),
        sum("fl_concluded").alias("err_concluded"),
        sum("fl_cancelled").alias("err_cancelled")) \
    .withColumn("total", col("err_registered") + col("err_placed") + col("err_concluded") + col("err_cancelled"))

total_de_erros = errosDF.select("total").take(1)[0]["total"]

print("> Total de valores desiguais confirmados: {}".format(total_de_erros))