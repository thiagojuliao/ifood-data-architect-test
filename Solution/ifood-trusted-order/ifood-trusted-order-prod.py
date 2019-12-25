# coding: UTF-8

###############################################
#   Importando pacotes & definindo funções    #
###############################################

from datetime import *
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Função para criptografar colunas de um dataframe
def criptografa_df(dataframe, colunas_a_criptografar):
    colunas = []
    
    for coluna in dataframe.columns:
        if coluna in colunas_a_criptografar:
            aux = sha2(col(coluna), 0).alias(coluna)
        else:
            aux = coluna
        colunas.append(aux)
    return colunas

###############################
#   Definição de variáveis    #
###############################

# Lista de colunas a serem criptografadas
cripto_ls = ["cpf", "customer_name", "delivery_address_city", "delivery_address_district", "delivery_address_country", "delivery_address_district", 
    "delivery_address_latitude", "delivery_address_longitude", "delivery_address_state", "delivery_address_zip_code", "customer_phone_area", 
    "customer_phone_number", "merchant_zip_code"]

# Ordem das colunas na base final
colunas = ["order_id", "customer_id", expr("cpf as customer_cpf"), "customer_name", "customer_phone_area", "customer_phone_number", expr("language as customer_language"), 
    "costumer_created_at", expr("active as is_costumer_active"), "merchant_id", "merchant_created_at", expr("enabled as is_merchant_enabled"), 
    expr("price_range as merchant_price_range"), expr("average_ticket as merchant_average_ticket"), expr("takeout_time as merchant_takeout_time"), 
    expr("delivery_time as merchant_delivery_time"), expr("minimum_order_value as merchant_minimum_order_value"), "merchant_zip_code", "merchant_city",
    "merchant_state", "merchant_country", "merchant_latitude", "merchant_longitude", "merchant_timezone", "delivery_address_city", 
    "delivery_address_country", "delivery_address_district", "delivery_address_external_id", "delivery_address_latitude", "delivery_address_longitude", 
    "delivery_address_state", "delivery_address_zip_code", expr("items as order_items"), "order_created_at", expr("order_scheduled as is_order_scheduled"),
    "order_scheduled_date", "order_total_amount", "origin_platform", "order_status"]

# Referências de processamento
ref = str(datetime.today() - timedelta(days=1))[0:10]

# Caminho de origem da raw order
origem_order = "hdfs://localhost:9000/ifood-raw-order/dt={}/*.parquet".format(ref)

# Caminho de origem da raw order status
origem_status = "hdfs://localhost:9000/ifood-raw-order-status/dt={}/*.parquet".format(ref)

# Caminho de origem da raw restaurant
origem_rest = "hdfs://localhost:9000/ifood-raw-restaurant/"

# Caminho de origem da raw consumer
origem_cons = "hdfs://localhost:9000/ifood-raw-consumer/"

# Caminho de destino da trusted order
destino_trusted = "hdfs://localhost:9000/ifood-trusted-order/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-trusted-order-prod").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima do tamanho das partições ao ler os arquivos de entrada
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024) # Define o limiar para broadcasting de até 100MB
spark.conf.set("spark.default.parallelism", 100) # Define a quantidade de tasks a serem executadas em paralelo

################################
#   Etapas do Processamento    #
################################

# Lê a raw order
#orderDF = spark.read.parquet(origem_order).drop("dt", "dt_proc")
orderDF = spark.read.parquet(origem_order).drop("dt", "dt_proc").persist(StorageLevel.DISK_ONLY)
print("> Volumetria da raw order: {}".format(orderDF.count()))

# Lê a raw order status
#statusDF = spark.read.parquet(origem_status).drop("dt", "dt_proc")
statusDF = spark.read.parquet(origem_status).drop("dt", "dt_proc")
print("> Volumetria da raw order status: {}".format(statusDF.count()))

# Lê a raw restaurant
#restDF = spark.read.parquet(origem_rest).drop("dt", "dt_proc")
restDF = spark.read.parquet(origem_rest).drop("dt", "dt_proc")
print("> Volumetria da raw restaurant: {}".format(restDF.count()))

# Lê a raw consumer
#consDF = spark.read.parquet(origem_cons).drop("dt", "dt_proc")
consDF = spark.read.parquet(origem_cons).drop("dt", "dt_proc")
print("> Volumetria da raw consumer: {}".format(consDF.count()))

# Status mais recentes de cada pedido'
statusDF_ = statusDF \
    .withColumn("order", expr("row_number() over(partition by order_id order by order_id, created_at desc)")) \
    .filter(col("order") == 1) \
    .select("order_id", "created_at", expr("value as order_status"))
    
# Cruzando com a base orders
# Otimização do join
spark.conf.set("spark.sql.shuffle.partitions", int(spark.conf.get("spark.default.parallelism")))
orderDF \
    .write.mode("overwrite") \
    .bucketBy(int(spark.conf.get("spark.default.parallelism")), "order_id") \
    .option("compression", "snappy") \
    .format("parquet") \
    .saveAsTable("orders")
    
orderDF_ = spark.read.table("orders")

statusDF_ \
    .write.mode("overwrite") \
    .bucketBy(int(spark.conf.get("spark.default.parallelism")), "order_id") \
    .option("compression", "snappy") \
    .format("parquet") \
    .saveAsTable("status")
    
statusDF_ = spark.read.table("status")
    
# Traz os status mais recentes de cada pedido
join00 = orderDF_.join(statusDF_, on=["order_id"], how="left")

# Tratamento de duplicidade na base order
# Em desenvolvimento verificamos que a base order apresentava duplicidade devido a registros inválidos na data do pedido
# Vamos tratá-los considerando todo registro que tiver na diferença created_at e order_created_at um valor menor ou igual a 1 como válido
join00_ = join00 \
    .withColumn("order_created_at", col("order_created_at").cast(TimestampType())) \
    .withColumn("created_at", col("created_at").cast(TimestampType())) \
    .withColumn("fl_valid", expr("case when datediff(created_at, order_created_at) <= 1 then 1 else 0 end")) \
    .filter(col("fl_valid") == 1) \
    .drop("fl_valid", "created_at")
    
# Cruzamento com as bases consumer e restaurant
# Otimização do join
join00_ \
    .write.mode("overwrite") \
    .bucketBy(int(spark.conf.get("spark.default.parallelism")), "customer_id") \
    .option("compression", "snappy") \
    .format("parquet") \
    .saveAsTable("temp00")

temp00DF = spark.read.table("temp00")

consDF \
    .drop("customer_name") \
    .withColumn("created_at", col("created_at").cast(TimestampType())) \
    .withColumnRenamed("created_at", "costumer_created_at") \
    .write.mode("overwrite") \
    .bucketBy(int(spark.conf.get("spark.default.parallelism")), "customer_id") \
    .option("compression", "snappy") \
    .format("parquet") \
    .saveAsTable("consumer")

consDF_ = spark.read.table("consumer")

restDF \
    .withColumn("created_at", col("created_at").cast(TimestampType())) \
    .withColumnRenamed("id", "merchant_id") \
    .withColumnRenamed("created_at", "merchant_created_at") \
    .write.mode("overwrite") \
    .bucketBy(int(spark.conf.get("spark.default.parallelism")), "merchant_id") \
    .option("compression", "snappy") \
    .format("parquet") \
    .saveAsTable("restaurant")

restDF_ = spark.read.table("restaurant")

# Traz todas as variáveis de consumer
join01 = temp00DF.join(consDF_, on=["customer_id"], how="left")

join01 \
    .write.mode("overwrite") \
    .bucketBy(int(spark.conf.get("spark.default.parallelism")), "merchant_id") \
    .option("compression", "snappy") \
    .format("parquet") \
    .saveAsTable("temp01")

temp01DF = spark.read.table("temp01")

# Traz todas as variáveis de restaurant
join02 = temp01DF.join(restDF_, on=["merchant_id"], how="left")

# Criptografa os dados sensíveis
criptoDF = join02.select(criptografa_df(join02, cripto_ls))

# Gera a base final trusted orders
# Renomeia as colunas e cria as partições para gravação
trustedDF = criptoDF \
    .select(colunas) \
    .withColumn("dt_proc", current_date()) \
    .withColumn("dt", from_utc_timestamp("order_created_at", col("merchant_timezone"))) \
    .withColumn("dt", col("dt").cast(DateType())) \
    .repartition("dt")

trustedDF.write.mode("overwrite").partitionBy("dt").option("compression", "snappy").format("parquet").save(destino_trusted)