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

# Caminho de origem da pouso order-status
origem_pouso = "s3://ifood-landing-order-status/dt={}/*.json".format(ref)

# Caminho de destino da raw order
destino_raw = "s3://ifood-raw-order-status/"

# Inicia sessão spark
spark = SparkSession.builder.appName("ifood-landing-order-status-prod").getOrCreate()

# Configurações básicas para o spark
spark.conf.set("spark.sql.maxPartitionBytes", 200 * 1024 * 1024) # Seta a quantidade máxima de bytes em uma partição ao ler os arquivos de entrada (Entre 100MB e 200MB é o ideal)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") # Necessário para sobrescrever partições  

################################
#   Etapas do Processamento    #
################################

# Lê a pouso de origem
pousoDF = spark.read.json(origem_pouso)

# Cria a tabela raw
rawDF = pousoDF_ \
    .withColumn("dt_proc", current_date()) \
    .withColumn("dt", col("created_at").cast(DateType())) \
    .dropDuplicates()
    
rawDF.write.partitionBy("dt").mode("overwrite").option("compression", "snappy").format("parquet").save(destino_raw)

# Validação
# Leitura da base final
rawDF_ = spark.read.parquet(destino_raw).filter(col("dt") == lit(ref))

# Validação Volumétrica
print("> Volumetria de saída equivale-se a de entrada ? --> {}".format(pousoDF.dropDuplicates().count() == rawDF_.count()))

# Validação do Schema
schema = pousoDF.schema
schema_ = rawDF_.drop("dt", "dt_proc").schema
colunas = pousoDF.columns
colunas_ = rawDF_.drop("dt", "dt_proc").columns
colunas.sort()
colunas_.sort()
print("> O schema de saída equivale-se ao de entrada ? ---> {}".format(schema == schema_ or colunas == colunas_))