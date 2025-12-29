from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder \
    .appName("DataQualityPedidos") \
    .master("local[*]") \
    .getOrCreate()

df_pedidos = spark.read.json(os.path.join(BASE_DIR, "pedidos.json"))
df_clients = spark.read.json(os.path.join(BASE_DIR, "clients.json"))

df_pedidos_valores_nulos = df_pedidos \
    .filter(Func.col("value").isNull()) \
    .select(
        Func.col("id"),
        Func.lit("o value é nulo").alias("motivo")
    )

df_pedidos_valores_negativos = df_pedidos \
    .filter(Func.col("value") < 0 ) \
        .select(
            Func.col("id"),
            Func.lit("value negativo").alias("motivo")
        )

dq_cliente_inexistente = df_pedidos \
    .join(
        df_clients,
        df_pedidos.client_id == df_clients.id,
        how="left_anti") \
    .select(
        df_pedidos.id,
        Func.lit("client_id inexistente").alias("motivo")
    )

df_data_quality = dq_cliente_inexistente \
    .unionByName(df_pedidos_valores_negativos) \
    .unionByName(df_pedidos_valores_nulos)

df_data_quality.show(truncate=False)

spark.stop()