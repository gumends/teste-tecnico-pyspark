from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import DecimalType
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder \
    .appName("AgregacaoPedidos") \
    .master("local[*]") \
    .getOrCreate()

df_pedidos = spark.read.json(os.path.join(BASE_DIR, "pedidos.json"))
df_clients = spark.read.json(os.path.join(BASE_DIR, "clients.json"))

df_join = df_clients.join(
    df_pedidos,
    df_clients.id == df_pedidos.client_id,
    how="left"
)

df_dados_agregados = df_join \
    .groupBy(
        df_clients.id,
        df_clients.name
    ) \
    .agg(
        Func.count(df_pedidos.id).alias("quantidade_pedidos"),
        Func.coalesce(
            Func.sum(df_pedidos.value),
            Func.lit(0)
        ).cast(DecimalType(11, 2)).alias("valor_total_pedidos")
    ) \
    .select(
        df_clients.name.alias("nome_cliente"),
        Func.col("quantidade_pedidos"),
        Func.col("valor_total_pedidos")
    ) \
    .orderBy(Func.col("quantidade_pedidos").desc())

df_dados_agregados.show(truncate=False)

spark.stop()