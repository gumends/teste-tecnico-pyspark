from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import DecimalType
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder \
    .appName("AnaiseEstatistica") \
    .master("local[*]") \
    .getOrCreate()

df_pedidos = spark.read.json(os.path.join(BASE_DIR, "pedidos.json"))
df_clients = spark.read.json(os.path.join(BASE_DIR, "clients.json"))

df_join = df_pedidos.join(
    df_clients,
    df_pedidos.client_id == df_clients.id,
    how="left"
)

df_clientes_total = df_join \
    .groupBy(
        df_clients.id.alias("client_id"),
        df_clients.name.alias("nome_cliente")) \
    .agg(
        Func.coalesce(Func.sum(df_pedidos.value), Func.lit(0))
        .cast(DecimalType(11, 2))
        .alias("valor_total_pedidos"),
        Func.count(df_pedidos.id).alias("quantidade_pedidos"))

df_estatisticas = df_clientes_total.agg(
    Func.avg("valor_total_pedidos").cast(DecimalType(11, 2)).alias("media"),
    Func.expr("percentile_approx(valor_total_pedidos, 0.5)").alias("mediana"),
    Func.expr("percentile_approx(valor_total_pedidos, 0.1)").alias("p10"),
    Func.expr("percentile_approx(valor_total_pedidos, 0.9)").alias("p90")
)

df_estatisticas.show(truncate=False)

spark.stop()