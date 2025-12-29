from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import DecimalType
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder \
    .appName("AcimaMedia") \
    .master("local[*]") \
    .getOrCreate()

df_pedidos = spark.read.json(os.path.join(BASE_DIR, "pedidos.json"))
df_clients = spark.read.json(os.path.join(BASE_DIR, "clients.json"))

df_join = df_pedidos.join(
    df_clients,
    df_pedidos.client_id == df_clients.id,
    how="inner"
)

df_dados_agregados = df_join.groupBy(
    df_clients.id.alias("client_id"),
    df_clients.name.alias("nome_cliente")
).agg(
    Func.count(df_pedidos.id).alias("quantidade_pedidos"),
    Func.sum(df_pedidos.value).alias("valor_total_pedidos").cast(DecimalType(11, 2)),
)

df_media = df_dados_agregados.select(
    Func.avg("valor_total_pedidos").alias("media_total")
).first()["media_total"]

df_resultado = df_dados_agregados \
    .where(
        Func.col("valor_total_pedidos") >
        Func.lit(df_media).cast(DecimalType(11, 2))
    ) \
    .withColumn(
        "media_total_valor_pedidos",
        Func.lit(df_media).cast(DecimalType(11, 2))
    ) \
    .select(
        Func.col("nome_cliente"),
        Func.col("valor_total_pedidos"),
        Func.col("quantidade_pedidos")
    ) \
    .orderBy(Func.col("valor_total_pedidos").desc())

df_resultado.show(truncate=False)

spark.stop()
