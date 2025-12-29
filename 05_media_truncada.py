from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import DecimalType
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder \
    .appName("MediaTruncada") \
    .master("local[*]") \
    .getOrCreate()

df_pedidos = spark.read.json(os.path.join(BASE_DIR, "pedidos.json"))
df_clients = spark.read.json(os.path.join(BASE_DIR, "clients.json"))

df_join = df_pedidos.join(
    df_clients,
    df_pedidos.client_id == df_clients.id,
    how="left"
)

df_total_por_cliente = df_join \
    .groupBy(
        df_clients.id.alias("client_id"),
        df_clients.name.alias("nome_cliente")
    ) \
    .agg(
        Func.coalesce(
            Func.sum(df_pedidos.value),
            Func.lit(0)
        ).cast(DecimalType(11, 2)).alias("valor_total_pedidos")
    )

percentis = df_total_por_cliente \
    .agg(
        Func.expr("percentile_approx(valor_total_pedidos, 0.1)").alias("p10"),
        Func.expr("percentile_approx(valor_total_pedidos, 0.9)").alias("p90")
    )

p10, p90 = percentis.first()

df_media_truncada = df_total_por_cliente \
    .where(
        (Func.col("valor_total_pedidos") > p10) &
        (Func.col("valor_total_pedidos") < p90)) \
    .orderBy(Func.col("valor_total_pedidos").desc())

df_media_truncada.show(truncate=False)

spark.stop()
