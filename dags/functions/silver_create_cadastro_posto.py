from pyspark.sql import SparkSession


def create_spark_session(connection_id: str = "aws"):
    spark = (
        SparkSession
        .builder
        .config("com.amazonaws.services.s3.enableV4", "true")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    return spark


def optimize_compaction_and_run_vacuum(spark: SparkSession, path: str = ""):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retentionHours=168)
    delta_table.optimize().executeCompaction()


def create_cadastro_posto_table():
    from pyspark.sql.functions import col, when, regexp_replace, trim

    spark = create_spark_session()
    df = spark.read.format("delta").load("s3a://etl-lakehouse/BRONZE/anp/", header=True)

    cadastro_posto_df = (
        # Seleciona as colunas de interesse e renomeia
        df.select(
            "regiao_sigla",
            "estado_sigla",
            "municipio",
            col("revenda").alias("nome_posto"),
            col("cnpj_da_revenda").alias("cnpj"),
            "nome_da_rua",
            "numero_rua",
            "complemento",
            "bairro",
            "cep",
            "data_da_coleta",
            "bandeira"
        )
        # Remove pontuações do cnpj e do cep
        .withColumn("cnpj", trim(regexp_replace("cnpj", r"[.\-/]", "")))
        .withColumn("cep", regexp_replace("cep", "-", ""))
        # Converte número da rua para int
        .withColumn("numero_rua", regexp_replace("numero_rua", ",", "").cast("int"))
        # Renomeia regioes
        .withColumn("regiao_sigla", when(col("regiao_sigla") == "NE", "nordeste")
                    .when(col("regiao_sigla") == "N", "norte")
                    .when(col("regiao_sigla") == "S", "sul")
                    .when(col("regiao_sigla") == "N", "norte")
                    .when(col("regiao_sigla") == "SE", "sudeste")
                    .when(col("regiao_sigla") == "CO", "centro-oeste"))
    )

    # Salva dados cadastrais dos postos na camada silver
    (
        cadastro_posto_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("header", "true")
        .save("s3a://etl-lakehouse/SILVER/cadastro_posto/")
    )

    # Executa otimização da compactação par melhorar velocidade de leitura das tabelas
    optimize_compaction_and_run_vacuum(spark, path="s3a://etl-lakehouse/SILVER/cadastro_posto/")


if __name__ == "__main__":
    create_cadastro_posto_table()
