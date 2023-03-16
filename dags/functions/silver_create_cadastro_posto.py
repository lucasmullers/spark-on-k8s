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
    delta_table.optimize().executeCompaction()
    delta_table.vacuum(retentionHours=168)


def create_cadastro_posto_table():
    from pyspark.sql.functions import col, when, regexp_replace, trim

    spark = create_spark_session()
    df = spark.read.format("delta").load("s3a://etl-data-lakehouse/BRONZE/anp/", header=True)

    cadastro_posto_df = (
        # Seleciona as colunas de interesse e renomeia
        df.select(
            "regiao",
            "estado",
            "municipio",
            "nome_posto",
            "cnpj",
            "nome_da_rua",
            "numero_rua",
            "complemento",
            "bairro",
            "cep",
            "data_coleta",
            "bandeira"
        )
        # Remove pontuações do cnpj e do cep
        .withColumn("cnpj", trim(regexp_replace("cnpj", r"[.\-/]", "")))
        .withColumn("cep", regexp_replace("cep", "-", ""))
        # Converte número da rua para int
        .withColumn("numero_rua", regexp_replace("numero_rua", ",", "").cast("int"))
        # Renomeia regioes
        .withColumn("regiao", when(col("regiao") == "NE", "nordeste")
                    .when(col("regiao") == "N", "norte")
                    .when(col("regiao") == "S", "sul")
                    .when(col("regiao") == "N", "norte")
                    .when(col("regiao") == "SE", "sudeste")
                    .when(col("regiao") == "CO", "centro-oeste"))
    )

    # Salva dados cadastrais dos postos na camada silver
    (
        cadastro_posto_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("header", "true")
        .save("s3a://etl-data-lakehouse/SILVER/cadastro_posto/")
    )

    # Executa otimização da compactação par melhorar velocidade de leitura das tabelas
    optimize_compaction_and_run_vacuum(spark, path="s3a://etl-data-lakehouse/SILVER/cadastro_posto/")


if __name__ == "__main__":
    create_cadastro_posto_table()
