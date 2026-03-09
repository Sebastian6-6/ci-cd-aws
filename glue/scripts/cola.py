import sys
import os
from datetime import datetime

# 1. Trava de Segurança para o GitHub Actions
if os.getenv('GITHUB_ACTIONS') == 'true':
    print("Validação de sintaxe concluída para a Versão 4.0.")
    sys.exit(0)

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.functions import col, when

# Inicialização do Contexto
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"--- Iniciando Processamento - {datetime.now()} ---")

# 2. Criando dados em memória (Sem custo de leitura de S3)
data = [
    Row(alimento="Whey Protein", categoria="Proteína", gramas=30),
    Row(alimento="Pasta de Amendoim", categoria="Gordura", gramas=15),
    Row(alimento="Brócolis", categoria="Vegetal", gramas=100),
    Row(alimento="Ovos", categoria="Proteína", gramas=120),
    Row(alimento="Doce de Leite", categoria="Carbo", gramas=20)
]

df = spark.createDataFrame(data)

# 3. Transformação: Classificando alimentos de alta proteína
df_final = df.withColumn(
    "prioridade_dieta", 
    when(col("categoria") == "Proteína", "Alta").otherwise("Normal")
)

# 4. Exibindo o resultado nos Logs (Interesting Output)
print("Schema do Script Gerado:")
df_final.printSchema()

print("Relatório de Alimentos Processados:")
df_final.show()

# 5. Agregação simples para ver o poder do Spark
print("Resumo por Categoria:")
df_final.groupBy("categoria").count().show()

job.commit()
print("--- Processo Finalizado com Sucesso ---")