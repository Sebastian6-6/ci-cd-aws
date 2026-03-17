import boto3

# ==========================================
# CHAOS LAB TEARDOWN (DESTRUIÇÃO CIRÚRGICA)
# ==========================================
REGION = "us-east-2"

s3_client = boto3.client('s3', region_name=REGION)
glue_client = boto3.client('glue', region_name=REGION)
sts_client = boto3.client('sts')

account_id = sts_client.get_caller_identity()["Account"]

# Definição dos alvos
BUCKET_NOVO = f"glu-glu-glu-123-{account_id}"
BUCKET_EXISTENTE = "scripts-automacao-pedro"

JOBS_TO_DELETE = [
    "Job_Pagamentos", "Job_Limpeza", "Job_DW", "Job_Legado", "Job_LGPD", "Job_Raw",
    "Job_Hubspot", "Job_CEPs", "Job_Parceiros", "Job_Relatorio"
]

KEYS_IN_EXISTING_BUCKET = [
    "artifacts/libs/lib_aws_utils.py",
    "artifacts/libs/lib_data_quality.py",
    "artifacts/libs/lib_criptografia.py",
    "artifacts/scripts/ext_crm_hubspot.py",
    "artifacts/scripts/trs_padronizacao_ceps.py",
    "artifacts/scripts/load_api_parceiros.py",
    "artifacts/scripts/relatorio_fechamento.py",
    "artifacts/scripts/utils_processamento.py",
    "artifacts/scripts/config_banco.py"
]

print(f"--- INICIANDO PURGE DO CHAOS LAB (CONTA: {account_id}) ---")

# ==========================================
# 1. LIMPEZA DOS JOBS DO GLUE
# ==========================================
print("\n[GLUE] Removendo Jobs de teste...")
deleted_jobs = 0
for job_name in JOBS_TO_DELETE:
    try:
        glue_client.delete_job(JobName=job_name)
        print(f"  -> Deletado: {job_name}")
        deleted_jobs += 1
    except glue_client.exceptions.EntityNotFoundException:
        print(f"  -> Ignorado: {job_name} (Não encontrado)")
    except Exception as e:
        print(f"❌ Erro ao deletar {job_name}: {e}")
print(f"✅ {deleted_jobs} Jobs removidos.")

# ==========================================
# 2. LIMPEZA CIRÚRGICA NO BUCKET EXISTENTE
# ==========================================
print(f"\n[S3] Removendo apenas os arquivos injetados em: {BUCKET_EXISTENTE}")
deleted_keys = 0
for key in KEYS_IN_EXISTING_BUCKET:
    try:
        s3_client.delete_object(Bucket=BUCKET_EXISTENTE, Key=key)
        print(f"  -> Arquivo deletado: {key}")
        deleted_keys += 1
    except Exception as e:
        print(f"❌ Erro ao deletar {key}: {e}")
print(f"✅ {deleted_keys} arquivos removidos cirurgicamente.")

# ==========================================
# 3. PURGE TOTAL DO BUCKET NOVO
# ==========================================
print(f"\n[S3] Esvaziando e deletando o bucket efêmero: {BUCKET_NOVO}")
try:
    # Passo 1: Esvaziar o bucket (Bypass do erro BucketNotEmpty)
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NOVO)
    
    total_objects_deleted = 0
    for page in pages:
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            s3_client.delete_objects(
                Bucket=BUCKET_NOVO,
                Delete={'Objects': objects_to_delete}
            )
            total_objects_deleted += len(objects_to_delete)
    
    print(f"  -> {total_objects_deleted} objetos deletados internamente.")
    
    # Passo 2: Deletar a infraestrutura do bucket
    s3_client.delete_bucket(Bucket=BUCKET_NOVO)
    print(f"✅ Bucket {BUCKET_NOVO} deletado com sucesso.")

except s3_client.exceptions.NoSuchBucket:
    print(f"⚠️ O bucket {BUCKET_NOVO} já não existe.")
except Exception as e:
    print(f"❌ Erro ao deletar o bucket novo: {e}")

print("\n--- TEARDOWN FINALIZADO. AMBIENTE LIMPO E SEGURO. ---")