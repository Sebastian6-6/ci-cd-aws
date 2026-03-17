import boto3

# ==========================================
# CHAOS LAB SEEDER (INFRASTRUCTURE AS CODE)
# ==========================================
REGION = "us-east-2"
IAM_ROLE_ARN = "arn:aws:iam::338578590490:role/GlueServiceRole" # Altere para a sua Role

s3_client = boto3.client('s3', region_name=REGION)
glue_client = boto3.client('glue', region_name=REGION)
sts_client = boto3.client('sts')

account_id = sts_client.get_caller_identity()["Account"]

# Definição dos Buckets
BUCKET_NOVO = f"glu-glu-glu-123-{account_id}"
BUCKET_EXISTENTE = "scripts-automacao-pedro" # Assumindo que você já o criou

print(f"--- INICIANDO CRIAÇÃO DO CHAOS LAB (CONTA: {account_id}) ---")

# 1. Garantir a existência do bucket novo
try:
    s3_client.create_bucket(
        Bucket=BUCKET_NOVO,
        CreateBucketConfiguration={'LocationConstraint': REGION}
    )
    print(f"✅ Bucket novo criado: {BUCKET_NOVO}")
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    print(f"✅ Bucket {BUCKET_NOVO} já existe.")

# ==========================================
# 2. DEFINIÇÃO DA TOPOLOGIA DE DADOS (ARQUIVOS)
# ==========================================

# LIBS (Bucket Existente)
libs_mock = {
    "artifacts/libs/lib_aws_utils.py": "print('LIB: lib_aws_utils.py - Funcoes de S3 e Boto3')",
    "artifacts/libs/lib_data_quality.py": "print('LIB: lib_data_quality.py - Validacao de nulos e schemas')",
    "artifacts/libs/lib_criptografia.py": "print('LIB: lib_criptografia.py - Mascaramento de PII')"
}

# SCRIPTS BUCKET 1 (12 Scripts no total)
b1_scripts = {
    # 6 Ativos (Serão atrelados a jobs)
    "job_scripts/ext_api_pagamentos.py": "print('SCRIPT: ext_api_pagamentos.py | Origem: B1')",
    "job_scripts/trs_limpeza_cadastros.py": "print('SCRIPT: trs_limpeza_cadastros.py | Origem: B1')",
    "job_scripts/load_dw_faturamento.py": "print('SCRIPT: load_dw_faturamento.py | Origem: B1')",
    "job_scripts/ext_banco_legado.py": "print('SCRIPT: ext_banco_legado.py | Origem: B1')",
    "job_scripts/trs_anonimizacao_lgpd.py": "print('SCRIPT: trs_anonimizacao_lgpd.py | Origem: B1')",
    "job_scripts/load_datalake_raw.py": "print('SCRIPT: load_datalake_raw.py | Origem: B1')",
    
    # 6 Chaos (Conflitos e lixo)
    "job_scripts/ext_api_pagamentos_v2.py": "print('SCRIPT: ext_api_pagamentos.py | Origem: B1')", # Mesmo conteúdo, nome diferente
    "job_scripts/bkp_limpeza_cadastros.py": "print('SCRIPT: trs_limpeza_cadastros.py | Origem: B1')", # Mesmo conteúdo, nome diferente
    "job_scripts/utils_processamento.py": "print('SCRIPT: utils_processamento.py | CONTEUDO B1 - CUIDADO')", # Colisão de nome com B2
    "job_scripts/config_banco.py": "print('SCRIPT: config_banco.py | CONTEUDO B1 - CUIDADO')", # Colisão de nome com B2
    "job_scripts/rascunho_teste.py": "print('SCRIPT: rascunho_teste.py | Lixo isolado')",
    "job_scripts/old_script_2023.py": "print('SCRIPT: old_script_2023.py | Lixo isolado')"
}

# SCRIPTS BUCKET 2 (6 Scripts no total)
b2_scripts = {
    # 4 Ativos (Serão atrelados a jobs)
    "artifacts/scripts/ext_crm_hubspot.py": "print('SCRIPT: ext_crm_hubspot.py | Origem: B2')",
    "artifacts/scripts/trs_padronizacao_ceps.py": "print('SCRIPT: trs_padronizacao_ceps.py | Origem: B2')",
    "artifacts/scripts/load_api_parceiros.py": "print('SCRIPT: load_api_parceiros.py | Origem: B2')",
    "artifacts/scripts/relatorio_fechamento.py": "print('SCRIPT: relatorio_fechamento.py | Origem: B2')",
    
    # 2 Chaos (Colisão de nome com B1, mas conteúdo diferente)
    "artifacts/scripts/utils_processamento.py": "print('SCRIPT: utils_processamento.py | CONTEUDO B2 - CUIDADO MIGRACAO')",
    "artifacts/scripts/config_banco.py": "print('SCRIPT: config_banco.py | CONTEUDO B2 - CUIDADO MIGRACAO')"
}

# ==========================================
# 3. UPLOAD PARA O S3
# ==========================================
print("\n[S3] Realizando upload dos artefatos...")

for path, content in libs_mock.items():
    s3_client.put_object(Bucket=BUCKET_EXISTENTE, Key=path, Body=content.encode('utf-8'))

for path, content in b2_scripts.items():
    s3_client.put_object(Bucket=BUCKET_EXISTENTE, Key=path, Body=content.encode('utf-8'))

for path, content in b1_scripts.items():
    s3_client.put_object(Bucket=BUCKET_NOVO, Key=path, Body=content.encode('utf-8'))

print("✅ Upload concluído: 3 Libs, 12 Scripts no B1, 6 Scripts no B2.")

# ==========================================
# 4. CRIAÇÃO DOS GLUE JOBS (10 JOBS)
# ==========================================
print("\n[GLUE] Provisionando Jobs com vínculos de dependência...")

job_definitions = [
    # Jobs apontando para B1 (6 Jobs)
    {"name": "Job_Pagamentos", "script": f"s3://{BUCKET_NOVO}/job_scripts/ext_api_pagamentos.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_aws_utils.py"},
    {"name": "Job_Limpeza", "script": f"s3://{BUCKET_NOVO}/job_scripts/trs_limpeza_cadastros.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_data_quality.py"},
    {"name": "Job_DW", "script": f"s3://{BUCKET_NOVO}/job_scripts/load_dw_faturamento.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_criptografia.py"},
    {"name": "Job_Legado", "script": f"s3://{BUCKET_NOVO}/job_scripts/ext_banco_legado.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_aws_utils.py"},
    {"name": "Job_LGPD", "script": f"s3://{BUCKET_NOVO}/job_scripts/trs_anonimizacao_lgpd.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_criptografia.py"},
    {"name": "Job_Raw", "script": f"s3://{BUCKET_NOVO}/job_scripts/load_datalake_raw.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_data_quality.py"},
    
    # Jobs apontando para B2 (4 Jobs)
    {"name": "Job_Hubspot", "script": f"s3://{BUCKET_EXISTENTE}/artifacts/scripts/ext_crm_hubspot.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_aws_utils.py"},
    {"name": "Job_CEPs", "script": f"s3://{BUCKET_EXISTENTE}/artifacts/scripts/trs_padronizacao_ceps.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_data_quality.py"},
    {"name": "Job_Parceiros", "script": f"s3://{BUCKET_EXISTENTE}/artifacts/scripts/load_api_parceiros.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_criptografia.py"},
    {"name": "Job_Relatorio", "script": f"s3://{BUCKET_EXISTENTE}/artifacts/scripts/relatorio_fechamento.py", "lib": f"s3://{BUCKET_EXISTENTE}/artifacts/libs/lib_aws_utils.py"}
]

for job in job_definitions:
    try:
        glue_client.create_job(
            Name=job["name"],
            Role=IAM_ROLE_ARN,
            Command={
                'Name': 'pythonshell',
                'ScriptLocation': job["script"],
                'PythonVersion': '3.9'
            },
            DefaultArguments={
                '--extra-py-files': job["lib"]
            },
            MaxCapacity=0.0625
        )
        print(f"  -> Criado: {job['name']} | Lib: {job['lib'].split('/')[-1]}")
    except glue_client.exceptions.IdempotentParameterMismatchException:
        print(f"  -> Job {job['name']} já existe com outra configuração. Delete-o antes.")
    except Exception as e:
        print(f"❌ Erro ao criar {job['name']}: {e}")

print("\n--- CHAOS LAB PRONTO PARA TESTE DE ESTRESSE ---")