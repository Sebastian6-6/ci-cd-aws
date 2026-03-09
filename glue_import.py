import boto3
import json
import os
from botocore.exceptions import ClientError

# --- CONFIGURAÇÃO DE SESSÃO ---
try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")

s3_client = session.client("s3")
glue_client = session.client("glue")
sts_client = session.client("sts")

# Otimização: Cache das variáveis estáticas fora de loops para evitar chamadas redundantes de API
ACCOUNT_ID = sts_client.get_caller_identity()["Account"]
REGION = session.region_name

def save_if_changed(path, new_content, is_json=True):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if is_json:
        content_to_save = json.dumps(new_content, indent=2, ensure_ascii=False, sort_keys=True)
    else:
        content_to_save = new_content

    if os.path.exists(path):
        with open(path, "rb" if not is_json else "r", encoding=None if not is_json else "utf-8") as f:
            old_content = f.read()
        compare_val = content_to_save.encode('utf-8') if is_json else content_to_save
        if old_content == compare_val:
            return False

    mode = "w" if is_json else "wb"
    encoding = "utf-8" if is_json else None
    with open(path, mode, encoding=encoding) as f:
        f.write(content_to_save)
    print(f"[+] ATUALIZADO: {os.path.basename(path)}")
    return True

def purge_orphans(directory, expected_files):
    if not os.path.exists(directory): return
    for filename in os.listdir(directory):
        if filename not in expected_files:
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"[🔥 PURGE] Removido arquivo órfão: {filename}")

def sync_from_s3_prefix(bucket, prefix, local_dir):
    seen_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = prefix.strip('/') + '/'
    
    print(f"📦 Sincronizando Assets de: s3://{bucket}/{prefix}")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key == prefix or key.endswith('/'):
                continue
                
            fname = os.path.basename(key)
            seen_files.append(fname)
            local_path = os.path.join(local_dir, fname)
            
            resp = s3_client.get_object(Bucket=bucket, Key=key)
            save_if_changed(local_path, resp['Body'].read(), is_json=False)
            
    return seen_files

def export_glue():
    j_dir = "./glue/jobs"
    sc_dir = "./glue/scripts"
    lb_dir = "./glue/libs"
    
    # Ponto de atenção para arquiteturas multi-conta: considerar usar os.environ.get("GLUE_BUCKET", "scripts-automacao-pedro")
    bucket_name = "scripts-automacao-pedro" 
    
    # 1. Sincronização de Assets
    seen_scripts = sync_from_s3_prefix(bucket_name, "artifacts/scripts/", sc_dir)
    purge_orphans(sc_dir, seen_scripts)

    seen_libs = sync_from_s3_prefix(bucket_name, "artifacts/libs/", lb_dir)
    purge_orphans(lb_dir, seen_libs)

    # 2. Exportação de Metadados
    seen_jobs = []
    paginator = glue_client.get_paginator("get_jobs")
    for page in paginator.paginate():
        for job in page.get("Jobs", []):
            job_name = job.get('Name')
            name = f"{job_name}.json"
            seen_jobs.append(name)
            
            # Correção do Bug Crítico: Usar o nome do job, não o nome do arquivo json
            job_arn = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/{job_name}"
    
            try:
                tags = glue_client.get_tags(ResourceArn=job_arn).get("Tags", {})
            except ClientError as e:
                # Capturar exceções específicas é Clean Code. Evita mascarar erros estruturais.
                print(f"Aviso: Não foi possível obter tags para {job_name}. Erro: {e}")
                tags = {}

            # Contrato Limpo e Preparado para Deploy
            job_data = {
                "JobName": job.get("Name"),
                "Tags": tags,
                "JobUpdate": {
                    "Description": job.get("Description"),
                    "Role": job.get("Role"),
                    "Command": job.get("Command"),
                    "ExecutionProperty": job.get("ExecutionProperty"), # Resiliência
                    "DefaultArguments": job.get("DefaultArguments"),
                    "NonOverridableArguments": job.get("NonOverridableArguments"),
                    "Connections": job.get("Connections"),
                    "MaxRetries": job.get("MaxRetries"),
                    "Timeout": job.get("Timeout"),
                    "WorkerType": job.get("WorkerType"),
                    "NumberOfWorkers": job.get("NumberOfWorkers"),
                    "SecurityConfiguration": job.get("SecurityConfiguration"), # Segurança
                    "GlueVersion": job.get("GlueVersion"),
                    "ExecutionClass": job.get("ExecutionClass") # FinOps (FLEX/STANDARD)
                }
            }
            save_if_changed(os.path.join(j_dir, name), job_data)
    
    purge_orphans(j_dir, seen_jobs)

if __name__ == "__main__":
    export_glue()