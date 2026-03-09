import boto3
import json
import os
import shutil
import zipfile
import io
import requests
from botocore.exceptions import ClientError

try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")

def sanitize_env_vars(env_dict):
    """
    Heurística de segurança: Mascara valores de chaves que pareçam sensíveis
    para evitar o vazamento de segredos no repositório Git.
    """
    if not env_dict:
        return {}
        
    sensitive_keywords = ['PASS', 'SECRET', 'KEY', 'TOKEN', 'PWD', 'AUTH', 'CREDENTIAL']
    sanitized = {}
    
    for k, v in env_dict.items():
        is_sensitive = any(word in k.upper() for word in sensitive_keywords)
        if is_sensitive:
            sanitized[k] = "🔒 ***REDACTED_BY_EXPORT_SCRIPT***"
        else:
            sanitized[k] = v
            
    return sanitized

def purge_orphans_dir(directory, expected_dirs):
    """Remove pastas locais (Lambdas) que foram deletadas no Console da AWS."""
    if not os.path.exists(directory): return
    for name in os.listdir(directory):
        dir_path = os.path.join(directory, name)
        if os.path.isdir(dir_path) and name not in expected_dirs:
            shutil.rmtree(dir_path)
            print(f"[🔥 PURGE] Removida pasta de Lambda órfã: {name}")

def export_lambdas(base_dir="./lambda"):
    print(f"🌐 Sincronizando Lambdas em: {base_dir}")
    os.makedirs(base_dir, exist_ok=True)
    
    client = session.client("lambda")
    seen_functions = []
    
    try:
        paginator = client.get_paginator('list_functions')
        for page in paginator.paginate():
            for function in page.get('Functions', []):
                name = function['FunctionName']
                seen_functions.append(name)
                
                func_folder = os.path.join(base_dir, name)
                config_file = os.path.join(func_folder, "config.json")
                os.makedirs(func_folder, exist_ok=True)
                
                response = client.get_function(FunctionName=name)
                conf = response['Configuration']
                code_url = response['Code']['Location']

                # 1. Exporta as configurações mascarando segredos
                raw_env_vars = conf.get("Environment", {}).get("Variables", {})
                
                config_data = {
                    "Runtime": conf.get("Runtime"),
                    "Handler": conf.get("Handler"),
                    "MemorySize": conf.get("MemorySize"),
                    "Timeout": conf.get("Timeout"),
                    "Environment": sanitize_env_vars(raw_env_vars),
                    "Layers": [layer["Arn"] for layer in conf.get("Layers", [])]
                }
                
                with open(config_file, "w", encoding="utf-8") as f:
                    json.dump(config_data, f, indent=2, ensure_ascii=False, sort_keys=True)

                # 2. Download e Extração Limpa do Código
                r = requests.get(code_url)
                if r.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zip_ref:
                        # Remove arquivos locais antigos (exceto config.json) 
                        # para garantir que arquivos deletados na AWS sumam do Git
                        for item in os.listdir(func_folder):
                            if item != "config.json":
                                p = os.path.join(func_folder, item)
                                if os.path.isfile(p): os.remove(p)
                                elif os.path.isdir(p): shutil.rmtree(p)
                        
                        zip_ref.extractall(func_folder)
                
                print(f"[+] Sincronizada: {name}")

        purge_orphans_dir(base_dir, seen_functions)

    except ClientError as e:
        print(f"❌ Erro ao exportar Lambdas: {e}")

if __name__ == "__main__":
    export_lambdas()