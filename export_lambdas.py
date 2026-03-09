import boto3
import json
import os
import shutil  # Adicione esta linha
import zipfile
import io
import requests
from botocore.exceptions import ClientError
from urllib.parse import urlparse


try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")
# --- UTILITÁRIO ADICIONAL PARA PASTAS ---

def purge_orphans_dir(directory, expected_dirs):
    """Remove pastas locais (Lambdas) que não existem mais na AWS."""
    if not os.path.exists(directory): return
    for name in os.listdir(directory):
        dir_path = os.path.join(directory, name)
        if os.path.isdir(dir_path) and name not in expected_dirs:
            import shutil
            shutil.rmtree(dir_path)
            print(f"[🔥 PURGE] Removida pasta de Lambda órfã: {name}")

# --- EXPORTADOR DE LAMBDAS ---

def export_lambdas(base_dir="./lambda"):
    """
    Sincroniza código das Lambdas com cache de Hash e Purge de funções deletadas.
    """
    print(f"🌐 Sincronizando Lambdas em: {base_dir}")
    os.makedirs(base_dir, exist_ok=True)
    
    client = session.client("lambda")
    seen_functions = []
    
    try:
        paginator = client.get_paginator('list_functions')
        for page in paginator.paginate():
            for function in page.get('Functions', []):
                name = function['FunctionName']
                seen_functions.append(name) # Para o Purge posterior
                
                func_folder = os.path.join(base_dir, name)
                hash_file = os.path.join(func_folder, ".aws_hash")
                
                # Pegamos a localização do código e o Hash atual da AWS
                response = client.get_function(FunctionName=name)
                aws_hash = response['Configuration']['CodeSha256']
                code_url = response['Code']['Location']

                # Lógica de Idempotência via Hash
                if os.path.exists(hash_file):
                    with open(hash_file, "r") as f:
                        if f.read() == aws_hash:
                            # print(f"[-] Sem mudanças: {name}")
                            continue

                # Download e Extração (Só ocorre se o Hash mudar)
                print(f"[+] ATUALIZANDO: {name} (Novo Hash detectado)")
                r = requests.get(code_url)
                if r.status_code == 200:
                    os.makedirs(func_folder, exist_ok=True)
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zip_ref:
                        # Extração limpa: remove arquivos antigos antes de extrair novos
                        # Isso evita que arquivos deletados no código continuem na pasta local
                        for f in os.listdir(func_folder):
                            if f != ".aws_hash":
                                p = os.path.join(func_folder, f)
                                if os.path.isfile(p): os.remove(p)
                                elif os.path.isdir(p): shutil.rmtree(p)
                        
                        zip_ref.extractall(func_folder)
                    
                    # Salva o novo Hash para a próxima comparação
                    with open(hash_file, "w") as f:
                        f.write(aws_hash)
        
        # Remove pastas de Lambdas que foram deletadas no Console
        purge_orphans_dir(base_dir, seen_functions)

    except ClientError as e:
        print(f"❌ Erro ao exportar Lambdas: {e}")

# No bloco final, adicione:
if __name__ == "__main__":
    # ... outros exportadores
    export_lambdas()