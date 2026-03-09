import boto3
import json
import os
from botocore.exceptions import ClientError

try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")

sfn_client = session.client("stepfunctions")

def save_if_changed(path, new_content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    content_to_save = json.dumps(new_content, indent=2, ensure_ascii=False, sort_keys=True)

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            if f.read() == content_to_save:
                return False

    with open(path, "w", encoding="utf-8") as f:
        f.write(content_to_save)
    print(f"[+] SFN ATUALIZADA: {os.path.basename(path)}")
    return True

def purge_orphans(directory, expected_files):
    if not os.path.exists(directory): return
    for filename in os.listdir(directory):
        if filename not in expected_files:
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"[🔥 PURGE] Removido arquivo órfão: {filename}")

def export_stepfunctions():
    out_dir = "./stepfunctions"
    seen_files = []
    paginator = sfn_client.get_paginator("list_state_machines")
    
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            name = f"{sm['name']}.json"
            seen_files.append(name)
            det = sfn_client.describe_state_machine(stateMachineArn=sm["stateMachineArn"])
            
            data = {
                "roleArn": det.get("roleArn"),
                "type": det.get("type"),
                "definition": json.loads(det.get("definition", "{}")),
                "loggingConfiguration": det.get("loggingConfiguration"),
                "tracingConfiguration": det.get("tracingConfiguration")
            }
            save_if_changed(os.path.join(out_dir, name), data)
            
    purge_orphans(out_dir, seen_files)

if __name__ == "__main__":
    export_stepfunctions()