import boto3
import json
import os
import re
from botocore.exceptions import ClientError

# --- CONFIGURAÇÃO DE SESSÃO ---
# Localmente usa o perfil 'pessoal'. No GitHub Actions, o Boto3 ignora e usa OIDC/EnvVars.
try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")

def save_if_changed(path, new_data):
    """
    Salva o JSON apenas se houver mudanças reais.
    Usa sort_keys=True para garantir que a comparação de hash/string seja consistente.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_content = json.dumps(new_data, indent=2, ensure_ascii=False, sort_keys=True)
    
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            old_content = f.read()
        if old_content == new_content:
            print(f"[-] Sem mudanças: {os.path.basename(path)}")
            return False

    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"[+] ATUALIZADO: {os.path.basename(path)}")
    return True

# --- EXPORTADORES ---

def export_stepfunctions(out_dir="./stepfunctions"):
    """Exporta State Machines removendo ARNs e metadados de execução."""
    sfn = session.client("stepfunctions")
    paginator = sfn.get_paginator("list_state_machines")

    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            name = sm["name"]
            try:
                desc = sfn.describe_state_machine(stateMachineArn=sm["stateMachineArn"])
                clean_data = {
                    "roleArn": desc.get("roleArn"),
                    "type": desc.get("type"),
                    "definition": json.loads(desc.get("definition")), # Transforma string em objeto
                    "loggingConfiguration": desc.get("loggingConfiguration"),
                    "tracingConfiguration": desc.get("tracingConfiguration")
                }
                save_if_changed(os.path.join(out_dir, f"{name}.json"), clean_data)
            except ClientError as e:
                print(f"Erro ao exportar Step Function {name}: {e}")

def export_eb_rules(out_dir="./eventbridge/regras"):
    """Exporta Regras do EventBridge ignorando as gerenciadas pela AWS."""
    ev = session.client("events")
    paginator = ev.get_paginator("list_rules")

    for page in paginator.paginate(EventBusName="default"):
        for rule in page.get("Rules", []):
            if rule.get("ManagedBy"): continue # Governança: Ignora regras de sistema
            
            name = rule["Name"]
            try:
                desc = ev.describe_rule(Name=name)
                targets = ev.list_targets_by_rule(Rule=name).get("Targets", [])
                
                clean_data = {
                    "Rule": {
                        "Name": name,
                        "EventPattern": json.loads(desc["EventPattern"]),
                        "State": desc["State"],
                        "Description": desc.get("Description", "")
                    },
                    "Targets": [
                        {"Id": t["Id"], "Arn": t["Arn"], "RoleArn": t.get("RoleArn")} 
                        for t in targets
                    ]
                }
                save_if_changed(os.path.join(out_dir, f"{name}.json"), clean_data)
            except ClientError as e:
                print(f"Erro ao exportar Rule {name}: {e}")

def export_eb_schedules(out_dir="./eventbridge/cronogramas"):
    """Exporta Schedules mantendo a configuração completa para deploy direto."""
    scheduler = session.client("scheduler")
    
    try:
        groups = scheduler.list_schedule_groups().get("ScheduleGroups", [])
        for group in groups:
            g_name = group["Name"]
            paginator = scheduler.get_paginator("list_schedules")
            
            for page in paginator.paginate(GroupName=g_name):
                for s in page.get("Schedules", []):
                    name = s["Name"]
                    try:
                        det = scheduler.get_schedule(Name=name, GroupName=g_name)
                        clean_data = {
                            "Name": name,
                            "GroupName": g_name,
                            "ScheduleExpression": det["ScheduleExpression"],
                            "ScheduleExpressionTimezone": det.get("ScheduleExpressionTimezone", "America/Sao_Paulo"),
                            "State": det["State"],
                            "FlexibleTimeWindow": det["FlexibleTimeWindow"],
                            "Target": {
                                "Arn": det["Target"]["Arn"],
                                "RoleArn": det["Target"]["RoleArn"],
                                "Input": det["Target"].get("Input", "{}")
                            }
                        }
                        save_if_changed(os.path.join(out_dir, f"{name}.json"), clean_data)
                    except ClientError as e:
                        print(f"Erro ao exportar Schedule {name}: {e}")
    except ClientError as e:
        print(f"Erro ao listar Schedule Groups: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando Exportação Limpa (Opção 1)...")
    export_stepfunctions()
    export_eb_rules()
    export_eb_schedules()
    print("\n✅ Processo finalizado com sucesso.")