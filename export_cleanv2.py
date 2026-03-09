import boto3
import json
import os
from botocore.exceptions import ClientError
from urllib.parse import urlparse

# --- CONFIGURAÇÃO DE SESSÃO ---
try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")

s3_client = session.client("s3")
glue_client = session.client("glue")
sfn_client = session.client("stepfunctions")
eb_client = session.client("events")
scheduler_client = session.client("scheduler")

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
    """Remove arquivos locais que não estão na lista de arquivos esperados da AWS."""
    if not os.path.exists(directory): return
    for filename in os.listdir(directory):
        if filename not in expected_files:
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"[🔥 PURGE] Removido arquivo órfão: {filename}")

# --- EXPORTADORES ---

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
                "roleArn": det["roleArn"], "type": det["type"],
                "definition": json.loads(det["definition"]),
                "loggingConfiguration": det.get("loggingConfiguration"),
                "tracingConfiguration": det.get("tracingConfiguration")
            }
            save_if_changed(os.path.join(out_dir, name), data)
    purge_orphans(out_dir, seen_files)

def export_eventbridge():
    # Rules
    r_dir, s_dir = "./eventbridge/regras", "./eventbridge/cronogramas"
    seen_rules, seen_scheds = [], []
    
    for rule in eb_client.list_rules()["Rules"]:
        if rule.get("ManagedBy"): continue
        name = f"{rule['Name']}.json"
        seen_rules.append(name)
        desc = eb_client.describe_rule(Name=rule['Name'])
        targets = eb_client.list_targets_by_rule(Rule=rule['Name'])["Targets"]
        save_if_changed(os.path.join(r_dir, name), {
            "Rule": {"Name": rule['Name'], "EventPattern": json.loads(desc["EventPattern"]), "State": desc["State"]},
            "Targets": [{"Id": t["Id"], "Arn": t["Arn"]} for t in targets]
        })
    purge_orphans(r_dir, seen_rules)

    # Schedules
    for group in scheduler_client.list_schedule_groups()["ScheduleGroups"]:
        for s in scheduler_client.list_schedules(GroupName=group["Name"])["Schedules"]:
            name = f"{s['Name']}.json"
            seen_scheds.append(name)
            det = scheduler_client.get_schedule(Name=s['Name'], GroupName=group["Name"])
            save_if_changed(os.path.join(s_dir, name), {
                "Name": s['Name'], "GroupName": group["Name"], "ScheduleExpression": det["ScheduleExpression"],
                "Target": {"Arn": det["Target"]["Arn"], "RoleArn": det["Target"]["RoleArn"], "Input": det["Target"].get("Input", "{}")},
                "State": det["State"], "FlexibleTimeWindow": det["FlexibleTimeWindow"]
            })
    purge_orphans(s_dir, seen_scheds)

def export_glue():
    j_dir, sc_dir, lb_dir = "./glue/jobs", "./glue/scripts", "./glue/libs"
    seen_jobs, seen_scripts, seen_libs = [], [], []

    for job in glue_client.get_jobs()["Jobs"]:
        name = f"{job['Name']}.json"
        seen_jobs.append(name)
        
        # Download Script
        script_path = job.get("Command", {}).get("ScriptLocation", "")
        if script_path:
            p = urlparse(script_path)
            fname = os.path.basename(p.path)
            seen_scripts.append(fname)
            resp = s3_client.get_object(Bucket=p.netloc, Key=p.path.lstrip('/'))
            save_if_changed(os.path.join(sc_dir, fname), resp['Body'].read(), is_json=False)

        # Download Libs
        libs = job.get("DefaultArguments", {}).get("--extra-py-files", "").split(",")
        for lib_path in filter(None, libs):
            p = urlparse(lib_path.strip())
            fname = os.path.basename(p.path)
            seen_libs.append(fname)
            resp = s3_client.get_object(Bucket=p.netloc, Key=p.path.lstrip('/'))
            save_if_changed(os.path.join(lb_dir, fname), resp['Body'].read(), is_json=False)

        save_if_changed(os.path.join(j_dir, name), {
            "Name": job["Name"], "Role": job["Role"], "Command": job["Command"],
            "DefaultArguments": job.get("DefaultArguments"), "WorkerType": job.get("WorkerType"),
            "NumberOfWorkers": job.get("NumberOfWorkers"), "GlueVersion": job["GlueVersion"]
        })

    purge_orphans(j_dir, seen_jobs)
    purge_orphans(sc_dir, seen_scripts)
    purge_orphans(lb_dir, seen_libs)

if __name__ == "__main__":
    export_stepfunctions()
    export_eventbridge()
    export_glue()