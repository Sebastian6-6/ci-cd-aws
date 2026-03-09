import boto3
import json
import os
from botocore.exceptions import ClientError

try:
    session = boto3.Session(profile_name="pessoal", region_name="us-east-2")
except:
    session = boto3.Session(region_name="us-east-2")

eb_client = session.client("events")
scheduler_client = session.client("scheduler")

def save_if_changed(path, new_content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    content_to_save = json.dumps(new_content, indent=2, ensure_ascii=False, sort_keys=True)

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            if f.read() == content_to_save:
                return False

    with open(path, "w", encoding="utf-8") as f:
        f.write(content_to_save)
    print(f"[+] EB ATUALIZADO: {os.path.basename(path)}")
    return True

def purge_orphans(directory, expected_files):
    if not os.path.exists(directory): return
    for filename in os.listdir(directory):
        if filename not in expected_files:
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"[🔥 PURGE] Removido arquivo órfão: {filename}")

def export_eventbridge():
    r_dir, s_dir = "./eventbridge/regras", "./eventbridge/cronogramas"
    seen_rules, seen_scheds = [], []
    
    paginator = eb_client.get_paginator("list_rules")
    for page in paginator.paginate(EventBusName="default"):
        for rule in page.get("Rules", []):
            if rule.get("ManagedBy"): continue
            
            name = f"{rule['Name']}.json"
            seen_rules.append(name)
            desc = eb_client.describe_rule(Name=rule['Name'])
            targets_raw = eb_client.list_targets_by_rule(Rule=rule['Name']).get("Targets", [])
            
            clean_targets = [
                {
                    "Id": t.get("Id"),
                    "Arn": t.get("Arn"),
                    "RoleArn": t.get("RoleArn"),
                    "Input": t.get("Input"),
                    "InputPath": t.get("InputPath"),
                    "RetryPolicy": t.get("RetryPolicy"),
                    "DeadLetterConfig": t.get("DeadLetterConfig")
                }
                for t in targets_raw
            ]

            save_if_changed(os.path.join(r_dir, name), {
                "Rule": {
                    "Name": rule.get('Name'),
                    "EventPattern": json.loads(desc.get("EventPattern", "{}")),
                    "State": desc.get("State"),
                    "Description": desc.get("Description")
                },
                "Targets": clean_targets
            })
    purge_orphans(r_dir, seen_rules)

    try:
        groups = scheduler_client.list_schedule_groups()["ScheduleGroups"]
        for group in groups:
            paginator = scheduler_client.get_paginator("list_schedules")
            for page in paginator.paginate(GroupName=group["Name"]):
                for s in page.get("Schedules", []):
                    name = f"{s['Name']}.json"
                    seen_scheds.append(name)
                    det = scheduler_client.get_schedule(Name=s['Name'], GroupName=group["Name"])

                    save_if_changed(os.path.join(s_dir, name), {
                        "Name": s.get('Name'),
                        "GroupName": group.get("Name"),
                        "State": det.get("State"),
                        "ScheduleExpression": det.get("ScheduleExpression"),
                        "ScheduleExpressionTimezone": det.get("ScheduleExpressionTimezone"),
                        "FlexibleTimeWindow": det.get("FlexibleTimeWindow"),
                        "Target": {
                            "Arn": det["Target"].get("Arn"),
                            "RoleArn": det["Target"].get("RoleArn"),
                            "Input": det["Target"].get("Input"),
                            "RetryPolicy": det["Target"].get("RetryPolicy"),
                            "DeadLetterConfig": det["Target"].get("DeadLetterConfig")
                        },
                        "ActionAfterCompletion": det.get("ActionAfterCompletion")
                    })
    except ClientError as e:
        print(f"Erro no Scheduler: {e}")
    
    purge_orphans(s_dir, seen_scheds)

if __name__ == "__main__":
    export_eventbridge()