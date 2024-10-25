from airflow.models import Variable
import requests

def notify_failure(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    execution_date = context['ts']

    # Payload structure for Teams notification
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "title": "Airflow Task Failure Alert",
        "summary": f"Task '{task_id}' in DAG '{dag_id}' failed.",
        "themeColor": "FF0000",  # Red to indicate failure
        "sections": [
            {
                "activityTitle": f"❌ Task '{task_id}' failed in DAG '{dag_id}'",
                "activitySubtitle": f"Execution Date: {execution_date}",
                "facts": [
                    {
                        "name": "DAG ID",
                        "value": dag_id
                    },
                    {
                        "name": "Task ID",
                        "value": task_id
                    },
                    {
                        "name": "Execution Date",
                        "value": execution_date
                    },
                    {
                        "name": "Log URL",
                        "value": log_url
                    }
                ]
            }
        ],
        "potentialAction": [{
            "@type": "OpenUri",
            "name": "View Task Logs",
            "targets": [
                {
                    "os": "default",
                    "uri": log_url
                }
            ]
        }]
    }

    headers = {"Content-Type": "application/json"}

    # Send notification to the Teams webhook
    webhook_url = Variable.get('teams_webhook_secret')
    response = requests.post(webhook_url, json=payload, headers=headers)
    
    # Log the result of the notification attempt
    if response.status_code == 200:
        print(f"Teams notification sent successfully for task '{task_id}'.")
    else:
        print(f"Failed to send Teams notification for task '{task_id}'. Status code: {response.status_code}, Response: {response.text}")

def notify_success(context):
    dag_id = context['dag'].dag_id
    execution_date = context['ts']
    dag_run = context['dag_run']

    # Extract start and end times
    start_time = dag_run.start_date
    end_time = dag_run.end_date
    duration = end_time - start_time if start_time and end_time else "N/A"

    # Format times for readability
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "N/A"
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S") if end_time else "N/A"
    duration_str = str(duration) if duration != "N/A" else "N/A"

    # Payload structure for Teams notification
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "title": "Airflow Pipeline Successful",
        "summary": f"DAG '{dag_id}' completed successfully.",
        "themeColor": "00FF00",  # Green to indicate success
        "sections": [
            {
                "activityTitle": f"✅ DAG '{dag_id}' completed successfully",
                "activitySubtitle": f"Execution Date: {execution_date}",
                "facts": [
                    {"name": "DAG ID", "value": dag_id},
                    {"name": "Start Time", "value": start_time_str},
                    {"name": "End Time", "value": end_time_str},
                    {"name": "Duration", "value": duration_str}
                ]
            }
        ]
    }

    headers = {"Content-Type": "application/json"}

    # Send notification to the Teams webhook
    webhook_url = Variable.get('teams_webhook_secret')
    response = requests.post(webhook_url, json=payload, headers=headers)

    # Log the result of the notification attempt
    if response.status_code == 200:
        print(f"Teams success notification sent successfully for DAG '{dag_id}'.")
    else:
        print(f"Failed to send Teams success notification for DAG '{dag_id}'. Status code: {response.status_code}, Response: {response.text}")