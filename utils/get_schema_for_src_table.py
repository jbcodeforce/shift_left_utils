"""
Get the SQL schema given a topic name
"""

import os
import http.client
TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')
BEARER_TOKEN=os.getenv("API_SECRET")







def find_sub_string(table_name, topic_name) -> bool:
    words=table_name.split("_")
    subparts=topic_name.split(".")
    all_present = True
    for w in words:
        if w not in subparts:
            all_present=False
            break
    return all_present

def search_matching_topic(table_name: str) -> str:
    with open(TOPIC_LIST_FILE,"r") as f:
        for line in f:
            if table_name == line:
                return line
            elif table_name in line:
                return line
            elif find_sub_string(table_name,line):
                return line
    return table_name

def get_schema_using_cc_api(topic_name: str, organization_id: str, environment_id: str) -> str:
    conn = http.client.HTTPSConnection("flink.region.provider.confluent.cloud")
    headers = {
        'content-type': "application/json",
        'Authorization': f"Basic {BEARER_TOKEN}"
    }
    payload = "{\"name\":\"sql123\",\"organization_id\":\"7c60d51f-b44e-4682-87d6-449835ea4de6\",\"environment_id\":\"string\",\"spec\":{\"statement\":\"SHOW CREATE TABLE WHERE ;\",\"properties\":{\"sql.current-catalog\":\"my_environment\",\"sql.current-database\":\"my_kafka_cluster\"},\"compute_pool_id\":\"fcp-00000\",\"principal\":\"sa-abc123\",\"stopped\":false},\"result\":{\"api_version\":\"sql/v1\",\"kind\":\"StatementResult\",\"metadata\":{\"self\":\"https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-123/statements\",\"next\":\"https://flink.us-west1.aws.confluent.cloud/sql/v1/environments/env-abc123/statements?page_token=UvmDWOB1iwfAIBPj6EYb\"},\"results\":{\"data\":[{\"op\":0,\"row\":[\"101\",\"Jay\",[null,\"abc\"],[null,\"456\"],\"1990-01-12 12:00.12\",[[null,\"Alice\"],[\"42\",\"Bob\"]]]}]}}}"
    conn.request("POST", f"/sql/v1/organizations/{organization_id}/environments/{environment_id}/statements", payload, headers)
    res = conn.getresponse()
    data = res.read()
    schema=data
    return schema

if __name__ == "__main__":
    print(search_matching_topic("production_record_status"))
    print(search_matching_topic("tdc_doc_document_type"))
