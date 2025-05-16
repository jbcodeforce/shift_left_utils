"""
Copyright 2024-2025 Confluent, Inc.
"""

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger
import json
from datetime import datetime, timedelta
import shift_left.core.statement_mgr as statement_mgr

def get_retention_size(table_name: str) -> int:
    """
    Get the retention size of a table using the REST API metrics endpoint.
    """
    logger.info(f"Getting retention size for table {table_name}")
    config = get_config()
    ccloud_client = ConfluentCloudClient(config)
    view="cloud"
    qtype="query"
    cluster_id = config["kafka"]["cluster_id"]
    now_minus_1_hour = datetime.now() - timedelta(hours=1)
    now= datetime.now()
    interval = f"{now_minus_1_hour.strftime('%Y-%m-%dT%H:%M:%S%z')}/{now.strftime('%Y-%m-%dT%H:%M:%S%z')}"
    q_retention = {"aggregations":[{"metric":"io.confluent.kafka.server/retained_bytes"}],
                       "filter": { "op": "AND", 
                                  "filters": [{"field":"resource.kafka.id","op":"EQ","value": cluster_id},
                                              {"field":"metric.topic","op":"EQ","value": table_name}]
                       },
                       "granularity":"PT1M",
                       "intervals":[interval],
                       "limit":100}
    metrics = ccloud_client.get_metrics(view, qtype, json.dumps(q_retention))
    logger.debug(f"metrics: {metrics}")
    sum= 0
    if metrics:
        for metric in metrics["data"]:
            sum += metric["value"]
        if len(metrics["data"]) > 0:
            return round(sum/len(metrics["data"]))
        else:
            return 0
    else:
        return 0


def get_total_amount_of_messages(table_name: str, compute_pool_id: str= None) -> int:
    """
    Get the total amount of messages in a table using a Flink statement to count the messages. This will be a COUNT(*) FROM <table_name>
    by getting result for a certain time, until the difference between result is below a threshold.
    """
    if not compute_pool_id:
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
    statement = f"SELECT COUNT(*) as nb_records FROM {table_name}"
    statement_name = f"cnt-rcds-{table_name.replace('_', '-')}"
    result = statement_mgr.post_flink_statement(compute_pool_id=compute_pool_id, statement_name=statement_name, sql_content=statement)
    if result:
        result = statement_mgr.get_statement_results(statement_name)
        print(f"result: {result}")
    return result


def get_pending_records(statement_name: str, compute_pool_id: str) -> int:
    config = get_config()
    ccloud_client = ConfluentCloudClient(config)
    view="cloud"
    qtype="query"
    now_minus_1_hour = datetime.now() - timedelta(hours=1)
    now= datetime.now()
    interval = f"{now_minus_1_hour.strftime('%Y-%m-%dT%H:%M:%S%z')}/{now.strftime('%Y-%m-%dT%H:%M:%S%z')}"
    query= {"aggregations":[{"metric":"io.confluent.flink/pending_records"}],
          "filter": {"op":"AND",
                     "filters":[{"field":"resource.compute_pool.id","op":"EQ","value": compute_pool_id},
                                {"field":"resource.flink_statement.name","op":"EQ","value": statement_name}
                                ]},
                    "granularity":"PT1M",
                    "intervals":[interval],
                    "limit":1000,
                    "group_by":["metric.table_name"],
                    "format":"GROUPED"}

    metrics = ccloud_client.get_metrics(view, qtype, json.dumps(query))
    sum= 0
    for metric in metrics["data"]:
        for point in metric["points"]:
            sum += point["value"]
        if len(metric["points"]) > 0:
            sum = round(sum/len(metric["points"]))
    return sum