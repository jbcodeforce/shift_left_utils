"""
Copyright 2024-2025 Confluent, Inc.
"""

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger
import json
from datetime import datetime, timedelta, timezone
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.models.flink_statement_model import StatementResult
import pytz

def get_available_metrics(compute_pool_id: str) -> list:
    """
    Get the available metrics for a compute pool.
    """
    config = get_config()
    ccloud_client = ConfluentCloudClient(config)
    dataset="cloud"

    url=f"https://api.telemetry.confluent.cloud/v2/metrics/{dataset}/descriptors/metrics"
    response = None
    try:
        response  = ccloud_client.make_request("GET", url)
        return response
    except Exception as e:
        logger.error(f"Error executing rest call: {e}")
        raise Exception(f"Error executing rest call: {e}")


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
    now_minus_1_hour = datetime.now(timezone.utc) - timedelta(hours=1)
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


def get_total_amount_of_messages(table_name: str, compute_pool_id: str= None, from_date: str = None) -> int:
    """
    Get the total amount of messages in a table using a Flink statement to count the messages. This will be a COUNT(*) FROM <table_name>
    by getting result for a certain time, until the difference between result is below a threshold.
    """
    if not compute_pool_id:
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
    result = 0
    statement = f"SELECT COUNT(*) as nb_records FROM {table_name}"
    statement_name = f"cnt-rcds-{table_name.replace('_', '-')}"
    statement_mgr.delete_statement_if_exists(statement_name)
    statement = statement_mgr.post_flink_statement(compute_pool_id=compute_pool_id, statement_name=statement_name, sql_content=statement)
    if statement and statement.status.phase == "RUNNING":
        statement_result = statement_mgr.get_statement_results(statement_name)
        if statement_result and isinstance(statement_result, StatementResult):
            result = _process_results(statement_result, result)
            while statement_result.metadata.next:
                statement_result = statement_mgr.get_next_statement_results(statement_result.metadata.next)
                result = _process_results(statement_result, result)
    statement_mgr.delete_statement_if_exists(statement_name)
    return result

def _process_results(statement_result: StatementResult, result: int) -> int:
    previous_result = result
    if statement_result.results and statement_result.results.data:
        for op_row in statement_result.results.data:
            if op_row.op == 0 or op_row.op == 2:
                result += int(op_row.row[0])
                previous_result = result
            elif op_row.op == 1 or op_row.op == 3:
                result -= int(op_row.row[0])
    if previous_result > result:
        result = previous_result
    return result

def get_pending_records(compute_pool_ids: list[str], from_date: str) -> dict[str,int]:
    """
    Get the pending records for a statement using the REST API metrics endpoint.
    Metric data points are typically available for query in the API within 5 minutes of their origination at the source.
    """
    return _get_int_metric(compute_pool_ids, "io.confluent.flink/pending_records", from_date)


def get_num_records_out(compute_pool_ids: list[str], from_date: str) -> dict[str,int]:
    return _get_int_metric(compute_pool_ids, "io.confluent.flink/num_records_out", from_date)

def get_num_records_in(compute_pool_ids: list[str], from_date: str) -> dict[str,int]:
    return _get_int_metric(compute_pool_ids, "io.confluent.flink/num_records_in", from_date)

def _get_int_metric(compute_pool_ids: list[str], metric_name: str, from_date: str) -> dict[str,int]:
    config = get_config()
    ccloud_client = ConfluentCloudClient(config)
    dataset="cloud"
    qtype="query"

    if from_date:
        # Parse input date and localize to configured timezone
        from_date_local = pytz.timezone(config['app']['timezone']).localize(datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%S"))
        # Convert to UTC-1
        now = from_date_local.astimezone(pytz.UTC)
    else:
        now= datetime.now(pytz.UTC)
    now_minus_60_minutes = now - timedelta(minutes=60)
    interval = f"{now_minus_60_minutes.strftime('%Y-%m-%dT%H:%M:%S')}/{now.strftime('%Y-%m-%dT%H:%M:%S')}"
    filters = []
    if compute_pool_ids and len(compute_pool_ids) > 0:
        for cpoolid in compute_pool_ids:
            filters.append({"field":"resource.compute_pool.id","op":"EQ","value": cpoolid})
        group_by = ["resource.compute_pool.id","resource.flink_statement.name"]
    else:
        raise Exception("No compute pool ids provided")
    query= {"aggregations":[
            {"metric": metric_name}
        ],
          "filter": {"op":"OR",
                     "filters": filters},
                    "granularity":"PT1M",
                    "format": "GROUPED",
                    "group_by": group_by,
                    "intervals":[interval],
                    "limit":1000}
    try:
        logger.info(f"query: {json.dumps(query)}")
        metrics = ccloud_client.get_metrics(dataset, qtype, json.dumps(query))
        logger.debug(f"-> metrics: {metrics}")
        results = {}
        for metric in metrics.get("data", []):
            # Changing the variable name from sum to metrics_sum
            metrics_sum= 0
            if "points" in metric:
                for point in metric.get("points", []):
                    metrics_sum += point["value"]
            else:
                metrics_sum += metric.get("value", 0)
            if metric.get("resource.flink_statement.name"):
                results[metric.get("resource.flink_statement.name")] = int(metrics_sum)
        return results
    except Exception as e:
        logger.error(f"Error getting {metric_name}: {e}")
        return {}
