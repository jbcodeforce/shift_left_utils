"""
Copyright 2024-2025 Confluent, Inc.

Create a compute pool, generate 100k messages to input topic,
run a flink statement that read and count the number of records without timing
pull to get the lag metrics
"""

import pytest
import shift_left.core.compute_pool_mgr as compute_pool_mgr
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.metric_mgr as metric_mgr
import time
from shift_left.core.models.flink_statement_model import StatementResult, Statement

def __create_table(compute_pool_id: str, table_name: str, definition: str) -> Statement:
   
    statement= statement_mgr.post_flink_statement(statement_name=f"st-{table_name.replace('_', '-')}", 
                                       sql_content=definition, 
                                       compute_pool_id=compute_pool_id)
    count = 0
    while statement and statement.status.phase != "COMPLETED" and count < 10:
        time.sleep(1)
        statement = statement_mgr.get_statement(statement.name)
        count += 1
    return statement

def _create_compute_pool(table_name: str):
    print("Create a compute pool")
    start_time = time.time()
    compute_pool_id, compute_pool_name = compute_pool_mgr.create_compute_pool(table_name)
    print(f"Compute pool created: {compute_pool_id} {compute_pool_name}  in {time.time() - start_time} seconds")
    return compute_pool_id, compute_pool_name

def _create_input_table(compute_pool_id: str, input_table_name: str):
    print("#"*30+ "\nCreate input table")
    start_time = time.time()
    sql_statement = f"CREATE TABLE IF NOT EXISTS {input_table_name}"
    sql_statement += """(id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED) DISTRIBUTED BY HASH(id) INTO 2 BUCKETS WITH (
    'kafka.producer.compression.type'='snappy',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'kafka.retention.time' = '0',
    'changelog.mode' = 'append',
    'kafka.cleanup-policy'= 'compact',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry');
    """
    statement = __create_table(compute_pool_id, input_table_name, sql_statement)
    print(statement.model_dump_json(indent=2))
    print(f"Input table created: {statement.name} in {time.time() - start_time} seconds")

def _create_output_table(compute_pool_id: str, output_table_name: str):
    print("#"*30+ "\nCreate output table")
    start_time = time.time()
    sql_statement = f"CREATE TABLE IF NOT EXISTS {output_table_name}"
    sql_statement += """(id int, total_count BIGINT, primary key(id) not enforced) DISTRIBUTED BY HASH(id) INTO 2 BUCKETS WITH (
    'kafka.producer.compression.type'='snappy',
    'key.avro-registry.schema-context' = '.flink-dev',      
    'value.avro-registry.schema-context' = '.flink-dev',
    'kafka.retention.time' = '0',
    'changelog.mode' = 'retract',
    'kafka.cleanup-policy'= 'compact',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',    
    'value.fields-include' = 'all',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry')
    """
    statement = __create_table(compute_pool_id, output_table_name, sql_statement)
    print(statement.model_dump_json(indent=2))
    print(f"Output table created: {statement.name} in {time.time() - start_time} seconds")  

def test_metrics_scenario():
    
    input_table_name = "in_topic_lag_metrics"
    output_table_name = "out_topic_lag_metrics"
    #compute_pool_id, compute_pool_name = _create_compute_pool(output_table_name)
    compute_pool_id = "lfcp-79xyyo"
    compute_pool_name = "stage-out-topic-lag-metrics"
    #_create_input_table(compute_pool_id, input_table_name)

    #_create_output_table(compute_pool_id, output_table_name)
    
    print("Generate 100k messages to input topic")
    nb_of_times = 100
    import threading
    thread_input = threading.Thread(target=__generate_messages, args=(input_table_name, 1000, compute_pool_id, nb_of_times))
    thread_output = threading.Thread(target=_run_flink_statement, args=(compute_pool_id, input_table_name, output_table_name, nb_of_times))
    thread_output.start()
    thread_input.start()

    thread_input.join()
    thread_output.join()



def test_clean_up():
    print("Clean up")
    input_table_name = "in_topic_lag_metrics"
    output_table_name = "out_topic_lag_metrics"
    compute_pool_id = "lfcp-79xyyo"
    compute_pool_name = "stage-out-topic-lag-metrics"
    
    statement_mgr.delete_statement_if_exists(f"st-{input_table_name.replace('_', '-')}")
    statement_mgr.delete_statement_if_exists(f"st-{output_table_name.replace('_', '-')}")
    #statement_mgr.drop_table(input_table_name, compute_pool_id)
    statement_mgr.drop_table(output_table_name, compute_pool_id)
    statement_mgr.delete_statement_if_exists("insert-input-table")
    statement_mgr.delete_statement_if_exists("run-flink-statement")
    #compute_pool_mgr.delete_compute_pool(compute_pool_id)
    

def __generate_messages(input_table_name: str, num_messages: int, compute_pool_id: str, nb_of_times: int):
    print(f"Generate {num_messages} messages {nb_of_times} times")
    for t in range(nb_of_times):
        statement_name = f"insert-input-table-{t}"
       
        statement_mgr.delete_statement_if_exists(statement_name)
        query = f"INSERT INTO {input_table_name} (id,name) VALUES"
        for i in range(num_messages):
            message = f"({i}, 'name_{i}'),"
            query += message
        query = query[:-1]+";"
        statement = statement_mgr.post_flink_statement(statement_name=statement_name, 
                                            sql_content=query, 
                                            compute_pool_id=compute_pool_id)
        print(f"Insert {num_messages} messages {t+1} times")


def _run_flink_statement(compute_pool_id: str, input_table_name: str, output_table_name: str, nb_of_times: int):
    sql_statement = f"insert into {output_table_name} SELECT id, COUNT(*) as total_count FROM {input_table_name} GROUP BY id;" 
    statement_name = f"run-flink-statement"
    #statement_name = "stage-aqem-dml-int-aqem-latest-element-revision"
    print(f"Run flink statement {statement_name}")
    statement_mgr.delete_statement_if_exists(statement_name)
    
    statement = statement_mgr.post_flink_statement(statement_name=statement_name, 
                                           sql_content=sql_statement, 
                                           compute_pool_id=compute_pool_id)
    count = 0
    print(statement.model_dump_json(indent=2))

    print("Pull to get the lag metrics")
    while statement and statement.status.phase == "RUNNING" and count < nb_of_times:

        pending_records = metric_mgr.get_pending_records(statement_name, compute_pool_id)
        print(f"Pending records: {pending_records}")
        output_records = metric_mgr.get_output_records(statement_name, compute_pool_id)
        print(f"Output records: {output_records}")
        statement_result = statement_mgr.get_statement_results(statement.name)
        if statement_result and isinstance(statement_result, StatementResult):
            if statement_result.results and statement_result.results.data:
                result_str = str(statement_result.results.data[0].row[0])
                print(f"Result: {result_str}")
            else:
                print("No result")
        else:
            print("No statement result")
        count += 1

    
