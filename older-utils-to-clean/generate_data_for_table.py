"""
Create avro messages for a specific topic giving its topic_name.
Input: topic name, a config file to get the schema registry url and credentials, number of message to create
Output: folder to save the output, filename for the json records. One file can include n records

Example of usage:
python generate_data_for_table.py -f data.json -t portal_role_raw
"""
import os, argparse
import random
import coloredlogs, logging
import string
from datetime import datetime, timedelta
import json, avro.schema
from kafka.app_config import read_config

coloredlogs.install()
from kafka_avro_producer import load_schema_definitions_from_schema_registry
parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate records for testing given a schema subject in a schema registry'
)

parser.add_argument('-t', '--topic_name', required=True, help="name of the topic to write message to")
parser.add_argument('-o', '--output_file_name', required=True, help="name of the file to persist the test json messages")
parser.add_argument('-n', '--nb_records', required=False, help="number of records to produce in the file")


# Function to recursively extract field types
def get_field_types(schema : avro.schema.Schema):
    if schema is None:
        return ""
    if isinstance(schema, avro.schema.RecordSchema):
        field_types = {}
        for field in schema.fields:
            field_name = field.name
            field_type = get_field_types(field.type)
            field_types[field_name] = field_type
        return field_types
    elif isinstance(schema, avro.schema.PrimitiveSchema):
        return str(schema.type)
    elif isinstance(schema, avro.schema.UnionSchema):
        return [get_field_types(s) for s in schema.schemas]
    elif isinstance(schema, avro.schema.ArraySchema):
        return {"array": get_field_types(schema.items)}
    elif isinstance(schema, avro.schema.MapSchema):
        return {"map": get_field_types(schema.values)}
    else:
        return str(schema.type)


def generate_random_string(length=5):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))

def generate_random_boolean():
    """Generate a random boolean value."""
    return random.choice([True, False])

def generate_random_timestamp():
    """Generate a random timestamp in milliseconds since epoch."""
    start = datetime(2020, 1, 1)
    end = datetime.now()
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return int((start + timedelta(seconds=random_seconds)).timestamp() * 1000)

def generate_random_email():
    """Generate a random email address."""
    name = generate_random_string(8)
    domain = random.choice(['example.com', 'test.com', 'sample.org'])
    return f"{name}@{domain}"

def generate_random_phone_number():
    """Generate a random phone number."""
    return ''.join(random.choices(string.digits, k=10))

def generate_data_for_field(field_name: str, field_type: str, value_in_keys: dict)-> dict:
    """
    This method is specific to the type of data to create
    :param: value_in_keys is used to keep the same values of the key inside the same attribute in the values.
    """
    logging.debug(f"{field_name} --> {field_type}")
    field_data = {}
    match field_type:
        case "string":
            if "mail" in field_name:
                field_data[field_name] = generate_random_email()
            elif "phone" in field_name:
                field_data[field_name] = generate_random_phone_number()
            else:
                field_data[field_name] = generate_random_string()
        case "long":
            field_data[field_name] = generate_random_timestamp()
        case "boolean":
            field_data[field_name] = generate_random_boolean()
        case "int":
            field_data[field_name] = random.randint(1,12)
        case _:
            field_data[field_name] = "NULL"
    match field_name:
        case "__op":
            field_data[field_name] = "c"
        case "__db":
            field_data[field_name] = "db_01"
        case "year":
            field_data[field_name] = 2023
        case _:
            pass
    # keep the value in the key matching the same field name with the value in the key
    if value_in_keys:
        for key_def in value_in_keys:
            if field_name in key_def:
                field_data[field_name]=value_in_keys[key_def]
                break
    return field_data

def generate_one_record(key_definitions, field_definitions):
    record = {}
    record["key"] = {}
    record["value"] = {}
    for k in key_definitions:
        record["key"].update(generate_data_for_field(k, key_definitions[k], None))
    logging.warning(f"Key = {record["key"]}")
    for fd in field_definitions:
        the_field_type= field_definitions[fd]
        if isinstance(the_field_type, list):
            the_field_type=the_field_type[1]
        record["value"].update(generate_data_for_field(fd, the_field_type, record["key"]))
    logging.warning(record)
    return record


def create_json_data_file(key_sch, value_sch, nb_records: int, file_name: str = "out.json"):
    """
    Create the json file given its file name, with nb_records rows. Each row is in the format
    {"key": {}, "value": {}}
    Each element of the array is "field_name": "field_value"
    :param: key_schema the avro schema of the key
    :param: value_schema the avro schema for the values of the record
    """
    key_types=[]
    if key_sch:
        parsed_key_schema = avro.schema.parse(key_sch)
        key_types = get_field_types(parsed_key_schema)
    field_types = get_field_types(avro.schema.parse(value_sch))
    records = []
    for _ in range(nb_records):
        row_as_json=generate_one_record(key_types, field_types)
        records.append(row_as_json)
    with open(file_name, "w") as f:
        json.dump(records, f)
        

if __name__ == "__main__":
    """
    Load the schemas for the key and the value from the schema registry
    and create afile with n records  with test data matchin the schema structure and field types
    """
    args = parser.parse_args()
    config=read_config()
    key_sch, value_sch=load_schema_definitions_from_schema_registry(config, args.topic_name)
    nb_records=1
    if args.nb_records:
        nb_records = int(args.nb_records)
    create_json_data_file(key_sch, value_sch, nb_records, args.output_file_name)
    
