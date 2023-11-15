from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

from stream_schema import SCHEMA_STR

kafka_topic = 'ais-lab-3'
schema_registry_subject = f"{kafka_topic}-value"

username="DM2QQACUP5UFSWQ7"
password="cpnHbtcizpWSZGPOQgg0kDTnE9XuoryLFUY0ciAEpMPsanwtGhO5zYCTVPPrRFwW"
schema_registry_url = f"https://{username}:{password}@psrc-e8vk0.southeastasia.azure.confluent.cloud"

def register_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)

    return schema_id

# def update_schema(schema_registry_url, schema_registry_subject, schema_str):
#     sr = SchemaRegistryClient({'url': schema_registry_url})
#     versions_deleted_list = sr.delete_subject(schema_registry_subject)
#     print(f"versions of schema deleted list: {versions_deleted_list}")

#     schema_id = register_schema(schema_registry_url, schema_registry_subject, schema_str)
#     return schema_id

schema_id = register_schema(schema_registry_url, schema_registry_subject, SCHEMA_STR)
print(schema_id)