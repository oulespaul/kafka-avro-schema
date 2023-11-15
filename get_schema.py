from confluent_kafka.schema_registry import SchemaRegistryClient

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

# sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
# print(latest_version.schema.schema_str)