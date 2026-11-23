import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from routewatch.config import load_kafka_settings

pytestmark = pytest.mark.integration


def test_raw_topic_has_expected_topology_and_retention() -> None:
    settings = load_kafka_settings()
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})

    metadata = admin.list_topics(timeout=10)

    assert settings.topic in metadata.topics, f"Missing topic: {settings.topic}"

    topic = metadata.topics[settings.topic]
    assert topic.error is None, topic.error
    assert set(topic.partitions) == {0}

    partition = topic.partitions[0]
    assert partition.error is None, partition.error
    assert partition.leader >= 0
    assert partition.replicas == [partition.leader]
    assert partition.isrs == [partition.leader]

    resource = ConfigResource(ResourceType.TOPIC, settings.topic)
    futures = admin.describe_configs(
        [resource],
        request_timeout=10,
    )
    configuration = futures[resource].result(timeout=15)

    expected = {
        "cleanup.policy": "delete",
        "retention.ms": "172800000",
        "retention.bytes": "2147483648",
        "segment.bytes": "67108864",
        "segment.ms": "3600000",
        "max.message.bytes": "8388608",
    }

    for name, value in expected.items():
        assert configuration[name].value == value, name
