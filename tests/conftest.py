from elasticsearch_sdk.elasticsearcher import ElasticSearcher, BadConnectionException
import pytest

@pytest.fixture()
def elk_host():
    return "ec2-35-178-182-30.eu-west-2.compute.amazonaws.com"

@pytest.fixture()
def elk_controler(elk_host):
    port = 9200
    index = "filebeat-*"
    elk = ElasticSearcher(elk_host, port, index)
    return elk
