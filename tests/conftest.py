from elasticsearch_sdk.elasticsearcher import ElasticSearcher, BadConnectionException
import pytest

@pytest.fixture()
def elk_controler():
    host = "ec2-3-8-16-183.eu-west-2.compute.amazonaws.com"
    port = 9200
    index = "filebeat-6.5.1-2018.11.29"
    elk = ElasticSearcher(host, port, index)
    return elk
