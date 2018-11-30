from elasticsearch_sdk.elasticsearcher import ElasticSearcher, BadConnectionException
import pytest

host = "ec2-3-8-16-183.eu-west-2.compute.amazonaws.com"
port = 9200
index = "filebeat-6.5.1-2018.11.29"

def test_connection():
    elk = ElasticSearcher(host, port, index)

def test_connection_bad_host():
    with pytest.raises(BadConnectionException):
        bad_host = "google.com"
        elk = ElasticSearcher(bad_host, port, index)

def test_connection_bad_port():
    with pytest.raises(BadConnectionException):
        bad_port = 9222
        elk = ElasticSearcher(host, bad_port, index)



