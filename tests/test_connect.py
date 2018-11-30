from elasticsearch_sdk.elasticsearcher import ElasticSearcher, BadConnectionException
import pytest

port = 9200
index = "filebeat-6.5.1-2018.11.29"

def test_connection(elk_host):
    elk = ElasticSearcher(elk_host, port, index)

def test_connection_bad_host():
    with pytest.raises(BadConnectionException):
        bad_host = "google.com"
        elk = ElasticSearcher(bad_host, port, index)


def test_connection_bad_port(elk_host):
    with pytest.raises(BadConnectionException):
        bad_port = 9222
        elk = ElasticSearcher(elk_host, bad_port, index)


def test_change_index(elk_host):
    elk = ElasticSearcher(elk_host, port, index)
    assert elk._INDEX == index
    index_new = "PERRO"
    elk.change_index(index_new)
    assert index_new == elk._INDEX


