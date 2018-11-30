import pytest
from elasticsearch_sdk.elasticsearcher import ElasticSearcher, NotFoundError

def test_get_data_bad_index(elk_controler):
    with pytest.raises(NotFoundError):
        data = elk_controler.get_from_elasticsearch("7ybbYWcBXFg0BIzEOjZC")

def test_get_data(elk_controler):
    good_index = "filebeat-6.5.1-2018.11.29"
    data = elk_controler.get_from_elasticsearch("7ybbYWcBXFg0BIzEOjZC", index=good_index)
    assert type(data) == dict
    assert len(data) > 0

def test_make_query(elk_controler):
    hostname = "WIN-BPGE5FV4KJ1"
    query = {
        "query": {"term": {"beat.name": hostname}}
    }

    data = elk_controler.make_query(query)
    assert len(data) > 0
    assert data['total'] > 0
    assert len(data['hits']) > 0

def test_make_query_not_found_data(elk_controler):

    hostname = "perro_matico"
    query = {
        "query": {"term": {"beat.name": hostname}}
    }

    data = elk_controler.make_query(query)
    assert data['total'] == 0
    assert len(data['hits']) == 0


def test_get_hosts(elk_controler):

    hosts = elk_controler.get_list_of_unique_elements("beat.name")
    assert len(hosts) >= 4
    assert "WIN-BPGE5FV4KJ1" in hosts
