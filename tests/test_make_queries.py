import pytest
from elasticsearch_sdk.elasticsearcher import ElasticSearcher

def test_get_data(elk_controler):
    data = elk_controler.get_from_elasticsearch("7ybbYWcBXFg0BIzEOjZC", doc_type="doc")
    assert type(data) == dict
    assert len(data) > 0

def test_make_query(elk_controler):

    hostname = "WIN-BPGE5FV4KJ1"
    query = {
        "query": {"term": {"beat.name": hostname}}
    }

    data = elk_controler.make_query(query, doc_type="doc")
    assert len(data) > 0
    assert data['total'] > 0
    assert len(data['hits']) > 0

def test_make_query_not_found_data(elk_controler):

    hostname = "perro_matico"
    query = {
        "query": {"term": {"beat.name": hostname}}
    }

    data = elk_controler.make_query(query, doc_type="doc")
    assert data['total'] == 0
    assert len(data['hits']) == 0


def test_get_hosts(elk_controler):

    hosts = elk_controler.get_list_of_unique_elements("beat.name", "doc")
    assert len(hosts) >= 4
    assert "WIN-BPGE5FV4KJ1" in hosts
