import pytest
from elasticsearch_sdk.elasticsearcher import ElasticSearcher, NotFoundError
import time

@pytest.fixture()
def elk_controler_test():
    host = "ec2-3-8-16-183.eu-west-2.compute.amazonaws.com"
    port = 9200
    index = "test-pytest"
    elk = ElasticSearcher(host, port, index)
    return elk

id_test = time.time()

def test_create_element(elk_controler_test):
    # Crea un documento
    id = int(time.time())
    data = {"perro":"gato"}
    res = elk_controler_test.create_document(id, data, doc_type="doc")
    assert int(res) == id

def test_create_element_preexist(elk_controler_test):
    id = int(time.time())
    data = {"perro":"gato"}
    res = elk_controler_test.create_document(id, data, doc_type="doc")
    assert int(res) == id
    res = elk_controler_test.create_document(id, data, doc_type="doc")
    assert int(res) == id

def test_update_element(elk_controler_test):
    id = int(time.time())
    data = {"perro":"gato"}
    res = elk_controler_test.create_document(id, data, doc_type="doc")
    assert int(res) == id

    data = elk_controler_test.get_from_elasticsearch(res, doc_type="doc")
    assert data['_id'] == str(id)
    assert data['_source']['perro'] == 'gato'

    data = {"perro":"doberman"}
    res = elk_controler_test.update_document(id, data, doc_type="doc")
    assert int(res) == id

    data = elk_controler_test.get_from_elasticsearch(res, doc_type="doc")
    assert data['_id'] == str(id)
    assert data['_source']['perro'] == 'doberman'

def test_partial_update(elk_controler_test):
    id = int(time.time())
    data = {"perro":"gato", "animal": "perro"}
    res = elk_controler_test.create_document(id, data, doc_type="doc")
    assert int(res) == id

    data = {"perro": "doberman"}
    res = elk_controler_test.update_document(id, data, doc_type="doc")
    assert int(res) == id

    data = elk_controler_test.get_from_elasticsearch(res, doc_type="doc")
    assert data['_id'] == str(id)
    assert data['_source'] == {"perro": "doberman", "animal":"perro"}

def test_update_element_no_exist(elk_controler_test):
    with pytest.raises(NotFoundError):
        id = int(time.time())+1000000000
        data = {"perro": "doberman"}
        res = elk_controler_test.update_document(id, data, doc_type="doc")
        assert int(res) == id
