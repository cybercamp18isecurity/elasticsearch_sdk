#!/usr/bin/env python3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.exceptions import NotFoundError
import json
from datetime import datetime

class BadConnectionException(Exception):
    """ Bad connection Exception """

class ElasticSearcher(object):
    def __init__(self, host, port, index):
        self._HOST = host
        self._PORT = port
        self._INDEX = index

        # Inicializa las clases auxiliares
        self.elasticsearch = Elasticsearch(
            hosts=[{'host': self._HOST, 'port': int(self._PORT)}],
            connection_class=RequestsHttpConnection
        )
        if self.elasticsearch.ping() == False:
            raise BadConnectionException()

    def change_index(self, index):
        self._INDEX = index

    def get_index(self, index):
        if index != None:
            index = index
        else:
            index = self._INDEX
        return index

    def get_from_elasticsearch(self, id, doc_type="doc", routing=None, ignore=[], index=None):
        data = self.elasticsearch.get(
            index=self.get_index(index), doc_type=doc_type, id=id, routing=routing, ignore=ignore)
        try:
            if data['found'] == False:
                return None
        except KeyError:
            raise Exception(data)
        return data

    def make_query(self, query, doc_type="doc", filter_path=[], ignore=[], index=None):
        response = self.elasticsearch.search(
            index=self.get_index(index), doc_type=doc_type, body=query, filter_path=filter_path, ignore=[])
        return response['hits']

    def get_list_of_unique_elements(self, field, doc_type="doc", order="asc", id=None, index=None):
        query = {
            "size": 0,
            "aggs": {
                "unique_elements": {
                    "terms": {
                        "field": field,
                        "order": {"_term": order}
                    }
                }
            }
        }
        if id:
            query["query"] = {"ids": {"values": [id]}}

        query = json.dumps(query)
        response = self.elasticsearch.search(
            index=self.get_index(index), doc_type=doc_type, body=query)

        results = []
        for bucket in response['aggregations']['unique_elements']['buckets']:
            results.append(bucket.get('key', ''))

        return results

    def count_results_of_query(self, query, doc_type="doc", filter_path=[], index=None):
        response = self.elasticsearch.count(
            index=self.get_index(index), doc_type=doc_type, body=query, filter_path=filter_path)
        return response

    def create_document(self, id, data, doc_type="doc", parent_id=None, index=None):
        query = json.dumps(data)
        response = self.elasticsearch.index(
            index=self.get_index(index), op_type='index', doc_type=doc_type, id=id, body=query, parent=parent_id)
        return response['_id']

    def update_document(self, id, data, doc_type="doc", parent_id=None, index=None):
        query = json.dumps({"doc": data})
        response = self.elasticsearch.update(
            index=self.get_index(index), doc_type=doc_type, id=id, body=query, parent=parent_id)
        return response['_id']

    @staticmethod
    def parseEpochToDatetime(epoch_mills, format="%Y-%m-%d %H:%M:%S"):
        if not epoch_mills:
            return ''
        timestamp = epoch_mills / 1000.0
        return datetime.fromtimestamp(timestamp).strftime(format)

    @staticmethod
    def parseDatetimeToEpoch(date, format="%Y-%m-%d %H:%M:%S.%f"):
        if not date:
            return date
        return int(datetime.strptime(date, format).timestamp() * 1000)
