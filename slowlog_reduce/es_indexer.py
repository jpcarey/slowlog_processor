# -*- coding: utf-8 -*-

def index(data):
    """
    Index results to elasticsearch
    """

    from elasticsearch import Elasticsearch, helpers

    es = Elasticsearch(
        ['http://elastic:changeme@localhost:9200/'],
    )

    k = ({'_type': 'my_type', '_index': 'slowlog', '_source': item}
         for item in data.results)
    # print(list(k))

    # mapping = '''
    # {
    #   "mappings": {
    #     "my_type": {
    #       "properties": {
    #         "ip_addr": {
    #           "type": "ip"
    #         }
    #       }
    #     }
    #   }
    # }'''
    #
    # es.indices.create('test', ignore=400, body=mapping)

    helpers.bulk(es, k)
    print(es.count(index='slowlog'))
