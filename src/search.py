# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 12:09:26 2020

@author: JurajBedej
"""

from elasticsearch import Elasticsearch 

es=Elasticsearch([{'host':'localhost','port':9200}])

while True:
    inp = input()
    if inp == ':exit':
       break
   
    res = es.search(index='final', doc_type='test', size=100, body = {
        "query": {
            "multi_match": {
                "query": inp,
                "fields": ["title^3", "keywords"],
            }
        }    
        
    })
    for i in res["hits"]["hits"]:
        print("Title: " + i["_source"]["title"])
        print("Wikipedia abstract: " + i["_source"]["wiki-abstract"])
        print("My abstract: " + i["_source"]["my-abstract"])
        print("Similarity: " + str(i["_source"]["similarity"]))
        print("Keywords: " + str(i["_source"]["keywords"]))
        print("\n\n")
        