#Author vikas saini
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

query = {
    "size": 0,
    "query": {
      "match_all": {}
    },
    "aggs": {
      "name": {
        "terms": {
          "field": "name.keyword"
        },
    "aggs": {
      "city": {
        "terms": {
          "field": "billing_address_city.keyword"
        },
        "aggs": {
          "postal": {
            "terms": {
              "field": "billing_address_postalcode.keyword"
            },
            "aggs": {
              "country": {
                "terms": {
                  "field": "billing_address_country.keyword"
                },
                "aggs": {
                  "id": {
                    "terms": {
                      "field": "_id",
                      "order" : { "_key" : "asc" }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
 }
}

res = es.search(index="accounts", body=query)
facets = res['aggregations']
for name in facets['name']['buckets']:
    for city in name['city']['buckets']:
      for postal in city['postal']['buckets']:
        docs = []
        for country in postal['country']['buckets']:
            if len(country['id']['buckets']) > 0 :
                    docid = country['id']['buckets'][0]['key']
                    print("doc id is %s" % docid)
                    res = es.get(index="accounts", doc_type='account', id=docid)
                    doc = {"_index":"accounts_new","_type":"account_new","_id":docid,"_source":res["_source"]}
                    docs.append(doc)
                    if len(docs) >= 1000:
                      helpers.bulk(es,docs)
        helpers.bulk(es,docs)
