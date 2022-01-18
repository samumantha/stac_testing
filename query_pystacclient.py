"""
import requests
headers = {
  'Accept': 'application/geo+json'
}


r = requests.get('https://a3s.fi/2001659_stactest/stacs_eo_7/collection.json', params={}, headers = headers)
#r = requests.get('https://pta.data.lit.fmi.fi/stac/root.json', params={}, headers = headers)
#r = requests.get('https://earth-search.aws.element84.com/v0/search', params={}, headers = headers)

print(r.json())


###

from pystac_client import Client

#URL = 'https://planetarycomputer.microsoft.com/api/stac/v1'
#URL = 'https://a3s.fi/2001659_stactest/stacs_eo_7/collection.json'
URL = './stac_catalog/catalog.json'

# custom headers
headers = []

cat = Client.open(URL, headers=headers)
print(cat)

"""

from pystac_client import Client
catalog = Client.open('./stac_catalog/catalog.json')
mysearch = catalog.search(collections=['Sentinel-2'],bbox=[65.5,25,66], max_items=10)
#search needs some item search implemented which is not provided by pystac afaiu (https://github.com/stac-utils/pystac-client/blob/main/pystac_client/client.py)
# mysearch = catalog.search(collections=['sentinel-s2-l2a-cogs'], bbox=[-72.5,40.5,-72,41], max_items=10)
print(f"{mysearch.matched()} items found")

for item in mysearch.get_items():
    print(item.id)

#items = mysearch.get_all_items()
#items.save_object('items.json')
