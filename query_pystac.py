# example adjusted from https://pystac.readthedocs.io/en/1.0/quickstart.html#Reading-STAC


import pystac

catalog = pystac.Catalog.from_file('./stac_catalog/catalog.json')
#collection = pystac.Collection.from_file('https://a3s.fi/swift/v1/AUTH_53f5b0ae8e724b439a4cd16d1237015f/2001659_stactest/stacs_eo_7/catalog.json')
print(catalog.description)


while len(catalog.get_item_links()) == 0:
    print('Crawling through {}'.format(catalog))
    catalog = next(catalog.get_children())


print('Contains {} items.'.format(len(catalog.get_item_links())))

#take first item
item = next(catalog.get_items())

asset = item.get_assets()['L2A_T34VDM_20160605T100032_B02_10m']
print(asset)

print(item.common_metadata.platform)
print(item.id)
print(item.properties['cloud_cover'])
print(item.geometry['coordinates'])

