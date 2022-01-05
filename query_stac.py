# example adjusted from https://pystac.readthedocs.io/en/1.0/quickstart.html#Reading-STAC


import pystac

collection = pystac.Collection.from_file('https://a3s.fi/swift/v1/AUTH_53f5b0ae8e724b439a4cd16d1237015f/2001659_stactest/stacs_eo_7/collection.json')
print(collection.description)


while len(collection.get_item_links()) == 0:
    print('Crawling through {}'.format(collection))
    collection = next(collection.get_children())


print('Contains {} items.'.format(len(collection.get_item_links())))

#take first item
item = next(collection.get_items())

print(item.common_metadata.platform)
print(item.id)
print(item.properties['cloud_cover'])

