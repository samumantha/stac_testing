

from satstac import Catalog, Collection, Item

#conda activate stac_query2

cat = Catalog.open('./stac_catalog5/catalog.json')
print(cat, cat.filename)

print('Description:', cat.description)
print('STAC Version:', cat.stac_version)

col = Collection.open('./stac_catalog5/Sentinel-2/collection.json')
print(col, col.filename)

item = Item.open('./stac_catalog5/Sentinel-2/T34VDM/S2A_MSIL2A_20160519T101032_N0202_R022_T34VDM_20160519T101312.SAFE/S2A_MSIL2A_20160519T101032_N0202_R022_T34VDM_20160519T101312.SAFE.json')
print(item, item.filename)

print('Title:', col.title)
print('Collection Version:', col.version)
print('Keywords: ', col.keywords)
print('License:', col.license)
print('Providers:', col.providers)
print('Extent', col.extent)

#for key in col.properties:
#    print('%s: %s' % (key, col[key]))

print(item.bbox)
print(item.geometry)
print(item.assets)



print(item.datetime)
print(item.date)

for key in item.properties:
    print('%s: %s' % (key, item.properties[key]))

print(item['eo:platform'])
print(item['eo:gsd'])



_col = item.collection()
print(_col, _col.filename)



print(item.assets.keys())
print(item.asset('thumbnail'))



print(item.asset('red'))

filename = item.download('L2A_T34VDM_20160519T101032_B03_60m')
# results in /home/samwitt/git/stac_testing/T34VDM/2016-05-19/S2A_MSIL2A_20160519T101032_N0202_R022_T34VDM_20160519T101312.SAFE_L2A_T34VDM_20160519T101032_B03_60m.jp2
print(filename)
filename = item.download('MTL', overwrite=True)
print(filename)
