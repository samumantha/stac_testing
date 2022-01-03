
import boto3
import os
from xml.dom import minidom



# from Kylli
client = boto3.client('s3', endpoint_url='https://a3s.fi')
bucket = 'Sentinel2-MSIL2A-cloud-0-95-2016-T34VDN'
objects = client.list_objects(Bucket=bucket)

object_keys = [s['Key'] for s in objects['Contents']]
scenes = list(map(lambda x: x.split('/')[0], object_keys))
unique_scenes = list(set(scenes))



#mine
# get uri
location = client.get_bucket_location(Bucket=bucket)['LocationConstraint']
print(location)

url = client.generate_presigned_url('get_object', ExpiresIn=10, Params={'Bucket': bucket, 'Key': unique_scenes[0]})

print(url)


resource =  boto3.resource('s3', endpoint_url='https://a3s.fi')
object_mtd_keys = [x['Key'] for x in objects['Contents'] if 'MTD_MSIL2A.xml' in x['Key']]


for object_key in object_mtd_keys:

    print(object_key)
    productname = object_key.split('/')[0]
    productpath = bucket + '/' + productname
    
    obj = resource.Object(bucket_name = bucket,key = object_key)
    body = obj.get()['Body']
    doc = minidom.parse(body)
    cc_perc = float(doc.getElementsByTagName('Cloud_Coverage_Assessment')[0].firstChild.data)
    nodata_perc = 100 - float(doc.getElementsByTagName('NODATA_PIXEL_PERCENTAGE')[0].firstChild.data)

    datetime = doc.getElementsByTagName('DATATAKE_SENSING_START')[0].firstChild.data

    bbox = doc.getElementsByTagName('EXT_POS_LIST')[0].firstChild.data




#print(datetime)
#print(bbox)


# from Kylli for bbox
for scene in scenes:
    uri = '/vsis3/' + bucket + '/' + scene
    print(uri)
    params = {}
    params['id'] = basename(uri).split('.')[0]
    with rasterio.open(uri) as src:
        params['bbox'] = list(src.bounds)
        params['geometry'] = mapping(box(*params['bbox']))
    params['datetime'] = datetime.strptime(uri.split('_')[2][0:7], '%Y%m%d')
    params['properties'] = {}
    i = pystac.Item(**params)
    i.add_asset(key='image', asset=pystac.Asset(href=uri,
                                                title='Geotiff',
                                                media_type=pystac.MediaType.GEOTIFF))
    vegas.add_item(i)

bounds = [list(GeometryCollection([shape(s.geometry) for s in spacenet.get_all_items()]).bounds)]
vegas.extent.spatial = pystac.SpatialExtent(bounds)