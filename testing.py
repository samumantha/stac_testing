
import boto3
import os
from xml.dom import minidom



# from Kylli

os.environ["AWS_S3_ENDPOINT"] = "a3s.fi"

client = boto3.client('s3', endpoint_url='https://a3s.fi')
# mine
# following gets all buckets names in project that Allas is set up with currently ('source allas_conf -u wittkesa --mode s3cmd'), should be done with regex (something like Sentinel2-MSIL2A-cloud-0-95-201|2[0-9]-T[0-9]{2}[A-Z]{3}$)
buckets = [x['Name'] for x in client.list_buckets()['Buckets'] if not x['Name'].endswith('segments') and not 'README' in x['Name'] and not 'readme' in x['Name']]
print(buckets)

#from Kylli

bucket = 'Sentinel2-MSIL2A-cloud-0-95-2016-T34VDN'
# does it have to use client again?
objects = client.list_objects(Bucket=bucket)

object_keys = [s['Key'] for s in objects['Contents'] if '.jp2' in s['Key']]
object_keys = [s['Key'] for s in objects['Contents']]
scenes = list(map(lambda x: x.split('/')[0], object_keys))
print(object_keys)

unique_scenes = list(set(scenes))
"""



#mine


url = '/vsis3/' + bucket + unique_scenes[0]
#url = client.generate_presigned_url('get_object', ExpiresIn=100, Params={'Bucket': bucket, 'Key': unique_scenes[0]})

print(url)


resource =  boto3.resource('s3', endpoint_url='https://a3s.fi')
object_mtd_keys = [x['Key'] for x in objects['Contents'] if 'MTD_MSIL2A.xml' in x['Key']]

def get_content(doc, tagname):
    content = doc.getElementsByTagName(tagname)[0].firstChild.data
    return content

"""
"""
for object_key in object_mtd_keys:

    print(object_key)
    productname = object_key.split('/')[0]
    productpath = bucket + '/' + productname
    
    obj = resource.Object(bucket_name = bucket,key = object_key)
    body = obj.get()['Body']
    doc = minidom.parse(body)
    cc_perc = float(get_content(doc,'Cloud_Coverage_Assessment'))
    nodata_perc = 100 - float(get_content(doc,'NODATA_PIXEL_PERCENTAGE'))
    datetime = get_content(doc,'DATATAKE_SENSING_START')
    bbox = get_content(doc,'EXT_POS_LIST')
    orbit = get_content(doc,'SENSING_ORBIT_NUMBER')
    baseline = get_content(doc,'PROCESSING_BASELINE')
    producttype = get_content(doc,'PRODUCT_TYPE')
    #productname = get_content(doc,'PRODUCT_URI').split('.')[0]
"""
"""


#print(datetime)
#print(bbox)


# from Kylli for bbox

from os.path import basename
from shapely.geometry import box, mapping
import rasterio
import pystac
from datetime import datetime



sp_extent = pystac.SpatialExtent([None,None,None,None])
capture_date = datetime.strptime('2015-10-22', '%Y-%m-%d')
tmp_extent = pystac.TemporalExtent([(capture_date, datetime.today())])
extent = pystac.Extent(sp_extent, tmp_extent)

parentcollection = pystac.Collection(id='Sentinel-2', description = 'Sentinel-2 dataset', extent = extent)

#parentcollection.add_child(vegas)
parentcollection.describe()

print(scenes[0])
for scene in object_keys:
    if '20160516' in scene or '20160625' in scene:
        uri = '/vsis3/' + bucket + '/' + scene
        print(uri)
        params = {}
        params['id'] = basename(uri).split('.')[0]
        with rasterio.open(uri) as src:
            params['bbox'] = list(src.bounds)
            params['geometry'] = mapping(box(*params['bbox']))
        params['datetime'] = datetime.strptime(uri.split('_')[2][0:7], '%Y%m%d')
        params['properties'] = {}
        stacitem = pystac.Item(**params)
        stacitem.add_asset(key='image', asset=pystac.Asset(href=uri,
                                                    title='Geotiff',
                                                    media_type=pystac.MediaType.GEOTIFF))
        parentcollection.add_item(stacitem)

parentcollection.describe()

#bounds = [list(GeometryCollection([shape(s.geometry) for s in spacenet.get_all_items()]).bounds)]
#vegas.extent.spatial = pystac.SpatialExtent(bounds)


####
#print(list(catalog.get_children()))
#print(list(catalog.get_items()))

"""