import pystac
from datetime import datetime
import os
import boto3
import rasterio
from shapely.geometry import box, mapping, GeometryCollection, shape
from xml.dom import minidom
import json

"""
## TODO:
* implement spatial extent updating
* implement temporal extent updating
* this is creating from scratch, make updating possible
* 
"""

# make Sentinel-2 collection with metadata

class STACing(object):

    def __init__(self):
        self.create_collection()


    def create_collection(self):

        # create client 
        os.environ["AWS_S3_ENDPOINT"] = "a3s.fi"
        # are both of following needed?
        client = boto3.client('s3', endpoint_url='https://a3s.fi')
        resource =  boto3.resource('s3', endpoint_url='https://a3s.fi')

        # following gets all buckets names in project that Allas is set up with currently ('source allas_conf -u wittkesa --mode s3cmd'), should be done with regex (something like Sentinel2-MSIL2A-cloud-0-95-201|2[0-9]-T[0-9]{2}[A-Z]{3}$)
        buckets = [x['Name'] for x in client.list_buckets()['Buckets'] if not x['Name'].endswith('segments') and not 'README' in x['Name'] and not 'readme' in x['Name']]
        
    
        itemlist = []
        rootcollection = self.make_root_collection()

        for bucket in buckets: 
            print(bucket)     
            
            # check that tilecollection exists
            tile = bucket.split('-')[-1]
            if not tile in rootcollection.get_collections():
                tilecollection = self.make_tile_collection(tile)
                rootcollection.add_child(tilecollection)
            else:
                tilecollection = rootcollection.get_collections()[tile]
            print(tile)
            
            # list everything in bucket
            # does it have to use client again?
            objects = client.list_objects(Bucket=bucket)

            # following could be done with a dict?
            # list of all jp2 images in bucket

            full_bucketcontent = [s['Key'] for s in objects['Contents']]

            bucketcontentjp2 = [x for x in full_bucketcontent if '.jp2' in x]
            #provides list of 'S2A_MSIL2A_20160625T100032_N0204_R122_T34VDN_20160625T100027.SAFE/GRANULE/L2A_T34VDN_A005267_20160625T100027/QI_DATA/L2A_T34VDN_20160625T100032_SNW_60m.jp2'
            bucketcontentmtd = [x for x in full_bucketcontent if 'MTD_MSIL2A.xml' in x]



            #list of safefiles in bucket
            listofsafes = list(set(list(map(lambda x: x.split('/')[0], full_bucketcontent))))
            print(listofsafes)

            for safe in listofsafes:
                print(safe)

                # this results in one file, should work without list!
                metadatafile = [x for x in bucketcontentmtd if safe in x][0]
                jp2images = []
                [jp2images.append(x) for x in bucketcontentjp2 if safe in x]

                metadatacontent = self.get_metadata_content(bucket, metadatafile, resource)

        
                for jp2image in jp2images:

                    uri = '/vsis3/' + bucket + '/' + jp2image

                    #safename = jp2image.split('/')[0]
                    #print(safename)
                    

                    if not safe in [x.id for x in list(tilecollection.get_items())]:
                        item = self.make_item(uri, metadatacontent)

                    else:
                        
                        item = [x for x in list(tilecollection.get_items()) if safe in x.id][0]
                        self.add_asset(item, uri)

                    tilecollection.add_item(item)

                    #print(list(tilecollection.get_items()))
                    #tilecollectionitems = [x.id for x in list(tilecollection.get_items())]
                    #print(tilecollectionitems)

                    #rootcollection.describe()
                    
                    rootcollection.normalize_hrefs('./stacs')

                    rootcollection.validate_all()
                    rootcollection.save()
        """
            # update spatial extent
            bounds = [list(GeometryCollection([shape(s.geometry) for s in tilecollection.get_all_items()]).bounds)]
            tilecollection.extent.spatial = pystac.SpatialExtent(bounds)
        # update spatial extent
        bounds = [list(GeometryCollection([shape(s.geometry) for s in rootcollection.get_all_items()]).bounds)]
        rootcollection.extent.spatial = pystac.SpatialExtent(bounds)
        """


    def get_metadata_content(self, bucket, metadatafile, resource):

        print('get_metadata_content')
        print(metadatafile)
        print(bucket)

        productname = metadatafile.split('/')[0]
        productpath = bucket + '/' + productname
        
        obj = resource.Object(bucket_name = bucket,key = metadatafile)
        metadatacontent = obj.get()['Body']

        return metadatacontent
        


    def make_root_collection(self):

        # preliminary apprx Finland, later with bbox of all tiles from bucketname
        sp_extent = pystac.SpatialExtent([[20.57,59.93,29.80,70.29]])
        # fill with general Sentinel-2 timeframe, later get from all safefiles
        capture_date = datetime.strptime('2015-06-29', '%Y-%m-%d')
        tmp_extent = pystac.TemporalExtent([(capture_date, datetime.today())])
        extent = pystac.Extent(sp_extent, tmp_extent)

        rootcollection = pystac.Collection(id='Sentinel-2', description = 'Sentinel-2 dataset', extent = extent)

        return rootcollection 


    def get_tile_extent(self,tile):


        tileextent = [[22.57,63.93,27.80,68.29]]
        print(tileextent)


        return tileextent

    def get_temporal_extent(self):
        
        temporalextent = [(datetime.strptime('2015-10-22', '%Y-%m-%d'), datetime.today())]

        return temporalextent


    # make tilecollection with collective metadata

    def make_tile_collection(self,tile):

        sp_extent = pystac.SpatialExtent(self.get_tile_extent(tile))
        tmp_extent = pystac.TemporalExtent(self.get_temporal_extent())
        extent = pystac.Extent(sp_extent, tmp_extent)
        

        tilecollection = pystac.Collection(id=tile, description = 'Sentinel-2 dataset of tile ' + tile, extent = extent, stac_extensions='')

        return tilecollection


    """
    def make_bucketname(self, safename):
        bucketbase = 'Sentinel2-MSIL2A-cloud-0-95-'
        year = safename.split('_')[2][0:4]
        tile = safename.split('_')[5]
        bucketname = bucketbase + year + '-' + tile
        return bucketname
    """



    def make_item(self, uri, metadatacontent):

    
        print('yea')
        params = {}
        #print('nametest: '+ uri.split('/')[3])
        params['id'] = uri.split('/')[3]

    
        with rasterio.open(uri) as src:
            # does this give lon,lat?
            params['bbox'] = list(src.bounds)
            # is this footprint? ie bounds of everything excluding bounding nan?
            params['geometry'] = mapping(box(*params['bbox']))
            
            
        # not currently used
        #mtddict = self.get_metadata_from_xml(metadatacontent)

        # could also be start_datetime and end_datetime from metadata
        params['datetime'] = datetime.strptime(uri.split('_')[2][0:8], '%Y%m%d')
        #are we sure this is right date? below are not accepted?
        #params['start_datetime'] = mtddict['start_time']
        #params['end_datetime'] = mtddict['end_time']
        params['properties'] = {}
        #following line is not tested
        #params['extensions'] = ['EOExtension']

        stacitem = pystac.Item(**params)

        # not currently used, could not find extension schema
        # mtddict = self.get_metadata_from_xml(metadatacontent)

        #eo_ext = pystac.extensions.eo.EOExtension.ext(stacitem)
        

        #print(eo_ext)

        #eo_ext.cloud_cover = mtddict['cc_perc']


        # try also following instead of above
        #item.ext.enable('eo')
        #item.ext.eo.cloud_cover = value

        return stacitem

    def add_asset(self, stacitem, uri):
        
        print('adding asset')
        full_bandname = uri.split('/')[-1].split('.')[0]

        #print(full_bandname)
        #print(uri)

        stacitem.add_asset(key=full_bandname, asset=pystac.Asset(href=uri,
                                                    title=full_bandname,
                                                    media_type=pystac.MediaType.JPEG2000))

        stacitem.validate()

        return stacitem

    def get_xml_content(self, doc, tagname):
        content = doc.getElementsByTagName(tagname)[0].firstChild.data
        return content

    def get_metadata_from_xml(self, metadatabody):


        doc = minidom.parse(metadatabody)
        metadatadict = {}
        metadatadict['cc_perc'] = int(float(self.get_xml_content(doc,'Cloud_Coverage_Assessment')))
        metadatadict['nodata_perc'] = 100 - float(self.get_xml_content(doc,'NODATA_PIXEL_PERCENTAGE'))
        metadatadict['start_time'] = self.get_xml_content(doc,'PRODUCT_START_TIME')
        metadatadict['end_time'] = self.get_xml_content(doc,'PRODUCT_STOP_TIME')
        metadatadict['bbox'] = self.get_xml_content(doc,'EXT_POS_LIST')
        metadatadict['orbit'] = self.get_xml_content(doc,'SENSING_ORBIT_NUMBER')
        metadatadict['baseline'] = self.get_xml_content(doc,'PROCESSING_BASELINE')
        metadatadict['producttype'] = self.get_xml_content(doc,'PRODUCT_TYPE')
        #productname = get_xml_content(doc,'PRODUCT_URI').split('.')[0]

        return metadatadict


STACing()


# see https://github.com/stac-extensions/eo