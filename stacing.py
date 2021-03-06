import pystac
from datetime import datetime
import os
import boto3
import rasterio
from shapely.geometry import box, mapping, GeometryCollection, shape
from xml.dom import minidom
import json
from pystac.extensions.eo import EOExtension #, Band

from rasterio.warp import transform_bounds
from rasterio.crs import CRS

"""
## TODO:
* make it proper class or script
* check client/resource
* implement temporal extent updating
* band information from eo extension to assets
* this is creating from scratch, make updating possible
* also check https://github.com/stac-utils/stac-sentinel/blob/main/stac_sentinel/sentinel.py

"""

#conda activate allas_stac

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

        rootcatalog = pystac.Catalog(id='Sentinel-2 catalog', description='Testcatalog.')
        rootcatalog.add_child(rootcollection)


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
            #crs is given in S2B_MSIL2A_20200626T095029_N0214_R079_T34VFN_20200626T123234.SAFE/GRANULE/L2A_T34VFN_A017265_20200626T095032/MTD_TL.xml
            bucketcontentcrs = [x for x in full_bucketcontent if 'MTD_TL.xml' in x]



            #list of safefiles in bucket
            listofsafes = list(set(list(map(lambda x: x.split('/')[0], full_bucketcontent))))
            print(listofsafes)

            for safe in listofsafes:
                print('one safe')

                # this results in one file, should work without list!
                metadatafile = [x for x in bucketcontentmtd if safe in x][0]
                crsmetadatafile = [x for x in bucketcontentcrs if safe in x][0]
                safecrs_string = self.get_crs(self.get_metadata_content(bucket, crsmetadatafile, resource))


                jp2images = []
                # only jp2 that are image bands
                [jp2images.append(x) for x in bucketcontentjp2 if safe in x and 'IMG_DATA' in x]
                # jp2 that are preview images
                
                previewimage = [x for x in bucketcontentjp2 if safe in x and 'PVI' in x][0]

                metadatacontent = self.get_metadata_content(bucket, metadatafile, resource)



                for jp2image in jp2images:

                    print('one image')

                    uri = '/vsis3/' + bucket + '/' + jp2image
                    #uri = 'https://a3s.fi/' + bucket + '/' + jp2image
                    print(uri)

                    #safename = jp2image.split('/')[0]
                    #print(safename)
                    

                    if not safe in [x.id for x in list(tilecollection.get_items())]:
                        item = self.make_item(uri, metadatacontent,safecrs_string)
                        # add preview image 
                        #self.add_asset(item, '/vsis/' + bucket + '/' + previewimage, True)
                        self.add_asset(item, 'https://a3s.fi/' + bucket + '/' + previewimage, True)

                    else:
                        
                        item = [x for x in list(tilecollection.get_items()) if safe in x.id][0]
                        self.add_asset(item, uri)
   

                    tilecollection.add_item(item)
                    
                    # safe to stac directory
                    #rootcollection.normalize_hrefs('stac_catalog')
                    rootcatalog.normalize_hrefs('stac_catalog5')
                    #rootcollection.validate_all()
                    rootcatalog.validate_all()

                    # update spatial extent
                    print('update spatial extent of root')
                    rootbounds= [list(GeometryCollection([shape(s.geometry) for s in rootcollection.get_all_items()]).bounds)]
                    #bounds_transformed = self.transform_crs(rootbounds, safecrs_string)
                    rootcollection.extent.spatial = pystac.SpatialExtent(rootbounds)

                    # update spatial extent
                    print('updating spatial extent of tile')
                    tilebounds= [list(GeometryCollection([shape(s.geometry) for s in tilecollection.get_all_items()]).bounds)]
                    #bounds_transformed = self.transform_crs(tilebounds, safecrs_string)
                    tilecollection.extent.spatial = pystac.SpatialExtent(tilebounds)

                    rootcatalog.save()
        


    def transform_crs(self, bounds, crs_string):
        
        crs = CRS.from_epsg(4326)
        safecrs = CRS.from_epsg(int(crs_string))
        bounds_transformed = list(transform_bounds(safecrs, crs, bounds[0][0], bounds[0][1], bounds[0][2], bounds[0][3]))
        
        return bounds_transformed


    def get_crs(self,crsmetadatafile):
        print(crsmetadatafile)
        with minidom.parse(crsmetadatafile) as doc:
            print(doc)

            crsstring = self.get_xml_content(doc, 'HORIZONTAL_CS_CODE')
        crsstring = crsstring.split(':')[-1]

        return crsstring

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

        print('making root collection')
        # collection instead of catalog because catalog does not have extent information

        # preliminary apprx Finland, later with bbox of all tiles from bucketname
        sp_extent = pystac.SpatialExtent([[0,0,0,0]])
        # fill with general Sentinel-2 timeframe, later get from all safefiles
        capture_date = datetime.strptime('2015-06-29', '%Y-%m-%d')
        tmp_extent = pystac.TemporalExtent([(capture_date, datetime.today())])
        extent = pystac.Extent(sp_extent, tmp_extent)

        # catalog_types: https://pystac.readthedocs.io/en/1.0/api.html#pystac.CatalogType
        rootcollection = pystac.Collection(id='Sentinel-2', description = 'Sentinel-2 dataset', extent = extent, catalog_type ='SELF_CONTAINED')

        print('root collection made')

        return rootcollection


    # make tilecollection with collective metadata

    def make_tile_collection(self,tile):

        print('making tilecollection')

        #placeholders added, spatial extent is being updated after item is added, temporal extent updating is still to do
        sp_extent = pystac.SpatialExtent([[0,0,0,0]])
        tmp_extent = pystac.TemporalExtent([(datetime.strptime('2015-10-22', '%Y-%m-%d'), datetime.today())])
        extent = pystac.Extent(sp_extent, tmp_extent)
        
        tilecollection = pystac.Collection(id=tile, description = 'Sentinel-2 dataset of tile ' + tile, extent = extent, stac_extensions='')

        print('tilecollection made')

        return tilecollection

    def make_item(self, uri, metadatacontent, crs_string):

        print('making item')
        params = {}
        #print('nametest: '+ uri.split('/')[3])
        params['id'] = uri.split('/')[3]
        print(params['id'])

    
        with rasterio.open(uri) as src:

            print(list([src.bounds]))
            # as lat,lon
            params['bbox'] = self.transform_crs(list([src.bounds]),crs_string)
            # is this footprint? ie bounds of everything excluding bounding nan?
            params['geometry'] = mapping(box(*params['bbox']))
            
        print(metadatacontent)
        mtddict = self.get_metadata_from_xml(metadatacontent)


        # could also be start_datetime and end_datetime from metadata
        params['datetime'] = datetime.strptime(uri.split('_')[2][0:8], '%Y%m%d')

        params['properties'] = {}

        params['properties']['cloud_cover'] = mtddict['cc_perc']
        #following are not part of eo extension
        params['properties']['data_cover'] = mtddict['data_cover']
        params['properties']['orbit'] = mtddict['orbit']
        params['properties']['baseline'] = mtddict['baseline']
        # following are part of general metadata
        params['properties']['platform'] = 'sentinel-2'
        params['properties']['instrument'] = 'msi'
        params['properties']['constellation'] = 'sentinel-2'
        params['properties']['mission'] = 'copernicus'
        

        stacitem = pystac.Item(**params)

        # below needed? possibly for reading , see https://github.com/stac-extensions/eo
        EOExtension.add_to(stacitem)

        # bands seem useful for multiband tifs but not for s2?
        # bands have more metadata but no link to file?
        
        print('item made')

        return stacitem

    def add_asset(self, stacitem, uri ,thumbnail=False):

        
        
        print('adding asset')
        full_bandname = uri.split('/')[-1].split('.')[0]
        public_uri = uri.replace('vsis3', 'https://a3s.fi')
        public_uri = public_uri[1:]

        #print(full_bandname)
        #print(uri)

        if not thumbnail:
            print('false')
            print(full_bandname)

            stacitem.add_asset(key=full_bandname, asset=pystac.Asset(href=public_uri,
                                                        title=full_bandname,
                                                        media_type=pystac.MediaType.JPEG2000))
        else:
            print('true')
            print(full_bandname)
            stacitem.add_asset(key=full_bandname, asset=pystac.Asset(href=public_uri,
                                                        title=full_bandname,
                                                        roles= ['thumbnail'],
                                                        media_type=pystac.MediaType.JPEG2000))
        print('asset added')
        stacitem.validate()
        print('asset validated')


        return stacitem

    def get_xml_content(self, doc, tagname):
        content = doc.getElementsByTagName(tagname)[0].firstChild.data
        return content

    def get_metadata_from_xml(self, metadatabody):

        print('metadata extraction start')
        print(metadatabody)
        with minidom.parse(metadatabody) as doc:
            metadatadict = {}
            metadatadict['cc_perc'] = int(float(self.get_xml_content(doc,'Cloud_Coverage_Assessment')))
            metadatadict['data_cover'] = 100 - int(float(self.get_xml_content(doc,'NODATA_PIXEL_PERCENTAGE')))
            #metadatadict['start_time'] = self.get_xml_content(doc,'PRODUCT_START_TIME')
            #metadatadict['end_time'] = self.get_xml_content(doc,'PRODUCT_STOP_TIME')
            #metadatadict['bbox'] = self.get_xml_content(doc,'EXT_POS_LIST')
            metadatadict['orbit'] = self.get_xml_content(doc,'SENSING_ORBIT_NUMBER')
            metadatadict['baseline'] = self.get_xml_content(doc,'PROCESSING_BASELINE')
            #metadatadict['producttype'] = self.get_xml_content(doc,'PRODUCT_TYPE')
            #productname = get_xml_content(doc,'PRODUCT_URI').split('.')[0]
        print('metadata extraction end')

        return metadatadict


STACing()


