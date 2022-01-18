from satsearch import Search

#https://geospatial.101workbook.org/ImportingData/ImportingImages.html#stac_func

def get_STAC_items(url, collection, dates, bbox):
    results = Search.search(url=url,
                            collections=collection,
                            datetime=dates,
                            bbox=bbox)
    return(results)
