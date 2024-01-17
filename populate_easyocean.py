import os, json, datetime, xarray, math
from pymongo import MongoClient
from geopy import distance

basins = xarray.open_dataset('parameters/basinmask_01.nc')
def find_basin(basins, lon, lat):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']
    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            #print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]
    return int(basin)

client = MongoClient('mongodb://database/argo')
db = client.argo

datadir = 'processed_data/converted_data/'
metadir = 'processed_data/metadata/'

datafiles = os.listdir(datadir)
metafiles = os.listdir(metadir)

for filename in metafiles:
	with open(metadir + filename) as f:
		m = json.load(f)
		m = {
			'occupancies': [m[str(i)] for i in range(len(m))],
			'_id': filename[0:-14],
			'date_updated_argovis': datetime.datetime.now()
		}

		for i in range(len(m['occupancies'])):
			m['occupancies'][i]['time_boundaries'][0] = datetime.datetime.strptime(m['occupancies'][i]['time_boundaries'][0],'%Y-%m-%d')
			m['occupancies'][i]['time_boundaries'][1] = datetime.datetime.strptime(m['occupancies'][i]['time_boundaries'][1],'%Y-%m-%d')

		# write metadata record to mongo
		try:
		    db.easyoceanMeta.replace_one({'_id': m['_id']}, m, True)
		except BaseException as err:
		    print('error: metadata upsert failure on', m)
		    print(err)

for filename in datafiles:
	with open(datadir + filename) as f:
		d = json.load(f)
		
		d['_id'] = filename[0:-5]
		d['metadata'] = d['woce_lines']
		del d['section_time_boundaries']
		d['section_start_date'] = datetime.datetime.strptime(d['section_start_date'],'%Y-%m-%d')
		d['section_end_date'] = datetime.datetime.strptime(d['section_end_date'],'%Y-%m-%d')
		d['dataset_created'] = datetime.datetime.strptime(d['dataset_created'],'%Y-%m-%d')
		del d['latitude']
		del d['longitude']
		d['timestamp'] = datetime.datetime.strptime(d['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
		d['basin'] = find_basin(basins, d['geolocation']['coordinates'][0], d['geolocation']['coordinates'][1])

		# write data record to mongo
		try:
		    db.easyocean.replace_one({'_id': d['_id']}, d, True)
		except BaseException as err:
		    print('error: data upsert failure on', d)
		    print(err)

