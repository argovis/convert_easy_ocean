# Create JSON from Easy Ocean gridded netCDF files

# unit of time
# units = "days since 1950-01-01 00:00:00 UTC";
# The time null value is Jan 1, 1950 at 00:00:00

# ncdump -v time p06.nc gives time as integers

# For the matlab files, time is datenum and not days
# In matlab, you get minutes resolution, but none in netcdf. None in ncdump or xarray

# Group says it didn't matter to them about the time resolution

# Get warning
# RuntimeWarning: invalid value encountered in cast
#   flat_num_dates_ns_int = (flat_num_dates * _NS_PER_TIME_DELTA[delta]).astype(

# import scipy.io as sio
from pathlib import Path
import requests
from tempfile import NamedTemporaryFile
import pandas as pd
import numpy as np
import math
from datetime import datetime
import xarray as xr
import os
import shutil
import json


from scrape_easyocean_html_for_files import scrape_easyocean_html_for_files
from get_metadata_matlab_gridded_easyocean import get_all_metadata_from_matlab_files

# Create requests session
session = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=5)
session.mount("https://", a)


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def write_lat_lon_dict(woce_line, section_index, lat_lon_dict):
    # expocodes = lat_lon_dict["expocodes"]
    # str_expocodes = "-".join(expocodes)

    # # Replace / with _
    # str_expocodes = str_expocodes.replace("/", "_")

    timestamp = lat_lon_dict["timestamp"]

    # timestamp of form 1993-10-25T00:00:00.000000000
    # extract date portion
    # timestamp = np.datetime_as_string(timestamp)

    datestamp = timestamp.split("T")[0]

    datestamp = datetime.strptime(datestamp, "%Y-%m-%d").strftime("%Y%m%d")

    latitude = lat_lon_dict["latitude"]
    lat_no_decimal = str(latitude).replace(".", "-")
    longitude = lat_lon_dict["longitude"]
    lon_no_decimal = str(longitude).replace(".", "-")

    filename = f"woce_{woce_line.lower()}_date_{datestamp}_lat_{lat_no_decimal}_lon_{lon_no_decimal}.json"

    # if varying_direction == "lat":
    #     latitude = lat_lon_dict["latitude"]
    #     lat_no_decimal = str(latitude).replace(".", "-")
    #     longitude = lat_lon_dict["longitude"]
    #     lon_no_decimal = str(longitude).replace(".", "-")

    #     filename = f"woce_{woce_line.lower()}_date_{datestamp}_lat_{lat_no_decimal}_lon_{lon_no_decimal}.json"

    # elif varying_direction == "lon":
    #     latitude = lat_lon_dict["latitude"]
    #     lat_no_decimal = str(latitude).replace(".", "-")
    #     longitude = lat_lon_dict["longitude"]
    #     lon_no_decimal = str(longitude).replace(".", "-")

    #     filename = f"woce_{woce_line.lower()}_date_{datestamp}_lat_{lat_no_decimal}_lon_{lon_no_decimal}.json"

    # else:
    #     filename = None

    # Write dict to json file

    dir = "../processed_data/converted_data"
    filepath = f"{dir}/{filename}"

    # datetime is not JSON serializable so use dtjson to convert
    if filename is not None:
        with open(filepath, "w") as f:
            json.dump(lat_lon_dict, f, indent=4, default=dtjson)


def to_none(val):
    if math.isnan(val):
        return None
    return val


def get_data(df_params, parameters):
    data = []

    for param in parameters:
        param_vals = df_params[param].tolist()

        param_vals = [to_none(val) for val in param_vals]

        data.append(param_vals)

    return data


def get_parameter_info(parameter):
    # possible parameters
    # pressure  temperature  practical_salinity  oxygen
    # conservative_temperature  absolute_salinity

    #     "units",
    #     "reference_scale",
    #     "data_keys_mapping",
    #     "data_source_standard_names",
    #     "data_source_units",
    #     "data_source_reference_scale"

    parameter_info = []

    if parameter == "pressure":
        parameter_info.append("decibar")
        parameter_info.append(None)
        parameter_info.append("pressure")
        parameter_info.append("sea_water_pressure")
        parameter_info.append("dbar")
        parameter_info.append(None)

    elif parameter == "temperature":
        parameter_info.append("Celcius")
        parameter_info.append("ITS-90")
        parameter_info.append("temperature")
        parameter_info.append("sea_water_temperature")
        parameter_info.append("degC")
        parameter_info.append("ITS-90")

    elif parameter == "practical_salinity":
        parameter_info.append("psu")
        parameter_info.append("PSS-78")
        parameter_info.append("practical_salinity")
        parameter_info.append("sea_water_practical_salinity")
        parameter_info.append("1")
        parameter_info.append("PSS-78")

    elif parameter == "oxygen":
        parameter_info.append("micromole/kg")
        parameter_info.append(None)
        parameter_info.append("oxygen")
        parameter_info.append("moles_of_oxygen_per_unit_mass_in_sea_water")
        parameter_info.append("umol kg-1")
        parameter_info.append(None)

    elif parameter == "conservative_temperature":
        parameter_info.append("Celcius")
        parameter_info.append("TEOS-10")
        parameter_info.append("conservative_temperature")
        parameter_info.append("seawater_conservative_temperature")
        parameter_info.append("degC")
        parameter_info.append("TEOS-10")

    elif parameter == "absolute_salinity":
        parameter_info.append("g/kg")
        parameter_info.append("TEOS-10")
        parameter_info.append("absolute_salinity")
        parameter_info.append("sea_water_absolute_salinity")
        parameter_info.append("g kg-1")
        parameter_info.append("TEOS-10")

    else:
        parameter_info = []

    return parameter_info


def rename_parameters_to_argovis(parameters):
    argovis_parameter_names = []

    for param in parameters:
        if param == "oxygen":
            new_param = "doxy"
        elif param == "practical_salinity":
            new_param = "ctd_salinity"
        elif param == "temperature":
            new_param = "ctd_temperature"
        else:
            new_param = param

        argovis_parameter_names.append(new_param)

    return argovis_parameter_names


def get_data_info(df_params):
    # [
    #     "salinity",
    #     "temperature",
    #     "pressure",
    # ],
    # [
    #     "units",
    #     "reference_scale",
    #     "data_keys_mapping",
    #     "data_source_standard_names",
    #     "data_source_units",
    #     "data_source_reference_scale"
    # ],
    # [
    #     [
    #         "psu",
    #         "PSS-78",
    #         "ctd_salinity",
    #         "sea_water_practical_salinity",
    #         "1",
    #         "PSS-78"
    #     ],

    data_info = []

    parameters = list(df_params.columns)

    argovis_parameter_names = rename_parameters_to_argovis(parameters)

    data_info.append(argovis_parameter_names)

    parameter_order = [
        "units",
        "reference_scale",
        "data_keys_mapping",
        "data_source_standard_names",
        "data_source_units",
        "data_source_reference_scale",
    ]

    data_info.append(parameter_order)

    parameters_information = []

    for parameter in parameters:
        # parameter_info = [
        #     "psu",
        #     "PSS-78",
        #     "ctd_salinity",
        #     "sea_water_practical_salinity",
        #     "1",
        #     "PSS-78"
        # ],

        parameter_info = get_parameter_info(parameter)

        parameters_information.append(parameter_info)

    data_info.append(parameters_information)

    return data_info


def create_geolocation_dict(lat, lon):
    # "geolocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict["coordinates"] = coordinates
    geo_dict["type"] = "Point"

    return geo_dict


def get_country_codes(section_expocodes):
    # Get country code and use ICES code value
    # This is first two numbers in expocode

    country_codes = []

    for expocode in section_expocodes:
        if expocode != "None":
            country = expocode[0:2]
        else:
            country = "unknown"

        country_codes.append(country)

    return country_codes


def get_iso_timestamp(cruise_date):
    timestamp = cruise_date.isoformat()

    return timestamp


def get_iso_timestamp_boundaries(section_index, sections_lat_lon_metadata):
    time_boundaries = sections_lat_lon_metadata[str(section_index)]["time_boundaries"]

    time_boundary_start = datetime.strptime(time_boundaries[0], "%Y-%m-%d")
    time_boundary_end = datetime.strptime(time_boundaries[1], "%Y-%m-%d")

    # Convert python times into timestamps
    start_timestamp_boundary = get_iso_timestamp(time_boundary_start)
    start_timestamp_boundary = get_iso_timestamp(time_boundary_end)

    timestamp_boundaries = [start_timestamp_boundary, start_timestamp_boundary]

    return timestamp_boundaries


def create_section_metadata(
    section_index,
    global_metadata,
    section_expocodes,
    section_time_boundaries,
    sections_lat_lon_metadata,
):
    metadata = {}

    # global_metadata["source_file"] = base_filename

    # Get section metadata
    # section_expocodes = sections_lat_lon_metadata[str(section_index)]["expocodes"]

    metadata["section_expocodes"] = section_expocodes

    metadata["section_time_boundaries"] = section_time_boundaries

    metadata["section_start_date"] = section_time_boundaries[0]
    metadata["section_end_date"] = section_time_boundaries[1]

    start_year = section_time_boundaries[0].split("-")[0]
    end_year = section_time_boundaries[1].split("-")[0]

    if start_year != end_year:
        print(
            f"section from {section_time_boundaries[0]} to  {section_time_boundaries[1]}"
        )

    # metadata['timestamp_boundaries'] = get_iso_timestamp_boundaries(section_index, sections_lat_lon_metadata)

    # metadata["cchdo_cruise_ids"] = get_cruise_ids(section_expocodes)

    goship_woce_line_id = global_metadata["goship_woce_line_id"]
    metadata["woce_lines"] = [goship_woce_line_id]

    metadata["instrument"] = global_metadata["instrument"]
    metadata["references"] = global_metadata["references"]

    date_issued = global_metadata["date_issued"]
    # Change format to YYYY-mm-dd
    reformatted_date_issued = datetime.strptime(date_issued, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    metadata["dataset_created"] = reformatted_date_issued

    metadata["section_countries"] = get_country_codes(section_expocodes)

    metadata["positioning_system"] = "GPS"
    metadata["data_center"] = "CCHDO"

    # Break the source key into multiple array entries
    # "source": [
    #     {
    #         "source": [
    #             "cchdo_go-ship"
    #         ],
    #         "cruise_url": "https://cchdo.ucsd.edu/cruise/06AQ200012_3",
    #         "url": "https://cchdo.ucsd.edu/data/39482/06ANTXVIII_3_ctd.nc",
    #         "file_name": "06ANTXVIII_3_ctd.nc"
    #     }
    # ],

    # Get sources
    sources = []

    source_elem = {}
    source_elem["source"] = ["Easy Ocean"]

    source_elem["url"] = global_metadata["source_url"]

    source_elem["filename"] = global_metadata["source_file"]

    sources.append(source_elem)

    metadata["source"] = sources

    return metadata


def extract_data(woce_line, sections_data, global_metadata, sections_lat_lon_metadata):
    section_indices = sections_data.keys()

    # Get metadata from global attrs and from lat/lon mat file

    for section_index in section_indices:
        lat_values = sections_data[section_index]["lat"]
        lon_values = sections_data[section_index]["lon"]
        time_values = sections_data[section_index]["time"]

        section_expocodes = sections_data[section_index]["section_expocodes"]

        section_time_boundaries = sections_data[section_index][
            "section_time_boundaries"
        ]

        params_dfs = sections_data[section_index]["params_dfs"]

        # # round lat and lon to two decimal places
        # lat_values = np.round(lat_values, 2)
        # lon_values = np.round(lon_values, 2)

        # Get section metadata
        section_metadata = create_section_metadata(
            section_index,
            global_metadata,
            section_expocodes,
            section_time_boundaries,
            sections_lat_lon_metadata,
        )

        # print(section_metadata)

        # The number of lat and lon indices are the same
        for lat_lon_index in range(len(lat_values)):
            varying_direction = sections_lat_lon_metadata[str(section_index)][
                "varying_direction"
            ]

            lat_lon_dict = {}

            lat = lat_values[lat_lon_index]
            lon = lon_values[lat_lon_index]
            timestamp = time_values[lat_lon_index]

            section_metadata["latitude"] = lat
            section_metadata["longitude"] = lon

            geolocation = create_geolocation_dict(lat, lon)
            section_metadata["geolocation"] = geolocation

            # convert timestamp to string
            # "1993-09-24T00:00:00.000000000Z" and chop of ms portion
            utc_timestamp = np.datetime_as_string(timestamp, timezone="UTC")

            pieces = utc_timestamp.split(".")
            timestamp = pieces[0] + "Z"

            section_metadata["timestamp"] = timestamp

            df_params = params_dfs[lat_lon_index]
            parameters = list(df_params.columns)

            data_info = get_data_info(df_params)

            data = get_data(df_params, parameters)

            lat_lon_dict = section_metadata

            lat_lon_dict["data_info"] = data_info
            lat_lon_dict["data"] = data

            write_lat_lon_dict(woce_line, section_index, lat_lon_dict)


def process_varying_lon(
    section_index, latitude, longitude, section_time, pressure, params_values
):
    # Save lat, lon, time, and parameter dfs for each loop
    lat_values = []
    lon_values = []
    time_values = []
    params_dfs = []

    # For the parameters, the latitude index = 0 because only longitude varies
    lat_index = 0

    for lon_index in range(longitude.size):
        params = list(params_values.keys())

        # Create a dataframe holding parameters at one lat and one lon point
        df_params = pd.DataFrame()

        # add pressure column to df with params
        df_params["pressure"] = pressure

        # print(f"size of pressure is {pressure.shape}")
        # print(f"size of section time is {section_time.shape}")

        prac_sal = params_values["practical_salinity"]

        # shape is (651, 1, 898, 5)
        # print(prac_sal.shape)

        # print(f"all params are {params}")

        time = None

        for param in params:
            if param == "time":
                # shape is (898, 5)
                # (params_values["time"].shape)
                time = params_values["time"][lon_index, section_index]
                # print(f"time is {time}")
                # print(f"lat is {latitude[0]}")
                # print(f"lon is {longitude[lon_index]}")

            else:
                df_params[param] = params_values[param][
                    :, lat_index, lon_index, section_index
                ]

        # print(df_params.columns)

        # Get df where remove columns other than pressure with all NaN
        df = df_params.dropna(
            subset=df_params.columns.difference(["pressure"]), axis=0, how="all"
        )

        if df.empty:
            continue

        lat = latitude[0]
        lon = longitude[lon_index]

        lat_values.append(lat)
        lon_values.append(lon)
        time_values.append(time)

        # Get df with pressure and remove other columns with all NaN
        # df = df_params.dropna(
        #     subset=df_params.columns.difference(["pressure"]), axis=0, how="all"
        # )

        params_dfs.append(df)

        # print(df)

    return lat_values, lon_values, time_values, params_dfs


def process_varying_lat(
    section_index, latitude, longitude, section_time, pressure, params_values
):
    # Save lat, lon, time, and parameter dfs for each loop
    lat_values = []
    lon_values = []
    time_values = []
    params_dfs = []

    # For the parameters, the longitude index = 0 because only latitude varies
    lon_index = 0

    # print("here")
    # print(latitude)

    for lat_index in range(latitude.size):
        params = list(params_values.keys())

        # Create a dataframe holding parameters at one lat and one lon point
        df_params = pd.DataFrame()

        # add pressure column to df with params
        df_params["pressure"] = pressure

        # print(f"size of pressure is {pressure.shape}")
        # print(f"size of section time is {section_time.shape}")

        prac_sal = params_values["practical_salinity"]

        # shape is (651, 1, 898, 5)
        # print(prac_sal.shape)

        # print(f"all params are {params}")

        time = None

        for param in params:
            if param == "time":
                # shape is (898, 5)
                # print(params_values["time"].shape)
                time = params_values["time"][lat_index, section_index]
                # print(f"time is {time}")
                # print(f"lat is {latitude[lat_index]}")
                # print(f"lon is {longitude[0]}")

            else:
                df_params[param] = params_values[param][
                    :, lat_index, lon_index, section_index
                ]

        # print(df_params.columns)

        # Get df where remove columns other than pressure with all NaN
        df = df_params.dropna(
            subset=df_params.columns.difference(["pressure"]), axis=0, how="all"
        )

        if df.empty:
            continue

        lat = latitude[lat_index]
        lon = longitude[0]

        lat_values.append(lat)
        lon_values.append(lon)
        time_values.append(time)

        params_dfs.append(df)

        # print(df)

    return lat_values, lon_values, time_values, params_dfs


def get_iso_timestamps(timestamp):
    dt_obj = pd.to_datetime(timestamp, unit="D", origin="1950-01-01")

    timestamp_str = dt_obj.strftime("%Y-%m-%d")

    return timestamp_str


def process_sections(ds, global_metadata, sections_lat_lon_metadata):
    num_gridded_sections = ds.dims["gridded_section"]

    # Get a list of all the parameters measured
    params = list(ds.keys())

    # print(f"params in ds are {params}")

    params_values = {param: ds[param].values for param in params}

    # latitude, longitude and pressure are stored in coords vars
    latitude = ds.coords["latitude"].to_numpy()
    longitude = ds.coords["longitude"].to_numpy()
    pressure = ds.coords["pressure"].to_numpy()

    # Round lat and lon to three decimal places
    latitude = np.round(latitude, 3)
    longitude = np.round(longitude, 3)

    # values are lat or lon
    static_direction = global_metadata["static_dir"]
    varying_direction = global_metadata["varying_dir"]

    sections_data = {}

    for section_index in range(num_gridded_sections):
        sections_data[section_index] = {}

        section_metadata = sections_lat_lon_metadata[str(section_index)]

        # ll_grid_direction = section_metadata["varying_direction"]
        ll_grid_direction = varying_direction

        section_expocodes = section_metadata["expocodes"]
        section_time_boundaries = section_metadata["time_boundaries"]

        section_time = ds["time"].values[:, section_index]

        # In the netcdf file, latitude is constant if longitude is varying,
        # and vice versa.
        # In the matlab file, latitude has a wiggle of variation and
        # its value was interpolated at each longitude point using the mat file

        # Use a for loop over the direction that is varying

        # Save lat, lon, time, and parameter values for each loop
        time_values = []
        lat_values = []
        lon_values = []
        params_dfs = []

        if ll_grid_direction == "lon":
            # latitude = section_metadata["lat_grid"]

            # lat_values, lon_values, time_values, params_dfs = process_varying_lon(
            #     section_index,
            #     latitude,
            #     longitude,
            #     section_time,
            #     pressure,
            #     params_values,
            #     section_expocodes,
            #     section_time_boundaries,
            # )

            # TODO
            # Don't need lat_values, lon_values and time_values

            lat_values, lon_values, time_values, params_dfs = process_varying_lon(
                section_index,
                latitude,
                longitude,
                section_time,
                pressure,
                params_values,
            )

        elif ll_grid_direction == "lat":
            # longitude = section_metadata["lon_grid"]

            # TODO
            # Don't need lat_values, lon_values and time_values

            lat_values, lon_values, time_values, params_dfs = process_varying_lat(
                section_index,
                latitude,
                longitude,
                section_time,
                pressure,
                params_values,
            )

        sections_data[section_index]["lat"] = lat_values
        sections_data[section_index]["lon"] = lon_values
        sections_data[section_index]["time"] = time_values

        sections_data[section_index]["section_expocodes"] = section_expocodes
        sections_data[section_index][
            "section_time_boundaries"
        ] = section_time_boundaries

        sections_data[section_index]["params_dfs"] = params_dfs

    return sections_data


def get_woce_line_metadata(woce_line):
    metadata_filename = f"../processed_data/metadata/{woce_line}_metadata.json"

    with open(metadata_filename, "r") as f:
        sections_lat_lon_metadata = json.load(f)

    return sections_lat_lon_metadata


def get_dataset_metadata(ds, woce_line_nc_file):
    global_metadata = {}

    geospatial_lat_min = ds.attrs["geospatial_lat_min"]
    geospatial_lat_max = ds.attrs["geospatial_lat_max"]

    geospatial_lon_min = ds.attrs["geospatial_lon_min"]
    geospatial_lon_max = ds.attrs["geospatial_lon_max"]

    global_metadata["lat_range"] = [geospatial_lat_min, geospatial_lat_max]
    global_metadata["lon_range"] = [geospatial_lon_min, geospatial_lon_max]

    # Find static and varying directions
    if math.isclose(geospatial_lat_min, geospatial_lat_max, rel_tol=1e-5):
        static_direction = "lat"
        varying_direction = "lon"

    elif math.isclose(geospatial_lon_min, geospatial_lon_max, rel_tol=1e-5):
        static_direction = "lon"
        varying_direction = "lat"

    else:
        print("Can't determine static lat and lon directions")
        print(f"Lat range {global_metadata['lat_range']}")
        print(f"Lon range {global_metadata['lon_range']}")

        static_direction = None
        varying_direction = None

    # print(f"lat range {geospatial_lat_min},{geospatial_lat_max}")
    # print(f"lon range {geospatial_lon_min}, {geospatial_lon_max}")

    global_metadata["static_dir"] = static_direction
    global_metadata["varying_dir"] = varying_direction
    global_metadata["date_issued"] = ds.attrs["date_issued"]
    global_metadata["expocodes"] = ds.attrs["expocode"]
    global_metadata["source_url"] = woce_line_nc_file
    global_metadata["source_file"] = Path(woce_line_nc_file).name
    global_metadata["goship_woce_line_id"] = ds.attrs["goship_woce_line_id"]
    global_metadata["all_years_used"] = ds.attrs["all_years_used"]
    global_metadata["instrument"] = ds.attrs["instrument"]
    global_metadata["references"] = ds.attrs["references"]

    return global_metadata


def get_woce_line_dataset(woce_line, nc_files):
    ocean_letter = woce_line[0]

    if woce_line == "75N":
        ocean = "atlantic"
    elif ocean_letter == "P":
        ocean = "pacific"
    elif ocean_letter == "I":
        ocean = "indian"
    elif ocean_letter == "A":
        ocean = "atlantic"
    elif ocean_letter == "S":
        ocean = "southern"
    else:
        ocean = ""
        print(f"Can't find ocean for line {woce_line}")

    woce_line_nc_file = [
        f for f in nc_files if Path(f.lower()).stem.startswith(woce_line.lower())
    ][0]

    base_filename = Path(woce_line_nc_file).name

    r = requests.get(woce_line_nc_file)

    with NamedTemporaryFile() as temp_file:
        temp_file.write(r.content)

        # mat_contents = sio.loadmat(temp_file.name)

        # file_name = f"{woce_line.lower()}.nc"

        # easy_file = f"../easy_ocean_data/gridded/{ocean}/{woce_line}/{file_name}"

        # Get warning when open file because netcdf file not
        # setup correctly for time

        # RuntimeWarning: invalid value encountered in cast
        # flat_num_dates_ns_int = (flat_num_dates * _NS_PER_TIME_DELTA[delta]).astype(
        # ds = xr.open_dataset(temp_file.name)

        # Try without decoding time, then warning goes away
        ds = xr.open_dataset(temp_file.name, decode_times=False)

        # pressure, latitude and longitude are in coords vars

        # But now need to convert the time variable manually

        # https://github.com/pydata/xarray/issues/1662

        time_units = ds["time"].attrs["units"]

        time_units = time_units.replace("days since", "")
        time_units = time_units.replace(" UTC", "")

        def fix_time_nan(day):
            ref = time_units
            td = pd.NaT
            if not np.isnan(day):
                td = pd.Timedelta(days=day)
            return pd.Timestamp(ref) + td

        ds["time"] = xr.apply_ufunc(fix_time_nan, ds["time"], vectorize=True)

        ds = xr.decode_cf(ds)

    return ds, woce_line_nc_file


def process_woce_line(woce_line, all_gridded_netcdf_files):
    ds, woce_line_nc_file = get_woce_line_dataset(woce_line, all_gridded_netcdf_files)

    global_metadata = get_dataset_metadata(ds, woce_line_nc_file)

    # Get metadata of interpolated lat and lon from matlab mat file
    sections_lat_lon_metadata = get_woce_line_metadata(woce_line)

    sections_data = process_sections(ds, global_metadata, sections_lat_lon_metadata)

    extract_data(woce_line, sections_data, global_metadata, sections_lat_lon_metadata)


def main():
    # Delete data dirs first and recreate
    meta_dir = "../processed_data/metadata"
    data_dir = "../processed_data/converted_data"

    if os.path.exists(meta_dir):
        shutil.rmtree(meta_dir)

    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    os.makedirs(meta_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    # Scrape CCHDO easy ocean html page to get the file paths to
    # matlab mat files and netcdf nc files

    (
        all_gridded_matlab_files,
        all_gridded_netcdf_files,
    ) = scrape_easyocean_html_for_files()

    # Create metadata files for all WOCE lines using Matlab data files
    get_all_metadata_from_matlab_files(all_gridded_matlab_files)

    # all_cruises_json = get_active_cruises_json()

    # cruises_expo_id_mapping = get_cruises_expo_id_mapping()

    # active_files_json = get_active_files_json()

    woce_line_lat_lon_variations_file = (
        "easyocean_woce_tracks_latlon_dir.csv"
    )

    df_woce_lines = pd.read_csv(woce_line_lat_lon_variations_file)

    woce_lines = df_woce_lines["woce_line"].values

    # With multiple sections
    # woce_lines = ["P06"]

    # With only one section
    # woce_lines = ['A03']

    # varyling lon
    # woce_lines = ["P01"]  # varying lon
    # woce_lines = ["P09"]  # varyling lat

    # woce_lines = [
    #     "P01",
    #     "P02",
    #     "P03",
    #     "P04",
    #     "P06",
    #     "P09",
    #     "P10",
    #     "P11",
    #     "P13",
    #     "P14",
    #     "P15",
    #     "P16",
    #     "P17",
    #     "P17E",
    #     "P18",
    #     "P21",
    # ]

    # TODO
    # NaN in json instead of null

    count = 0

    for woce_line in woce_lines:
        print(f"processing woce line {woce_line}")

        # process_woce_line(
        #     woce_line, all_cruises_json, active_files_json, cruises_expo_id_mapping
        # )

        process_woce_line(woce_line, all_gridded_netcdf_files)

        # count = count + 1

        # if count == 3:
        #     break


if __name__ == "__main__":
    main()
