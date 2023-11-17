# Create metadata JSON files from Easy Ocean gridded matlab mat files

# matlab lon is from 0 to 360
#
# and then netcdf is from -180 to 180
#

# Only need to find expocodes from Matlab files
# and time boundaries to see how the sections are chosen

from pathlib import Path
import requests
from tempfile import NamedTemporaryFile
import scipy.io as sio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime
from datetime import timedelta
import json

# Location of files

# file_name = f"{line.lower()}.mat"

# easy_file = f'../easy_ocean_data/gridded/pacific/{line}/{file_name}'

# woce_line_lat_lon_variations_file = '../easyocean_lat_lon_tracks/easyocean_woce_tracks_latlon_dir.csv'

# output_filename = f'../processed_data/metadata/{line}_metadata.json'


def convert_matlab_time(timestamp):
    # 719529 is the datenum value of the Unix epoch start (1970-01-01)
    converted_timestamp = pd.to_datetime(timestamp - 719529, unit="D")

    return converted_timestamp


def datenum_to_datetime(datenum):
    """
    Convert Matlab datenum into Python datetime.
    :param datenum: Date in datenum format
    :return:        Datetime object corresponding to datenum.
    """

    # https://stackoverflow.com/questions/13965740/converting-matlabs-datenum-format-to-python

    if math.isnan(datenum):
        converted_datenum = np.NaN
    else:
        converted_datenum = (
            datetime.fromordinal(int(datenum))
            + timedelta(days=datenum % 1)
            - timedelta(days=366)
        )

    return converted_datenum


# Function to create matlab datenum times to python timestamps
def convert_matlab_time_section_grids(time_section_grids):
    num_section_grids = len(time_section_grids)

    timestamps_section_grids = []

    for section_index in range(num_section_grids):
        # times are embedded in an outer array, use first index of outer array
        time_section_grid = time_section_grids[section_index][0]

        df = pd.DataFrame(time_section_grid, columns=["matlab_datenum"])

        df["python_datetime"] = df["matlab_datenum"].apply(datenum_to_datetime)

        timestamps_array = df["python_datetime"].to_numpy()

        timestamps_section_grids.append(timestamps_array)

    return timestamps_section_grids


def create_sections_metadata(
    ll_grid_direction, sections_lon_lat_grid, section_expocodes, section_time_boundaries
):
    all_sections_metadata = {}

    for i, section_lon_lat_grid in sections_lon_lat_grid.items():
        section_metadata = {}

        section_metadata["varying_direction"] = ll_grid_direction

        # grid direction is the direction that is varying
        # return the direction values that are not varyling
        if ll_grid_direction == "lat":
            section_metadata["static_direction"] = "lon"

        elif ll_grid_direction == "lon":
            section_metadata["static_direction"] = "lat"

        # # Matlab data files range from 0 to 360
        # lon = section_lon_lat_grid["lon"]

        # lon_np = np.array(lon)
        # lon_adjusted = np.where(lon_np > 180, lon_np - 360, lon_np)

        # lon_adjusted = lon_adjusted.tolist()

        # section_metadata["lon_grid"] = lon_adjusted
        # section_metadata["lat_grid"] = section_lon_lat_grid["lat"]

        all_sections_metadata[i] = section_metadata

    for i, expocodes in section_expocodes.items():
        all_sections_metadata[i]["expocodes"] = expocodes

    for i, time_boundaries in section_time_boundaries.items():
        all_sections_metadata[i]["time_boundaries"] = time_boundaries

    return all_sections_metadata


# Get time range of each section and expocodes used
def get_meta_data(stations_section_dfs, timestamps_section_grids):
    section_time_boundaries = {}
    section_expocodes = {}

    for i, stations_section_df in enumerate(stations_section_dfs):
        section_timestamps = timestamps_section_grids[i]

        timestamps_array = np.array(section_timestamps)

        # Remove NaT entries to get the max and min
        timestamps_array = [val for val in timestamps_array if not np.isnat(val)]

        min_timestamp = np.min(timestamps_array)
        max_timestamp = np.max(timestamps_array)

        # print(f'min and max times are {min_timestamp} to {max_timestamp}')

        # Get year, month, day from timestamp
        dt_obj = pd.to_datetime(str(min_timestamp), format="ISO8601")
        min_timestamp_str = dt_obj.strftime("%Y-%m-%d")

        dt_obj = pd.to_datetime(str(max_timestamp), format="ISO8601")
        max_timestamp_str = dt_obj.strftime("%Y-%m-%d")

        section_time_boundaries[i] = [min_timestamp_str, max_timestamp_str]

        expocodes = stations_section_df["EXPO"].to_list()

        unique_expocodes = list(set(expocodes))

        section_expocodes[i] = unique_expocodes

        print(unique_expocodes)

    return section_expocodes, section_time_boundaries


# Put stations data into a pandas dataframe


# Store all grid station dataframes into one array


def store_stations_meta_in_dataframes(stations_section_grids):
    stations_section_dfs = []

    # Each station is an array of arrays from reading in the data from
    # a matlab mat file. Reading in the mat file embeds values within
    # arrays, so you need to keep getting the first element to find the
    # array values

    # Matlab Fields from D_pr.Station
    # {'EXPO'      }
    # {'Stnnbr'    }
    # {'Cast'      }
    # {'Lat'       }
    # {'Lon'       }
    # {'Time'      }
    # {'Depth'     }
    # {'CTDtemUnit'}
    # {'CTDsalUnit'}
    # {'CTDoxyUnit'}

    # CTDtemUnit = ITS-90 and IPTS-68 and ITS-68 and DEG_C
    # CTDsalUnit = PSS-78
    # CTDoxyUnit = UMOL/KG

    # But the gridded temperature, salinity and oxygen
    # are all on the same scale and units
    # See the netcdf file

    # If more than one expocode in section, it could be they are
    # from one year. How do they divide up the sections?

    for stations_section_grid in stations_section_grids:
        # Get the column names from one entry
        # Each array is a numpy array, so use dtype.names to get station names
        # from one array
        station_columns = stations_section_grid[0][0].dtype.names

        data = {}

        for col in station_columns:
            values = []

            for row in stations_section_grid:
                try:
                    value = row[0][col][0][0][0]
                except:
                    value = row[0][col][0][0]

                if isinstance(value, np.ndarray) and len(value):
                    values.append(value[0])

                else:
                    values.append(value)

            data[col] = values

        df = pd.DataFrame.from_dict(data)

        df.reset_index(inplace=True)
        df = df.rename(columns={"index": "station"})

        # Remove rows that are all nan
        df = df.dropna(how="all")

        # Remove columns that are all nan
        df = df.dropna(how="all", axis="columns")

        # columns are
        # 'station', 'EXPO', 'Stnnbr', 'Cast', 'Lat', 'Lon', 'Time', 'Depth',
        # 'CTDtemUnit', 'CTDsalUnit', 'CTDoxyUnit'

        # print("temp units")
        # print(df["CTDtemUnit"].head())

        # print("sal units")
        # print(df["CTDsalUnit"].head())

        # print("oxy units")
        # print(df["CTDoxyUnit"].head())

        stations_section_dfs.append(df)

    return stations_section_dfs


# Create a gridded lat/lon variable in relation to the varying direction of the ll_grid variable
# From easyocean github, get lat and lon grid from ll_grid
#
# https://github.com/kkats/GO-SHIP-Easy-Ocean
#
# It is a matlab function to get the lon_grid
#
# When have ll_grid containing latitude, use this
#
# (hinterp_bylat.m) lon_grid = interp1(lat, lon, lat_grid, 'linear');
#
# (hinterp_bylon.m) lat_grid = interp1(lon, lat, lon_grid, 'linear');
#


def get_lon_lat_grid(stations_section_dfs, ll_grid, ll_grid_direction):
    # Get the interpolated lat/lon for the corresponding varying direction
    # because that is not given in the metadata of the matlab mat file

    # Set precision of lat and lon in the station dfs to 1 decimal point

    sections_lon_lat_grid = {}

    for i, stations_section_df in enumerate(stations_section_dfs):
        # Get the lat and lon from the stations meta data (lat and lon before gridding)
        lat = stations_section_df["Lat"].to_numpy(dtype="float64")
        lon = stations_section_df["Lon"].to_numpy(dtype="float64")

        if ll_grid_direction == "lat":
            lon_interp = np.interp(ll_grid, lat, lon)

            # limit to one decimal place
            lon_grid = np.around(lon_interp, decimals=1)

            # Change to one decimal place which is the grid size
            lat_grid = np.around(ll_grid, decimals=1)

            sections_lon_lat_grid[i] = {}

            sections_lon_lat_grid[i]["lat"] = lat_grid.tolist()
            sections_lon_lat_grid[i]["lon"] = lon_grid.tolist()

        elif ll_grid_direction == "lon":
            lat_interp = np.interp(ll_grid, lon, lat)

            # limit to one decimal place
            lat_grid = np.around(lat_interp, decimals=1)

            # Change to one decimal place which is the grid size
            lon_grid = np.around(ll_grid, decimals=1)

            sections_lon_lat_grid[i] = {}

            sections_lon_lat_grid[i]["lat"] = lat_grid.tolist()
            sections_lon_lat_grid[i]["lon"] = lon_grid.tolist()

    return sections_lon_lat_grid


# Function to get file variables
#
# Variables are stored in stations grids (4 of them)
#
# And the pr_grid and ll_grid are common to all the stations grids


# Function to get file variables
def get_file_variables(easy_file):
    # From matlab, variables from mat file are
    # D_pr, ll_grid, pr_grid

    # Fields of D_pr struct are
    # {'Station'}
    # {'NTime'  }
    # {'CTDtem' }
    # {'CTDsal' }
    # {'CTDoxy' }
    # {'CTDCT'  }
    # {'CTDSA'  }

    # Fields from D_pr.Station
    # {'EXPO'      }
    # {'Stnnbr'    }
    # {'Cast'      }
    # {'Lat'       }
    # {'Lon'       }
    # {'Time'      }
    # {'Depth'     }
    # {'CTDtemUnit'}
    # {'CTDsalUnit'}
    # {'CTDoxyUnit'}

    # D_pr struct is broken down into sub sections I call grids
    # Print out the NTime values min and max to see how years and
    # expocodes are divided up into grids

    r = requests.get(easy_file)

    with NamedTemporaryFile() as temp_file:
        temp_file.write(r.content)

        mat_contents = sio.loadmat(temp_file.name)

    mdata = mat_contents["D_pr"]  # variable in mat file

    # The data is split into grids (the number varies depending
    # on the WOCE line) which I'll call section grids.

    # There is Stations meta data for each section.

    # There is also gridded Time data and the variables data
    # for each section.

    # The degrees grid of latitude or longitude (ll_grid) and the
    # pressure grid (pr_grid) are the same for all section grids.

    # The ll_grid and pr_grid are each embedded in a single element array
    # so to get the one dimensional values, access the first element
    # of the outer array.

    # Station data is the metadata for the file
    # It is limited to the actual lon, lat locations
    # before the data is gridded

    # This Station variable holds stations meta data for all section grids.
    # The stations meta contains the following variables:
    # 'EXPO','Stnnbr','Cast','Lat','Lon','Time','Depth','CTDtemUnit','CTDsalUnit','CTDoxyUnit')

    # Get first element because it is embedded in a single element array
    stations_section_grids = mdata["Station"][0]

    # To get the info for one section grid, call the two dimensional array
    # where the index is the section grid.
    # e.g., 1st section grid is stations_section_grids[0]

    # Get first element because it is embedded in a single element array
    time_section_grids = mdata["NTime"][0]

    timestamps_section_grids = convert_matlab_time_section_grids(time_section_grids)

    # Get the first element of the outer array holding the
    # one dimensional arrays of pr_grid (pressure) and
    # ll_grid (longitude or latitude depending on the WOCE line)
    pr_grid = mat_contents["pr_grid"][0]

    ll_grid = mat_contents["ll_grid"][0]

    return mdata, stations_section_grids, timestamps_section_grids, pr_grid, ll_grid


def create_metadata(ll_grid_direction, easy_file):
    (
        mdata,
        stations_section_grids,
        timestamps_section_grids,
        pr_grid,
        ll_grid,
    ) = get_file_variables(easy_file)

    stations_section_dfs = store_stations_meta_in_dataframes(stations_section_grids)

    sections_lon_lat_grid = get_lon_lat_grid(
        stations_section_dfs, ll_grid, ll_grid_direction
    )

    section_expocodes, section_time_boundaries = get_meta_data(
        stations_section_dfs, timestamps_section_grids
    )

    sections_metadata = create_sections_metadata(
        ll_grid_direction,
        sections_lon_lat_grid,
        section_expocodes,
        section_time_boundaries,
    )

    return sections_metadata


# Start processing files (using Matlab data)
def get_all_metadata_from_matlab_files(mat_files):
    # woce_line_lat_lon_variations_file = (
    #     "../easyocean_lat_lon_tracks/easyocean_woce_tracks_latlon_dir.csv"
    # )

    woce_line_lat_lon_variations_file = "easyocean_woce_tracks_latlon_dir.csv"

    df_woce_lines = pd.read_csv(woce_line_lat_lon_variations_file)

    woce_lines = df_woce_lines["woce_line"].values

    # woce_lines = ["P01"]  # varying lon direction

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

    count = 0

    for woce_line in woce_lines:
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

        # Filter all mat files for woce_line subset
        # example mat file path https://cchdo.ucsd.edu/data/38124/75n.mat
        # and https://cchdo.ucsd.edu/data/37913/a03.mat
        woce_line_mat_file = [
            f for f in mat_files if Path(f.lower()).stem.startswith(woce_line.lower())
        ][0]

        base_filename = Path(woce_line_mat_file).name

        output_filename = f"../processed_data/metadata/{woce_line}_metadata.json"

        print(f"Getting metadata for line {woce_line}")
        # (df_woce_lines['woce_line'])

        # Determine if meridonal (NS) or zonal (EW) is varyling
        # ll_grid_direction is the lat or lon direction that it is varying
        ll_grid_direction_row = df_woce_lines[df_woce_lines["woce_line"] == woce_line]

        ll_grid_direction = ll_grid_direction_row["lat_lon_varying_dir"].values[0]

        # Get metadata which also includes getting the values of the
        # lat or lon direction that isn't varying
        sections_metadata = create_metadata(ll_grid_direction, woce_line_mat_file)

        # write metadata to file
        with open(output_filename, "w") as f:
            f.write(json.dumps(sections_metadata, indent=4))

        # count = count + 1

        # if count == 3:
        #     break
