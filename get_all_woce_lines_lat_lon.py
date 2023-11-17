# From each NetCDF easy ocean file, get the latitude and longitude values
# for all sections

from pathlib import Path
import requests
from tempfile import NamedTemporaryFile
import pandas as pd
import numpy as np
import xarray as xr


from scrape_easyocean_html_for_files import scrape_easyocean_html_for_files


def get_woce_line_dataset(woce_line, nc_files):
    woce_line_nc_file = [
        f for f in nc_files if Path(f.lower()).stem.startswith(woce_line.lower())
    ][0]

    r = requests.get(woce_line_nc_file)

    with NamedTemporaryFile() as temp_file:
        temp_file.write(r.content)

        # Try without decoding time, then warning goes away
        ds = xr.open_dataset(temp_file.name, decode_times=False)

        longitude = ds.coords["longitude"].to_numpy()
        latitude = ds.coords["latitude"].to_numpy()

        num_lon = longitude.size
        num_lat = latitude.size

        if num_lon == 1:
            longitude = np.repeat(longitude, num_lat)
        else:
            latitude = np.repeat(latitude, num_lon)

        # round to 1 decimal place
        df_lon_lat = pd.DataFrame(
            {
                "longitude": np.round(longitude, decimals=1),
                "latitude": np.round(latitude, decimals=1),
            }
        )

        output_dir = "../processed_data/woce_lines_lat_lon"
        file_out = f"{output_dir}/{woce_line}_lat_lon.csv"

        df_lon_lat.round(1).to_csv(file_out, index=False)


def process_woce_line(woce_line, all_gridded_netcdf_files):
    get_woce_line_dataset(woce_line, all_gridded_netcdf_files)


def main():
    (
        all_gridded_matlab_files,
        all_gridded_netcdf_files,
    ) = scrape_easyocean_html_for_files()

    woce_line_lat_lon_variations_file = (
        "../easyocean_lat_lon_tracks/easyocean_woce_tracks_latlon_dir.csv"
    )

    df_woce_lines = pd.read_csv(woce_line_lat_lon_variations_file)

    woce_lines = df_woce_lines["woce_line"].values

    for woce_line in woce_lines:
        print(f"processing woce line {woce_line}")

        process_woce_line(woce_line, all_gridded_netcdf_files)


if __name__ == "__main__":
    main()
