# argovis/easyocean:dev

FROM python:3.9
RUN apt-get update; apt-get install -y nano
RUN pip install requests pandas numpy xarray bs4 scipy matplotlib html5lib netcdf4 pymongo geopy
WORKDIR /app
