import requests
from bs4 import BeautifulSoup
import re


"""
Get the file source path from html of matlab mat and netcdf nc files
from the CCHDO Easy Ocean web page

url: https://cchdo.ucsd.edu/products/goship-easyocean

Looking for the following html

<ul id="tree">
<li>
    gridded
    <ul>
    <li>
    <a class="btn btn-success btn-xs" href="?download=/gridded" type="button">
    Download all gridded
    </a>
    </li>
    <li>
    atlantic
    <ul>
    <li>
        <a class="btn btn-success btn-xs" href="?download=/gridded/atlantic" type="button">
        Download all atlantic
        </a>
    </li>
    <li>
        75N
        <ul>
        <li>
        <a class="btn btn-success btn-xs" href="?download=/gridded/atlantic/75N" type="button">
        Download all 75N
        </a>
        </li>
        <li>
        <a href="/data/38163/75n.bin">
        <span aria-hidden="true" class="glyphicon glyphicon-download">
        </span>
        75n.bin
        </a>
        </li>
        <li>
        <a href="/data/16610/75n.bin.ctl">
        <span aria-hidden="true" class="glyphicon glyphicon-download">
        </span>
        75n.bin.ctl
        </a>
        </li>
        <li>
        <a href="/data/38124/75n.mat">
        <span aria-hidden="true" class="glyphicon glyphicon-download">
        </span>
        75n.mat
        </a>
        </li>
        <li>
        <a href="/data/38218/75n.nc">
        <span aria-hidden="true" class="glyphicon glyphicon-download">
        </span>
        75n.nc
        </a>
        </li>

"""


def scrape_easyocean_html_for_files():
    print("scrapping CCHDO easyocean data page")

    url = "https://cchdo.ucsd.edu/products/goship-easyocean"
    r = requests.get(url)

    soup = BeautifulSoup(r.content, "html5lib")

    ul_tree = soup.find("ul", {"id": "tree"})

    gridded_li = None
    for li in ul_tree.select("li"):
        if li.contents[0].strip() == "gridded":
            gridded_li = li
            break

    # Find all
    # <li><a href="/data/38124/75n.mat"><span class="glyphicon glyphicon-download" aria-hidden="true"></span> 75n.mat</a></li>

    all_gridded_matlab_files = []
    all_gridded_netcdf_files = []

    ul = gridded_li.select("ul")[0]

    for a in ul.find_all(name="a", href=True):
        href = str(a["href"])

        file_mat = re.findall(r"/data/.*/.*.mat", href)

        if file_mat:
            url = f"https://cchdo.ucsd.edu{file_mat[0]}"
            all_gridded_matlab_files.append(url)

        file_netcdf = re.findall(r"/data/.*/.*.nc", href)

        if file_netcdf:
            url = f"https://cchdo.ucsd.edu{file_netcdf[0]}"
            all_gridded_netcdf_files.append(url)

    return all_gridded_matlab_files, all_gridded_netcdf_files
