import requests
import httplib2
from bs4 import BeautifulSoup, SoupStrainer

def get_gaia_csv_urls(url='https://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/'):
    """
    This function parses the source url 'https://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/'
    for .csv.gz files and returns them as a list.

    :param url: the source url from where the Gaia data needs to be downloaded
    :return: list object with file names
    """

    try:
        http = httplib2.Http()
        status, response = http.request(url)
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
    csv_files = []
    for link in BeautifulSoup(response, parse_only=SoupStrainer('a'), features="html.parser"):
        if link.has_attr('href') and link['href'].endswith('.csv.gz'):
            abs_path = url + link['href']
            csv_files.append(abs_path)

    return csv_files
