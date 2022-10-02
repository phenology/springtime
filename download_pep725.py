"""Download data from the Plant Phenology Data Portal.

This script contains functions to download data from the plant phenology portal
REST API. This portal has three endpoints:

* https://biscicol.org/api/v1/query - An old version (?) returns json objects
* https://biscicol.org/api/v2/ppo - Lookup traits from ontology
* https://biscicol.org/api/v3/download - New version (?) download as zipped csv

The v1 endpoint is not used here because it is a bit inconsistent (e.g. size
instead of limit, planStructurePrensenceTypes instead of termID, etc.). That
might only cause confusion.

Useful resources:

* Paper describing the PPO: https://doi.org/10.3389/fpls.2018.00517
* Data portal frontend: http://plantphenology.org/
* Data portal backend: https://github.com/biocodellc/biscicol-server
* Similar R-package: https://docs.ropensci.org/rppo/reference/rppo-package.html
"""


import zipfile
import requests
import pandas as pd
import tempfile



def get_terms(present=None):
    """Retrieve a list of phenological terms with their PPO codes.

    Args:
        present: If True or False, only include the ppo codes for the presence
            resp. absence of terms. If None (the default), both are provided.

    Returns:
        dict: descriptions of terms with their corresponding PPO codes.

    Notes:
        Similar to
        [rppo.ppo_terms](https://docs.ropensci.org/rppo/articles/rppo-vignette.html#ppo_terms-function)

    Examples:

        List the first 5 terms with their PPO codes >>> terms = get_terms() >>>
        list(terms.items())[:5] [('abscised cones or seeds absent',
        'obo:PPO_0002658'),
         ('abscised cones or seeds present', 'obo:PPO_0002359'), ('abscised
         fruits or seeds absent', 'obo:PPO_0002657'), ('abscised fruits or seeds
         present', 'obo:PPO_0002358'), ('abscised leaves absent',
         'obo:PPO_0002656')]

        List the first 5 PPO codes for absence of terms >>> terms =
        get_terms(present=False) >>> list(terms.items())[:5] [('abscised cones
        or seeds absent', 'obo:PPO_0002658'),
         ('abscised fruits or seeds absent', 'obo:PPO_0002657'), ('abscised
         leaves absent', 'obo:PPO_0002656'), ('breaking leaf buds absent',
         'obo:PPO_0002610'), ('cones absent', 'obo:PPO_0002645')]
    """
    if present is None:
        url = f"https://biscicol.org/api/v2/ppo/all_short"
    elif present is True:
        url = f"https://biscicol.org/api/v2/ppo/present_short"
    elif present is False:
        url = f"https://biscicol.org/api/v2/ppo/absent_short"
    else:
        raise ValueError(f"Present should be None or Boolean, got {present}")

    r = requests.get(url)

    if r.status_code == 200:
        return r.json()

    raise ValueError(f"Request failed with status code {r.status_code}")




def to_dataframe(response):
    sources = [source['_source'] for source in response['hits']['hits']]
    return pd.DataFrame(sources)#[columns]


def download(limit=5, **options):
    """Download data from the plant phenology data portal.

    This function builds a query string from the provided arguments, and uses it
    to fetch data from the REST API into a temporary folder, then unpacks the
    data and loads it into a pandas dataframe.

    Args:
        limit: Maximum number of records to retreive.
        **options: keyword arguments used to filter the data before retreiving
            it from the server.

    Notes:
        Similar to
        [rppo.ppo_data()](https://docs.ropensci.org/rppo/articles/rppo-vignette.html#ppo_data-function)

        The queries build here follow lucene query syntax; see
        https://www.elastic.co/guide/en/kibana/current/lucene-query.html. They
        may look something like this:
        https://biscicol.org/api/v3/download/_search?limit=5&q=genus:Syringa+AND+year:[2000
        TO 2021]

    """
    # Build query string
    base_url = f"https://biscicol.org/api/v3/download/_search?limit={limit}"
    query = "&q=" + "+AND+".join([f"{k}:{v}" for k, v in options.items()])

    # Send friendly message
    print(f"Retrieving data from {base_url + query}")

    # Fetch data
    r = requests.get(base_url + query)

    if r.status_code == 200:

        with tempfile.TemporaryDirectory() as tempdir:

            # Save data to tempfile
            tf = tempdir + '/tempzip.zip'
            with open(tf, 'wb') as f:
                f.write(r.content)

            # Extract data
            file = zipfile.ZipFile(tf)
            file.extractall(path=tempdir)

            # Load data
            df = pd.read_csv(f'{tempdir}/data.csv')

            # Add README to dataframe
            with open(tempdir + '/README.txt', 'r') as f:
                readme = ''.join(f.readlines())
            df.attrs["request_info"] = readme

            # Add license info to dataframe
            with open(tempdir + '/citation_and_data_use_policies.txt', 'r') as f:
                licence = ''.join(f.readlines())
            df.attrs["license"] = licence
            print(readme)

        return df

    if r.status_code == 204:
        # 204 means "no data"
        print("No data found, you may try to broaden your search.")

        # Try to be consistent in what we return
        return pd.DataFrame()

    # If something bad happens, return the response object for debugging purposes.
    print(f"Requests failed with status code {r.status_code}. Please raise an issue.")
    return r



if __name__ == "__main__":
    # get_traits(present=True)
    # get_traits(present=False)
    # df = download(genus="Quercus")
    # df = download(genus="Syringa", source="PEP725")
    # df = download(genus="Syringa", source="PEP725", year="[2000 TO 2021]")
    # df = download(genus="Syringa", source="PEP725", year="[2000 TO 2021]", latitude="[40 TO 70]", longitude="[-10 TO 40]")
    df = download(
        genus="Syringa",
        source="PEP725",
        year="[2000 TO 2021]",
        latitude="[40 TO 70]",
        longitude="[-10 TO 40]",
        termID="\"obo:PPO_0002330\"",  # flowers present
        )

    import IPython; IPython.embed(); quit()

    # TODO: document options (see https://github.com/ropensci/rppo/blob/master/R/ppo_data.R#L6-L29)
    # TODO: write tests
    # TODO: helper utility for formatting termID's
    # TODO: accept multiple inputs for e.g. genus
    # TODO: accept either a single year or a range
    # TODO: utility for formatting ranges
