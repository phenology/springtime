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


class InvalidRequestError(Exception):
    pass


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

        List the first 5 terms with their PPO codes
        >>> terms = get_terms()
        >>> list(terms.items())[:5]
        [('abscised cones or seeds absent', 'obo:PPO_0002658'),
         ('abscised cones or seeds present', 'obo:PPO_0002359'),
         ('abscised fruits or seeds absent', 'obo:PPO_0002657'),
         ('abscised fruits or seeds present', 'obo:PPO_0002358'),
         ('abscised leaves absent','obo:PPO_0002656')]

        List the first 5 PPO codes for absence of terms
        >>> terms = get_terms(present=False)
        >>> list(terms.items())[:5]
        [('abscised cones or seeds absent', 'obo:PPO_0002658'),
         ('abscised fruits or seeds absent', 'obo:PPO_0002657'),
         ('abscised leaves absent', 'obo:PPO_0002656'),
         ('breaking leaf buds absent', 'obo:PPO_0002610'),
         ('cones absent', 'obo:PPO_0002645')]
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

    raise InvalidRequestError(f"Request failed with status code {r.status_code}")


def download(explode=True, limit=5, timeout=3.05, **options):
    """Download data from the plant phenology data portal.

    This function builds a query string from the provided arguments, and uses it
    to fetch data from the REST API into a temporary folder, then unpacks the
    data and loads it into a pandas dataframe.

    Args:
        limit: Maximum number of records to retreive
        explode: If True, each termID will get its own row
        timeout: Number of seconds to wait for the server to respond
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
    url = _build_url(limit, **options)

    # Wait 3 seconds for a connection, 30 for the response.
    response = requests.get(url, timeout=timeout)

    if response.status_code == 200:
        return _to_dataframe(response, explode)

    if response.status_code == 204:
        print("No data found, you may try to broaden your search.")
        return pd.DataFrame()  # Trying to be consistent in what we return

    raise InvalidRequestError(f"Requests failed with status code {response.status_code}. Please raise an issue.")


# Having this as a separate function makes it much easier to write tests :-)
def _build_url(limit, **options):
    """Parse options to build query string."""
    base_url = f"https://biscicol.org/api/v3/download/_search?limit={limit}"

    if "termID" in options:
        # These need special formatting to retain quotes in URL, otherwise the
        # colon in "obo:PPO_XXXXXXX" messes things up.
        options['termID'] = f"\"{options['termID']}\""

    # Build query string
    query = "&q=" + "+AND+".join([f"{k}:{v}" for k, v in options.items()])

    return base_url + query


def _to_dataframe(response, explode):
    """Extract response body and load as pandas dataframe."""

    with tempfile.TemporaryDirectory() as tempdir:

        # Save data to tempfile
        tf = tempdir + '/tempzip.zip'
        with open(tf, 'wb') as f:
            f.write(response.content)

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

    if explode:
        # Split termID observations into their own rows
        df['termID'] = df['termID'].apply(lambda x: x.split(','))
        df = df.explode('termID').reset_index(drop=True)

    return df


if __name__ == "__main__":
    # Perform some tests
    import pytest

    def test(f):
        """Immediately invoke test function.

        Temporarily used as decorator for real test functions.
        """
        print('---')
        print(f"Testing function {f.__name__}")
        f()
        print('test passed')
        print('---')

    @test
    def get_terms_plain():
        terms = get_terms()
        assert list(terms.items())[:5] == [
            ('abscised cones or seeds absent', 'obo:PPO_0002658'),
            ('abscised cones or seeds present', 'obo:PPO_0002359'),
            ('abscised fruits or seeds absent', 'obo:PPO_0002657'),
            ('abscised fruits or seeds present', 'obo:PPO_0002358'),
            ('abscised leaves absent','obo:PPO_0002656')
        ]

    @test
    def get_terms_absent():
        terms = get_terms(present=False)
        assert list(terms.items())[:5] == [
            ('abscised cones or seeds absent', 'obo:PPO_0002658'),
            ('abscised fruits or seeds absent', 'obo:PPO_0002657'),
            ('abscised leaves absent', 'obo:PPO_0002656'),
            ('breaking leaf buds absent', 'obo:PPO_0002610'),
            ('cones absent', 'obo:PPO_0002645')
        ]

    @test
    def download_passes_simple():
        df = download(genus="Quercus")

    @test
    def download_empty():
        df = download(genus="nonsense")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @test
    def download_timeout():
        with pytest.raises(requests.exceptions.Timeout):
            # this request with bad formatting hangs
            download(genus="[1to2]")

    # df = download(genus="Syringa", source="PEP725")
    # df = download(genus="Syringa", source="PEP725", year="[2000 TO 2021]")
    # df = download(genus="Syringa", source="PEP725", year="[2000 TO 2021]", latitude="[40 TO 70]", longitude="[-10 TO 40]")

    df = download(
        genus="Syringa",
        source="PEP725",
        year="[2000 TO 2021]",
        latitude="[40 TO 70]",
        longitude="[-10 TO 40]",
        termID="obo:PPO_0002330",  # flowers present
        )


    import IPython; IPython.embed(); quit()

    # TODO: document options (see https://github.com/ropensci/rppo/blob/master/R/ppo_data.R#L6-L29)
    # TODO: write tests
    # TODO: accept multiple inputs for options
    # TODO: accept either a single year or a range
    # TODO: utility for formatting ranges
    # TODO: Add functions to convert state to event
