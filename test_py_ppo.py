import pytest
from py_ppo import get_terms, download
import numpy as np
import pandas as pd
import requests

class TestGetTerms:

    def test_get_terms_plain(self):
        terms = get_terms()
        assert list(terms.items())[:5] == [
            ('abscised cones or seeds absent', 'obo:PPO_0002658'),
            ('abscised cones or seeds present', 'obo:PPO_0002359'),
            ('abscised fruits or seeds absent', 'obo:PPO_0002657'),
            ('abscised fruits or seeds present', 'obo:PPO_0002358'),
            ('abscised leaves absent','obo:PPO_0002656')
        ]

    def test_get_terms_absent(self):
        terms = get_terms(present=False)
        assert list(terms.items())[:5] == [
            ('abscised cones or seeds absent', 'obo:PPO_0002658'),
            ('abscised fruits or seeds absent', 'obo:PPO_0002657'),
            ('abscised leaves absent', 'obo:PPO_0002656'),
            ('breaking leaf buds absent', 'obo:PPO_0002610'),
            ('cones absent', 'obo:PPO_0002645')
        ]


class TestDownload:

    def test_download_passes(self):
        download(genus="Quercus")

    def test_download_empty(self):
        df = download(genus="nonsense")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_filter_genus(self):
        df = download(genus = "Syringa")
        assert np.all(df.genus == "Syringa")

    def test_filter_source(self):
        df = download(source = "PEP725")
        assert np.all(df.source == "PEP725")

    def test_filter_year(self):
        df = download(year = "[2000 TO 2021]")
        assert np.all(df.year >= 2000)
        assert np.all(df.year <= 2021)

    def test_filter_latitude(self):
        df = download(latitude="[40 TO 70]")
        assert np.all(df.latitude >= 40)
        assert np.all(df.latitude <= 70)

    def test_filter_longitude(self):
        df = download(longitude="[-10 TO 40]")
        assert np.all(df.longitude >= -10)
        assert np.all(df.longitude <= 40)

    def test_filter_termID(self):
        df = download(termID="obo:PPO_0002313")
        assert np.all(df.termID.str.contains("obo:PPO_0002313"))

    def test_download_timeout(self):
        with pytest.raises(requests.exceptions.Timeout):
            # this request with bad formatting hangs
            download(genus="[1to2]")
