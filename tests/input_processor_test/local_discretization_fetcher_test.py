""" Test local input fetcher """
from unittest import mock
from unittest.mock import Mock

import pandas as pd
import pytest
from cepact.input_processor.concrete_fetchers import LocalDiscretizationFetcher
from cepact.representations import Discretization

response_mock = pd.read_csv('tests/input_processor_test/mocks/hc_discr.csv')
mock_stat = Mock()
mock_stat.st_size = 1


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_basic():
    """ Test basic fucntioning. """
    with mock.patch('pandas.read_csv', return_value=response_mock), \
            mock.patch('os.path.exists', return_value=True), \
            mock.patch('os.stat', return_value=mock_stat):
        fetcher = LocalDiscretizationFetcher(local_in_dir="mocks")
        disc = fetcher.get_discretization()
        assert isinstance(disc, Discretization)
        assert disc.discretize("VDY_accel_x", 7000) == "baseline"
