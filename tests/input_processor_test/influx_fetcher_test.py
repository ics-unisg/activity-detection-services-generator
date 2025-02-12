""" Test influx fetcher """
import datetime
from datetime import datetime as dt
from types import SimpleNamespace
from unittest import mock
from unittest.mock import Mock

import pytest
from cepact.input_processor.concrete_fetchers import InfluxFetcher
from cepact.representations import AnnotationParams, Signature


class Record:
    def __init__(self, **kwargs):
        self._data = kwargs  # Store data as dictionary

    def __getitem__(self, key):
        # Enable dictionary-like access
        return self._data.get(key)

    def get_field(self):
        # Define a custom method
        return self._data.get('field')  # Adjust 'field' as needed

    def get_value(self):
        # Define another custom method
        return self._data.get('value')  # Adjust 'value' as needed


record1 = Record(_time=dt.fromisoformat("2022-03-19T00:00:00Z"), field="VDY_accel_x", value=7000)
record2 = Record(_time=dt.fromisoformat("2022-03-19T00:00:00Z"), field="VDY_accel_y", value=7001)
record3 = Record(_time=dt.fromisoformat("2022-03-19T00:00:01Z"), field="VDY_accel_x", value=7000)
record4 = Record(_time=dt.fromisoformat("2022-03-19T00:00:01Z"), field="VDY_accel_y", value=7001)

mock_influx_client = Mock()
mock_query_api = Mock()
mock_influx_client.query_api.return_value = mock_query_api
mock_query_api.query.return_value = [SimpleNamespace(records=[record1]),
                                     SimpleNamespace(records=[record2]),
                                     SimpleNamespace(records=[record3]),
                                     SimpleNamespace(records=[record4])]
mock_influx_client.__enter__ = Mock(return_value=mock_influx_client)
mock_influx_client.__exit__ = Mock(return_value=None)

mock_influx_client2 = Mock()
mock_query_api2 = Mock()
mock_influx_client2.query_api.return_value = mock_query_api2
mock_query_api2.query.return_value = [SimpleNamespace(records=[record1]),
                                      SimpleNamespace(records=[record4])]
mock_influx_client2.__enter__ = Mock(return_value=mock_influx_client2)
mock_influx_client2.__exit__ = Mock(return_value=None)


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_basic():
    """ Test basic functioning. """
    with mock.patch(
            'cepact.input_processor.concrete_fetchers.influx_fetcher.InfluxDBClient',
            return_value=mock_influx_client):
        influx_fetcher = InfluxFetcher(url="http://localhost:8086",
                                       auth="mock",
                                       org="mock",
                                       station_bucket_map={"mock":
                                                               "mock"})
        mock_annotation_params = AnnotationParams(activity_name="mock",
                                                  annotation_id="mock",
                                                  stations=["mock"],
                                                  start=datetime.datetime.now(),
                                                  end=datetime.datetime.now())
        sig: Signature = influx_fetcher.get_signature(mock_annotation_params,
                                                      {"general": []},
                                                      sampling_freq=2)
        assert isinstance(sig, Signature)
        assert len(sig) == 4
        sig_ign_gen: Signature = influx_fetcher.get_signature(mock_annotation_params,
                                                              {"general": ["VDY_accel_x"]},
                                                              sampling_freq=2)
        assert len(sig_ign_gen) == 2
        sig_ign_act: Signature = influx_fetcher.get_signature(
            mock_annotation_params, {
                "general": [],
                "mock": ["VDY_accel_y"]
            },
            sampling_freq=2
        )
        assert len(sig_ign_act) == 2


def test_inconsistent_sensors_at_timestamps_exception():
    """ Test exception handling. """
    with mock.patch(
            'cepact.input_processor.concrete_fetchers.influx_fetcher.InfluxDBClient',
            return_value=mock_influx_client2):
        influx_fetcher = InfluxFetcher(url="http://localhost:8086",
                                       auth="mock",
                                       org="mock",
                                       station_bucket_map={"mock":
                                                               "mock"})
        mock_annotation_params = AnnotationParams(activity_name="mock",
                                                  annotation_id="mock",
                                                  stations=["mock"],
                                                  start=datetime.datetime.now(),
                                                  end=datetime.datetime.now())
        with pytest.raises(ValueError):
            influx_fetcher.get_signature(mock_annotation_params,
                                         {"general": []},
                                         sampling_freq=2)
