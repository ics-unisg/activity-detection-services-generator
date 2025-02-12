""" Test signature feature """
from datetime import datetime

import pytest
from cepact.representations import SignatureItem, SignatureBuilder


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_basic():
    """ Test adding general signature items. """
    dt_1 = datetime.fromisoformat("2021-01-01T00:00:00")
    dt_2 = datetime.fromisoformat("2021-01-01T00:01:00")
    signature_builder = SignatureBuilder(activity_name="act1",
                                         annotation_id="ann1",
                                         sampling_freq=2)
    assert len(signature_builder) == 0
    signature_builder.add_signature_item(SignatureItem(
        station="station1",
        timestamp=dt_1,
        sensor="sensor1",
        value=1)
    )
    assert len(signature_builder) == 1
    signature_builder.add_signature_item(SignatureItem(
        station="station1",
        timestamp=dt_1,
        sensor="sensor2",
        value=1)
    )
    signature_builder.add_signature_item(SignatureItem(
        station="station1",
        timestamp=dt_2,
        sensor="sensor1",
        value=1)
    )
    signature_builder.add_signature_item(SignatureItem(
        station="station2",
        timestamp=dt_1,
        sensor="sensor1",
        value=1)
    )
    # make sure an exception is raised if the signature is not complete/consistent
    with pytest.raises(ValueError):
        signature_builder.build()
    signature_builder.add_signature_item(SignatureItem(
        station="station1",
        timestamp=dt_2,
        sensor="sensor2",
        value=1)
    )
    assert len(signature_builder) == 5
    signature = signature_builder.build()
    assert signature.get_sigs_by_ts_station() == {
        dt_1: {
            "station1": [
                SignatureItem(
                    station="station1",
                    timestamp=dt_1,
                    sensor="sensor1",
                    value=1),
                SignatureItem(
                    station="station1",
                    timestamp=dt_1,
                    sensor="sensor2",
                    value=1)
            ],
            "station2": [
                SignatureItem(
                    station="station2",
                    timestamp=dt_1,
                    sensor="sensor1",
                    value=1)
            ]
        },
        dt_2: {
            "station1": [
                SignatureItem(
                    station="station1",
                    timestamp=dt_2,
                    sensor="sensor1",
                    value=1),
                SignatureItem(
                    station="station1",
                    timestamp=dt_2,
                    sensor="sensor2",
                    value=1)
            ]
        }
    }
    assert signature.get_exemplary_datapoints_per_resource() == {
        "station1": {
            "timestamp": dt_1,
            "station": "station1",
            "sensor1": 1,
            "sensor2": 1
        },
        "station2": {
            "timestamp": dt_1,
            "station": "station2",
            "sensor1": 1
        }
    }
    with pytest.raises(AttributeError):
        signature.add_signature_item(SignatureItem(
            station="station1",
            timestamp=dt_1,
            sensor="sensor1",
            value=1)
        )
