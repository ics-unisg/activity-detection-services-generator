""" Test changes feature """
from datetime import datetime

import pytest
from cepact.representations import (SignatureItem, SignatureBuilder,
                                               DiscretizationBuilder, Changes)


def add_many(signature_builder, sensor: str, times: list, values: list):
    """ Add many signature items to the signature builder """
    assert len(times) == len(values)
    for time, value in zip(times, values):
        signature_builder.add_signature_item(SignatureItem(
            station="station1",
            timestamp=time,
            sensor=sensor,
            value=value)
        )


mock_signature_builder = SignatureBuilder(activity_name="paperact",
                                          annotation_id="ann1",
                                          sampling_freq=2)
dt0 = datetime.fromisoformat("2021-01-01T00:00:00")
dt1 = datetime.fromisoformat("2021-01-01T00:01:00")
dt2 = datetime.fromisoformat("2021-01-01T00:02:00")
dt3 = datetime.fromisoformat("2021-01-01T00:03:00")
dt4 = datetime.fromisoformat("2021-01-01T00:04:00")
add_many(mock_signature_builder, "i1_pos_switch", [dt0, dt1, dt2, dt3, dt4], [0, 0, 1, 1, 1])
add_many(mock_signature_builder, "i2_pos_switch", [dt0, dt1, dt2, dt3, dt4], [0, 0, 0, 0, 0])
add_many(mock_signature_builder, "i5_light_barrier", [dt0, dt1, dt2, dt3, dt4], [1, 1, 1, 1, 1])
add_many(mock_signature_builder, "m1_speed", [dt0, dt1, dt2, dt3, dt4], [0, -512, 0, 0, 0])
add_many(mock_signature_builder, "o7_valve", [dt0, dt1, dt2, dt3, dt4], [0, 512, 512, 512, 0])
add_many(mock_signature_builder, "o8_compressor", [dt0, dt1, dt2, dt3, dt4], [0, 0, 0, 0, 0])
mock_signature = mock_signature_builder.build()
add_many(mock_signature_builder, "temp_sens",
         [dt0, dt1, dt2, dt3, dt4], [1.4, 1.2, 7.9, 10.2, 20.1])
mock_signature2 = mock_signature_builder.build()
mock_discretization_builder = DiscretizationBuilder()
mock_discretization_builder.add_discretization_item(sensor="temp_sens", beg=float("-inf"), to=9,
                                                    beg_incl=True, to_incl=True, target_value="low")
mock_discretization_builder.add_discretization_item(sensor="temp_sens", beg=9, to=float("inf"),
                                                    beg_incl=False, to_incl=True, target_value="high")
mock_discretization = mock_discretization_builder.build()


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_basic():
    """ Test if changes works generally. """
    changes = Changes([mock_signature], mock_discretization)
    assert len(changes) == 5
    assert changes.timestamps == [dt1, dt2, dt4]
    for change in changes.changes:
        assert change.station == "station1"
    changes_dt1 = changes.get_changes_at_ts_station(dt1, "station1")
    assert len(changes_dt1) == 2
    change_dt1_o7_valve = [change for change in changes_dt1 if change.sensor == "o7_valve"]
    assert len(change_dt1_o7_valve) == 1
    assert change_dt1_o7_valve[0].prev_value == 0
    assert change_dt1_o7_valve[0].value == 512
    assert change_dt1_o7_valve[0].station == "station1"
    assert change_dt1_o7_valve[0].sensor == "o7_valve"
    assert change_dt1_o7_valve[0].timestamp == dt1
    change_dt1_m1_speed = [change for change in changes_dt1 if change.sensor == "m1_speed"]
    assert len(change_dt1_m1_speed) == 1
    assert change_dt1_m1_speed[0].prev_value == 0
    assert change_dt1_m1_speed[0].value == -512
    assert change_dt1_m1_speed[0].timestamp == dt1
    assert change_dt1_m1_speed[0].station == "station1"
    assert change_dt1_m1_speed[0].sensor == "m1_speed"
    changes_dt2 = changes.get_changes_at_ts_station(dt2, "station1")
    assert len(changes_dt2) == 2
    change_dt2_i1_pos_switch = [change for change in changes_dt2 if
                                change.sensor == "i1_pos_switch"]
    assert len(change_dt2_i1_pos_switch) == 1
    assert change_dt2_i1_pos_switch[0].prev_value == 0
    assert change_dt2_i1_pos_switch[0].value == 1
    assert change_dt2_i1_pos_switch[0].timestamp == dt2
    assert change_dt2_i1_pos_switch[0].station == "station1"
    assert change_dt2_i1_pos_switch[0].sensor == "i1_pos_switch"
    change_dt2_m1_speed = [change for change in changes_dt2 if change.sensor == "m1_speed"]
    assert len(change_dt2_m1_speed) == 1
    assert change_dt2_m1_speed[0].prev_value == -512
    assert change_dt2_m1_speed[0].value == 0
    assert change_dt2_m1_speed[0].timestamp == dt2
    assert change_dt2_m1_speed[0].station == "station1"
    assert change_dt2_m1_speed[0].sensor == "m1_speed"
    changes_dt4 = changes.get_changes_at_ts_station(dt4, "station1")
    assert len(changes_dt4) == 1
    change_dt4_o7_valve = [change for change in changes_dt4 if change.sensor == "o7_valve"]
    assert len(change_dt4_o7_valve) == 1
    assert change_dt4_o7_valve[0].prev_value == 512
    assert change_dt4_o7_valve[0].value == 0
    assert change_dt4_o7_valve[0].timestamp == dt4
    assert change_dt4_o7_valve[0].station == "station1"
    assert change_dt4_o7_valve[0].sensor == "o7_valve"


def test_with_discr():
    """ Test if changes works with discretization. """
    changes = Changes([mock_signature2], mock_discretization)
    assert len(changes) == 6
    assert changes.timestamps == [dt1, dt2, dt3, dt4]
    changes_dt3 = changes.get_changes_at_ts_station(dt3, "station1")
    assert len(changes_dt3) == 1
    change_dt3_temp_sens = [change for change in changes_dt3 if change.sensor == "temp_sens"]
    assert len(change_dt3_temp_sens) == 1
    assert change_dt3_temp_sens[0].prev_value == "low"
    assert change_dt3_temp_sens[0].value == "high"
    assert change_dt3_temp_sens[0].timestamp == dt3
    assert change_dt3_temp_sens[0].station == "station1"
    assert change_dt3_temp_sens[0].sensor == "temp_sens"
