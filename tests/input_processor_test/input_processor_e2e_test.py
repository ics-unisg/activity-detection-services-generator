import json
import os
from datetime import datetime

import pandas as pd
import pytest
from cepact import (DAGConfig, SiddhiConfig, LocalSignatureFetcher,
                               LocalAnnotationParamFetcher, LocalIgnoreSensorFetcher,
                               LocalDiscretizationFetcher, AllHighLevelPatternQuery,
                               First50HighLevelPatternQuery, FirstLastLowLevelPatternQuery,
                               Any25LowLevelPatternQuery, Any50LowLevelPatternQuery,
                               Any75LowLevelPatternQuery)
from cepact.input_processor import InputProcessor

test_path = os.path.join("tests", "local_input_mocks")
test_cases = [f for f in os.listdir(test_path) if os.path.isdir(os.path.join(test_path, f))]
# Define the format
format_str = '%Y-%m-%d %H:%M:%S.%f'


# Convert to datetime object

@pytest.mark.parametrize("testcase_dir", test_cases)
def test_activites_signature_changes(testcase_dir):
    """ Test basic functioning. """
    local_in_dir = os.path.join(test_path, testcase_dir, "in")
    mock_siddhi_conf = SiddhiConfig(mqtt_url="mock",
                                    mqtt_user="mock",
                                    mqtt_pwd="mock",
                                    topic_prefix="mock",
                                    map_sensor_name_data_to_mqtt={"mock": "mock"})
    mock_dag_conf = DAGConfig(det_methods=[AllHighLevelPatternQuery(),
                                           First50HighLevelPatternQuery(),
                                           FirstLastLowLevelPatternQuery(),
                                           Any25LowLevelPatternQuery(),
                                           Any50LowLevelPatternQuery(),
                                           Any75LowLevelPatternQuery()],
                              out_dir="mock",
                              sampling_freq=2,
                              siddhi_config=mock_siddhi_conf,
                              signature_fetcher=LocalSignatureFetcher(local_in_dir),
                              annotation_param_fetcher=LocalAnnotationParamFetcher(local_in_dir),
                              ignore_sensor_fetcher=LocalIgnoreSensorFetcher(local_in_dir),
                              discretization_fetcher=LocalDiscretizationFetcher(local_in_dir))
    # run the input processor
    input_processor = InputProcessor(mock_dag_conf)
    activities = input_processor.get_activities()
    discretization = input_processor.get_discretization()
    # check if correct activities are returned
    # get all activity names from directory names in "test_path + testcase_dir + assert"
    activities_to_check = [f for f in os.listdir(os.path.join(test_path, testcase_dir, "assert"))
                           if os.path.isdir(os.path.join(test_path, testcase_dir, "assert", f))]
    assert len(activities) == len(activities_to_check)
    for act_ass_n in activities_to_check:
        # get correct activity based on name
        acts_test = [a for a in activities if a.get_annotation_params().activity_name == act_ass_n]
        assert len(acts_test) == 1
        act_test = acts_test[0]
        # check if the changes are correct
        changes = act_test.get_changes(discretization)
        changes_counter = 0
        with open(os.path.join(test_path, testcase_dir, "assert", act_ass_n, "changes.jsonl"), "r",
                  encoding="utf-8") as chgs_file:
            for line in chgs_file:
                rec = json.loads(line)
                timestamp = datetime.strptime(rec["timestamp"], format_str)
                assert {ci.sensor for ci in changes.get_changes_at_ts(timestamp)} == {ca["sensor"]
                                                                                      for ca in rec[
                                                                                          "changes"]}
                for change in rec["changes"]:
                    changes_counter += 1
                    rel_chs = changes.get_changes_at_ts_station(timestamp, change["station"])
                    rel_chs_2 = [c for c in rel_chs if c.sensor == change["sensor"]]
                    assert len(rel_chs_2) == 1
                    rel_ch = rel_chs_2[0]
                    assert rel_ch.station == change["station"]
                    assert rel_ch.sensor == change["sensor"]
                    assert rel_ch.value == change["value"]
                    assert rel_ch.prev_value == change["prev_value"]
        assert changes_counter == len(changes.changes)

    # get all unique activity_name annotation_id combos
    anno_params = pd.read_csv(os.path.join(test_path, testcase_dir, "in", "anno_params.csv"))
    unique_activity_annotation_ids = set(
        anno_params.apply(lambda x: (x["activity_name"], x["annotation_id"]), axis=1))
    for activity_name, annotation_id in unique_activity_annotation_ids:
        relevant_activities = [a for a in activities if
                               a.get_annotation_params().activity_name == activity_name]
        assert len(relevant_activities) == 1
        curr_act = relevant_activities[0]
        # check if the signatures are correct
        signature_all = curr_act.get_signature()
        signature_filtered = [sig for sig in signature_all if sig.annotation_id == annotation_id]
        if len(signature_filtered) == 0:
            raise ValueError(
                "No signature found for activity_name: " + activity_name + " and annotation_id: " + annotation_id)
        elif len(signature_filtered) > 1:
            raise ValueError(
                "Multiple signatures found for activity_name: " + activity_name + " and annotation_id: " + annotation_id)
        signature_ts_rs = signature_filtered[0].get_sigs_by_ts_station()
        line_counter = 0
        with open(os.path.join(test_path, testcase_dir, "assert", activity_name, annotation_id,
                               "signature.jsonl"), "r",
                  encoding="utf-8") as sig_file:
            for line in sig_file:
                line_counter += 1
                rec = json.loads(line)

                timestamp = datetime.strptime(rec["timestamp"], format_str)
                for measurement in rec["measurements"]:
                    station = measurement["station"]
                    current_sigs = signature_ts_rs[timestamp][station]
                    assert {"station"} | {sig.sensor for sig in current_sigs} == set(
                        measurement.keys())
                    for field, value in measurement.items():
                        if field == "station":
                            continue
                        rel_sigs = [sig for sig in current_sigs if sig.sensor == field]
                        assert len(rel_sigs) == 1
                        assert rel_sigs[0].value == value
        assert line_counter == len(signature_ts_rs.keys())
