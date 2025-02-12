import json
import os
from typing import Set

import pandas as pd
import pytest
from cepact import (DetServiceGenerator, DAGConfig, SiddhiConfig,
                               LocalDiscretizationFetcher, LocalAnnotationParamFetcher,
                               LocalSignatureFetcher, LocalIgnoreSensorFetcher,
                               AllHighLevelPatternQuery,
                               First50HighLevelPatternQuery, FirstLastLowLevelPatternQuery,
                               Any25LowLevelPatternQuery, Any50LowLevelPatternQuery,
                               Any75LowLevelPatternQuery)

test_path = os.path.join("tests", "local_input_mocks")
test_cases = [f for f in os.listdir(test_path) if os.path.isdir(os.path.join(test_path, f))]
# Define the format
format_str = '%Y-%m-%d %H:%M:%S.%f'


# Convert to datetime object

@pytest.mark.parametrize("testcase_dir", test_cases)
def test_file_generation_overall(testcase_dir, tmp_path):
    """ Test basic functioning. """
    local_in_dir = os.path.join(test_path, testcase_dir, "in")
    mock_siddhi_conf = SiddhiConfig(mqtt_url="mockurl",
                                    mqtt_user="mockuser",
                                    mqtt_pwd="mockpw",
                                    topic_prefix="mockpref",
                                    map_sensor_name_data_to_mqtt={})

    mock_dag_conf = DAGConfig(det_methods=[AllHighLevelPatternQuery(),
                                           First50HighLevelPatternQuery(),
                                           FirstLastLowLevelPatternQuery(),
                                           Any25LowLevelPatternQuery(),
                                           Any50LowLevelPatternQuery(),
                                           Any75LowLevelPatternQuery()],
                              siddhi_config=mock_siddhi_conf,
                              out_dir=tmp_path,
                              sampling_freq=2,
                              signature_fetcher=LocalSignatureFetcher(local_in_dir),
                              annotation_param_fetcher=LocalAnnotationParamFetcher(local_in_dir),
                              ignore_sensor_fetcher=LocalIgnoreSensorFetcher(local_in_dir),
                              discretization_fetcher=LocalDiscretizationFetcher(local_in_dir))
    # run the input processor
    dag = DetServiceGenerator(mock_dag_conf)
    dag.run()
    activity_names: Set[str] = set(
        pd.read_csv(os.path.join(test_path, testcase_dir, "in", "anno_params.csv"))[
            "activity_name"].tolist())
    for activity_name in activity_names:
        # check if all files are generated
        act_wo_spaces = activity_name.replace(" ", "")
        changes_out = tmp_path / "changes" / f"{act_wo_spaces}_changes.jsonl"
        assert changes_out.exists()
        # make sure that changes content is correct
        with open(changes_out, "r", encoding="utf-8") as f:
            with open(
                    os.path.join(test_path, testcase_dir, "assert", activity_name, "changes.jsonl"),
                    "r", encoding="utf-8") as f_assert:
                for line in f:
                    file_dict = json.loads(line.replace("\n", ""))
                    relevant_assert = json.loads(f_assert.readline().replace("\n", ""))
                    assert file_dict == relevant_assert
                # make sure that f_assert is empty
                assert f_assert.readline() == ""
        app_out = tmp_path / "apps" / f"Detect{act_wo_spaces}App.siddhi"
        assert app_out.exists()
        # make sure that app content is correct
        # first check if app.siddhi assertion file exists
        if os.path.exists(
                os.path.join(test_path, testcase_dir, "assert", activity_name, "app.siddhi")):
            with open(app_out, "r", encoding="utf-8") as f:
                app_content = f.read()
                with open(os.path.join(test_path, testcase_dir, "assert", activity_name,
                                       "app.siddhi"), "r", encoding="utf-8") as f_assert:
                    assert app_content == f_assert.read()
    # get tuples of unique activity_name annotation_id combinations in csv
    anno_params = pd.read_csv(os.path.join(test_path, testcase_dir, "in", "anno_params.csv"))
    unique_activity_annotation_ids = set(
        anno_params.apply(lambda x: (x["activity_name"], x["annotation_id"]), axis=1))
    for activity_name, annotation_id in unique_activity_annotation_ids:
        # check if all files are generated
        act_wo_spaces = activity_name.replace(" ", "")
        annotation_id_wo_spaces = annotation_id.replace(" ", "")
        sig_out = tmp_path / "signatures" / f"{act_wo_spaces}_{annotation_id_wo_spaces}_signature.jsonl"
        assert sig_out.exists()
        # make sure that signature content is correct
        with open(sig_out, "r", encoding="utf-8") as f:
            with open(os.path.join(test_path, testcase_dir, "assert", activity_name, annotation_id,
                                   "signature.jsonl"), "r", encoding="utf-8") as f_assert:
                for line in f:
                    file_dict = json.loads(line.replace("\n", ""))
                    relevant_assert = json.loads(f_assert.readline().replace("\n", ""))
                    for key, value in file_dict.items():
                        if isinstance(value, list):
                            # order by name of station
                            value.sort(key=lambda x: x["station"])
                            relevant_assert[key].sort(key=lambda x: x["station"])
                        assert value == relevant_assert[key]
                # make sure that f_assert is empty
                assert f_assert.readline() == ""
    # make sure that a log file is generated
    log_out = tmp_path / "log.txt"
    assert log_out.exists()
