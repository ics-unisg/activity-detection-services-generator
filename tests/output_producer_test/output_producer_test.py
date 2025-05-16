""" Test output producer. """
import json
from datetime import datetime

import pytest
from cepact import DAGConfig, SiddhiConfig, GrafanaFetcher, \
    InfluxFetcher, LocalDiscretizationFetcher
from cepact.output_producer import (OutputProducer, AllHighLevelPatternQuery,
                                               First50HighLevelPatternQuery,
                                               FirstLastLowLevelPatternQuery,
                                               Any25LowLevelPatternQuery, Any50LowLevelPatternQuery,
                                               Any75LowLevelPatternQuery)
from cepact.representations import Activity, SignatureBuilder, \
    AnnotationParams, DiscretizationBuilder, SignatureItem

siddhi_app_assert = """@App:name('DetectpaperactApp')

@source(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act', topic = 'mockpref/station1', username = 'mockuser', password = 'mockpw',
@map(type = 'json', @attributes(timestamp = '$.timestamp', i1_pos_switch = '$.i1_pos_switch', i2_pos_switch = '$.i2_pos_switch', i5_light_barrier = '$.i5_light_barrier', m1_speed = '$.m1_speed', o7valve = '$.o7_valve', o8_compressor = '$.o8_compressor')))
define stream station1Stream(timestamp string, i1_pos_switch int, i2_pos_switch int, i5_light_barrier int, m1_speed int, o7valve int, o8_compressor int);

define stream station1StreamDisc(timestamp string, i1_pos_switch string, i2_pos_switch int, i5_light_barrier int, m1_speed int, o7valve int, o8_compressor int);

@sink(type = 'log', prefix = 'LowLevel Log', priority = 'INFO')
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.ll', topic = 'ActivityEvents/LowLevel', username = 'mockuser', password = 'mockpw', @map(type = 'json'))
define Stream DetectedLowLevelActivityEvents(event string, activity string, ts_first string, ts_second string, ll_pattern_num int);

@sink(type = 'log', prefix = 'HighLevel Log', priority = 'INFO')
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.hl', topic = 'ActivityEvents/HighLevel', username = 'mockuser', password = 'mockpw', @map(type = 'json'))
define Stream DetectedHighLevelActivityEvents(event string, next_pattern string, activity string, ts_first string, ts_second string);

@sink(type = 'log', prefix = 'InstanceLevel Log', priority = 'INFO')
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.il.start', topic = 'DefaultBase/DefaultSource/DefaultCase/paper act', username = 'mockuser', password = 'mockpw', @map(type = 'json', enclosing.element = '$.event', validate.json = 'true', @payload(""\"{"lifecycle:transition":"start","time:timestamp":"{{ts_start}}", "detection:type": "{{detection_type}}"}""\")))
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.il.complete', topic = 'DefaultBase/DefaultSource/DefaultCase/paper act', username = 'mockuser', password = 'mockpw', @map(type = 'json', enclosing.element = '$.event', validate.json = 'true', @payload(""\"{"lifecycle:transition":"complete","time:timestamp":"{{ts_end}}", "detection:type": "{{detection_type}}"}""\")))
define Stream DetectedInstanceLevelActivities(activity string, detection_type string, ts_start string, ts_start_unix long, ts_end string, ts_end_unix long);

@info(name = 'station1DiscSourceMapper')
from station1Stream
select timestamp as timestamp, ifThenElse(i1_pos_switch <= 0, 'off', ifThenElse(i1_pos_switch > 0, 'on', 'ERROR')) as i1_pos_switch, i2_pos_switch as i2_pos_switch, i5_light_barrier as i5_light_barrier, m1_speed as m1_speed, o7valve as o7valve, o8_compressor as o8_compressor
insert into station1StreamDisc;

@info(name="HighToLow-Helper")
from every e1 = DetectedLowLevelActivityEvents, e2 = DetectedLowLevelActivityEvents[e1.ll_pattern_num >= e2.ll_pattern_num]
select "HighToLow" as event
insert into HelperStream;

@info(name="Detect-LowLevel-Pattern-1")
from every e1 = station1StreamDisc, e2 = station1StreamDisc[(e1.m1_speed==0 and e2.m1_speed==-512) and (e1.o7valve==0 and e2.o7valve==512)]
select "LowLevel-Pattern-1" as event, "paper act" as activity, e1.timestamp as ts_first, e2.timestamp as ts_second, 1 as ll_pattern_num
insert into DetectedLowLevelActivityEvents;

@info(name="Detect-HighLevel-Pattern-1")
from DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-1"]
select "HighLevel-Pattern-1" as event,  "LowLevel-Pattern-2" as next_pattern, "paper act" as activity, ts_first, ts_second
insert into DetectedHighLevelActivityEvents;

@info(name="Detect-LowLevel-Pattern-2")
from every e1 = station1StreamDisc, e2 = station1StreamDisc[(e1.i1_pos_switch=='off' and e2.i1_pos_switch=='on') and (e1.m1_speed==-512 and e2.m1_speed==0)]
select "LowLevel-Pattern-2" as event, "paper act" as activity, e1.timestamp as ts_first, e2.timestamp as ts_second, 2 as ll_pattern_num
insert into DetectedLowLevelActivityEvents;

@info(name="Detect-HighLevel-Pattern-2")
from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] -> not DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] and e2 = DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-2" and time:timestampInMilliseconds(e1.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') <= time:timestampInMilliseconds(ts_second, 'yyyy-MM-dd HH:mm:ss.SS')]
select "HighLevel-Pattern-2" as event,  "LowLevel-Pattern-3" as next_pattern, "paper act" as activity, e2.ts_first as ts_first, e2.ts_second as ts_second
insert into DetectedHighLevelActivityEvents;

@info(name="Detect-LowLevel-Pattern-3")
from every e1 = station1StreamDisc, e2 = station1StreamDisc[(e1.o7valve==512 and e2.o7valve==0)]
select "LowLevel-Pattern-3" as event, "paper act" as activity, e1.timestamp as ts_first, e2.timestamp as ts_second, 3 as ll_pattern_num
insert into DetectedLowLevelActivityEvents;

@info(name="Detect-HighLevel-Pattern-3")
from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-2"] -> not DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] and e2 = DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-3" and time:timestampInMilliseconds(e1.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') <= time:timestampInMilliseconds(ts_second, 'yyyy-MM-dd HH:mm:ss.SS')]
select "HighLevel-Pattern-3" as event,  "LowLevel-Pattern-4" as next_pattern, "paper act" as activity, e2.ts_first as ts_first, e2.ts_second as ts_second
insert into DetectedHighLevelActivityEvents;

@info(name="Detect-AllHighLevelPattern-InstanceLevelActivity")
from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] -> not DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] and e2 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-3"]
select "paper act" as activity, "AllHighLevelPattern" as detection_type, e1.ts_second as ts_start, time:timestampInMilliseconds(e1.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') as ts_start_unix, e2.ts_second as ts_end, time:timestampInMilliseconds(e2.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') as ts_end_unix
insert into DetectedInstanceLevelActivities;
"""

signature_assert = """{"timestamp": "2021-01-01 00:00:00.00", "measurements": [{"station": "station1", "i1_pos_switch": 0, "i2_pos_switch": 0, "i5_light_barrier": 1, "m1_speed": 0, "o7_valve": 0, "o8_compressor": 0}]}
{"timestamp": "2021-01-01 00:00:01.00", "measurements": [{"station": "station1", "i1_pos_switch": 0, "i2_pos_switch": 0, "i5_light_barrier": 1, "m1_speed": -512, "o7_valve": 512, "o8_compressor": 0}]}
{"timestamp": "2021-01-01 00:00:02.00", "measurements": [{"station": "station1", "i1_pos_switch": 1, "i2_pos_switch": 0, "i5_light_barrier": 1, "m1_speed": 0, "o7_valve": 512, "o8_compressor": 0}]}
{"timestamp": "2021-01-01 00:00:03.00", "measurements": [{"station": "station1", "i1_pos_switch": 1, "i2_pos_switch": 0, "i5_light_barrier": 1, "m1_speed": 0, "o7_valve": 512, "o8_compressor": 0}]}
{"timestamp": "2021-01-01 00:00:04.00", "measurements": [{"station": "station1", "i1_pos_switch": 1, "i2_pos_switch": 0, "i5_light_barrier": 1, "m1_speed": 0, "o7_valve": 0, "o8_compressor": 0}]}
"""

changes_assert = """{"timestamp":"2021-01-01 00:00:01.00","changes":[{"station":"station1","sensor":"m1_speed","prev_value":0,"value":-512},{"station":"station1","sensor":"o7_valve","prev_value":0,"value":512}]}
{"timestamp":"2021-01-01 00:00:02.00","changes":[{"station":"station1","sensor":"i1_pos_switch","prev_value":"off","value":"on"},{"station":"station1","sensor":"m1_speed","prev_value":-512,"value":0}]}
{"timestamp":"2021-01-01 00:00:04.00","changes":[{"station":"station1","sensor":"o7_valve","prev_value":512,"value":0}]}
"""

mock_siddhi_conf = SiddhiConfig(mqtt_url="mockurl",
                                mqtt_user="mockuser",
                                mqtt_pwd="mockpw",
                                topic_prefix="mockpref",
                                map_sensor_name_data_to_mqtt={"o7_valve": "o7valve"})


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


signature_builder = SignatureBuilder(activity_name="paperact",
                                     annotation_id="ann1",
                                     sampling_freq=2)
dt0 = datetime.fromisoformat("2021-01-01T00:00:00")
dt1 = datetime.fromisoformat("2021-01-01T00:00:01")
dt2 = datetime.fromisoformat("2021-01-01T00:00:02")
dt3 = datetime.fromisoformat("2021-01-01T00:00:03")
dt4 = datetime.fromisoformat("2021-01-01T00:00:04")
add_many(signature_builder, "i1_pos_switch", [dt0, dt1, dt2, dt3, dt4], [0, 0, 1, 1, 1])
add_many(signature_builder, "i2_pos_switch", [dt0, dt1, dt2, dt3, dt4], [0, 0, 0, 0, 0])
add_many(signature_builder, "i5_light_barrier", [dt0, dt1, dt2, dt3, dt4], [1, 1, 1, 1, 1])
add_many(signature_builder, "m1_speed", [dt0, dt1, dt2, dt3, dt4], [0, -512, 0, 0, 0])
add_many(signature_builder, "o7_valve", [dt0, dt1, dt2, dt3, dt4], [0, 512, 512, 512, 0])
add_many(signature_builder, "o8_compressor", [dt0, dt1, dt2, dt3, dt4], [0, 0, 0, 0, 0])
signature = signature_builder.build()
annotation_params = AnnotationParams(annotation_id="ann1",
                                     activity_name="paper act",
                                     stations=["station1"],
                                     start=datetime.fromisoformat("2021-01-01T00:00:00"),
                                     end=datetime.fromisoformat("2021-01-01T00:05:00"))
activity = Activity(annotation_params, [signature], [])
db = DiscretizationBuilder()
db.add_discretization_item(sensor="i1_pos_switch", beg=float("-inf"), to=0,
                           to_incl=True, beg_incl=True, target_value="off")
db.add_discretization_item(sensor="i1_pos_switch", beg=0, to=float("inf"),
                           beg_incl=False, to_incl=True, target_value="on")
discr = db.build()


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_paper(tmp_path):
    """ Test if output producer works generally. """
    mock_dag_conf = DAGConfig(det_methods=[AllHighLevelPatternQuery()],
                              siddhi_config=mock_siddhi_conf,
                              out_dir=tmp_path,
                              sampling_freq=2,
                              signature_fetcher=InfluxFetcher(
                                  url="http://localhost:8086",
                                  auth="mock",
                                  org="mock",
                                  station_bucket_map={"mock":
                                                          "mock"}, ),
                              annotation_param_fetcher=GrafanaFetcher(
                                  url="http://localhost:3000",
                                  auth="mock"),
                              ignore_sensor_fetcher=GrafanaFetcher(
                                  url="http://localhost:3000",
                                  auth="mock"),
                              discretization_fetcher=LocalDiscretizationFetcher("mock"))
    output_producer = OutputProducer(discr, mock_dag_conf)
    output_producer.write_app(activity)
    # check that app exists in subfolder apps
    output_siddhi_file = tmp_path / "apps" / "DetectpaperactApp.siddhi"
    assert output_siddhi_file.exists()
    # check that app content is correct
    with open(output_siddhi_file, "r", encoding="utf-8") as file:
        siddhi_app = file.read()
        assert siddhi_app == siddhi_app_assert
    output_producer.write_signature(activity)
    output_signature_file = tmp_path / "signatures" / "paperact_ann1_signature.jsonl"
    assert output_signature_file.exists()
    with open(output_signature_file, "r", encoding="utf-8") as file:
        signature = file.read()
        assert signature == signature_assert
    output_producer.write_changes(activity)
    output_changes_file = tmp_path / "changes" / "paperact_changes.jsonl"
    assert output_changes_file.exists()
    with open(output_changes_file, "r", encoding="utf-8") as file:
        # assert for each line that the jsons are equal
        for i in range(3):
            file_dict = json.loads(file.readline().replace("\n", ""))
            relevant_assert = json.loads(changes_assert.split("\n")[i])
            assert file_dict == relevant_assert


def test_all_il_queries_active(tmp_path) -> None:
    """ Test whether activation of all active il queries works. """
    mock_dag_conf = DAGConfig(det_methods=[AllHighLevelPatternQuery(),
                                           First50HighLevelPatternQuery(),
                                           FirstLastLowLevelPatternQuery(),
                                           Any25LowLevelPatternQuery(),
                                           Any50LowLevelPatternQuery(),
                                           Any75LowLevelPatternQuery()],
                              siddhi_config=mock_siddhi_conf,
                              out_dir=tmp_path,
                              sampling_freq=2,
                              signature_fetcher=InfluxFetcher(
                                  url="http://localhost:8086",
                                  auth="mock",
                                  org="mock",
                                  station_bucket_map={"mock":
                                                          "mock"}, ),
                              annotation_param_fetcher=GrafanaFetcher(
                                  url="http://localhost:3000",
                                  auth="mock"),
                              ignore_sensor_fetcher=GrafanaFetcher(
                                  url="http://localhost:3000",
                                  auth="mock"),
                              discretization_fetcher=LocalDiscretizationFetcher("mock"))
    output_producer = OutputProducer(discr, mock_dag_conf)
    output_producer.write_app(activity)
    # check that app exists in subfolder apps
    output_siddhi_file = tmp_path / "apps" / "DetectpaperactApp.siddhi"
    assert output_siddhi_file.exists()
    # check that app content is correct
    with open(output_siddhi_file, "r", encoding="utf-8") as file:
        siddhi_app = file.read()
        assert "Detect-AllHighLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-First50HighLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-FirstLastLowLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-Any25LowLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-Any50LowLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-Any75LowLevelPattern-InstanceLevelActivity" in siddhi_app


def test_some_il_queries_active(tmp_path) -> None:
    """ Test whether activation of some active il queries works. """
    mock_dag_conf = DAGConfig(det_methods=[AllHighLevelPatternQuery(),
                                           First50HighLevelPatternQuery(),
                                           Any75LowLevelPatternQuery()],
                              siddhi_config=mock_siddhi_conf,
                              out_dir=tmp_path,
                              sampling_freq=2,
                              signature_fetcher=InfluxFetcher(
                                  url="http://localhost:8086",
                                  auth="mock",
                                  org="mock",
                                  station_bucket_map={"mock":
                                                          "mock"}, ),
                              annotation_param_fetcher=GrafanaFetcher(
                                  url="http://localhost:3000",
                                  auth="mock"),
                              ignore_sensor_fetcher=GrafanaFetcher(
                                  url="http://localhost:3000",
                                  auth="mock"),
                              discretization_fetcher=LocalDiscretizationFetcher("mock"))
    output_producer = OutputProducer(discr, mock_dag_conf)
    output_producer.write_app(activity)
    # check that app exists in subfolder apps
    output_siddhi_file = tmp_path / "apps" / "DetectpaperactApp.siddhi"
    assert output_siddhi_file.exists()
    # check that app content is correct
    with open(output_siddhi_file, "r", encoding="utf-8") as file:
        siddhi_app = file.read()
        assert "Detect-AllHighLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-First50HighLevelPattern-InstanceLevelActivity" in siddhi_app
        assert "Detect-FirstLastLowLevelPattern-InstanceLevelActivity" not in siddhi_app
        assert "Detect-Any25LowLevelPattern-InstanceLevelActivity" not in siddhi_app
        assert "Detect-Any50LowLevelPattern-InstanceLevelActivity" not in siddhi_app
        assert "Detect-Any75LowLevelPattern-InstanceLevelActivity" in siddhi_app


def test_no_il_queries_active(tmp_path) -> None:
    with pytest.raises(ValueError):
        DAGConfig(det_methods=[],
                  siddhi_config=mock_siddhi_conf,
                  out_dir=tmp_path,
                  sampling_freq=2,
                  signature_fetcher=InfluxFetcher(
                      url="http://localhost:8086",
                      auth="mock",
                      org="mock",
                      station_bucket_map={"mock":
                                              "mock"}, ),
                  annotation_param_fetcher=GrafanaFetcher(
                      url="http://localhost:3000",
                      auth="mock"),
                  ignore_sensor_fetcher=GrafanaFetcher(
                      url="http://localhost:3000",
                      auth="mock"),
                  discretization_fetcher=LocalDiscretizationFetcher("mock"))
