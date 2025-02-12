""" Test siddhi producer. """
from datetime import datetime

import pytest
from cepact import (DAGConfig, SiddhiConfig, LocalSignatureFetcher,
                               GrafanaFetcher, LocalDiscretizationFetcher)
from cepact.output_producer import AllHighLevelPatternQuery
from cepact.output_producer.siddhi_producer import SiddhiProducer
from cepact.representations import Activity, SignatureBuilder, AnnotationParams, \
    DiscretizationBuilder, SignatureItem

mock_siddhi_conf = SiddhiConfig(mqtt_url="mockurl",
                                mqtt_user="mockuser",
                                mqtt_pwd="mockpw",
                                topic_prefix="mockpref",
                                map_sensor_name_data_to_mqtt={"o7_valve": "o7valve"})

mock_dag_conf = DAGConfig(det_methods=[AllHighLevelPatternQuery()],
                          siddhi_config=mock_siddhi_conf,
                          out_dir="mock",
                          sampling_freq=2,
                          signature_fetcher=LocalSignatureFetcher(local_in_dir="mock"),
                          annotation_param_fetcher=GrafanaFetcher(
                              url="http://localhost:3000",
                              auth="mock"),
                          ignore_sensor_fetcher=GrafanaFetcher(
                              url="http://localhost:3000",
                              auth="mock"),
                          discretization_fetcher=LocalDiscretizationFetcher("mock"))


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


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_paper():
    """ Test if siddhi producer works generally. """
    signature_builder = SignatureBuilder(activity_name="paperact",
                                         annotation_id="ann1",
                                         sampling_freq=2)
    dt0 = datetime.fromisoformat("2021-01-01T00:00:00")
    dt1 = datetime.fromisoformat("2021-01-01T00:01:00")
    dt2 = datetime.fromisoformat("2021-01-01T00:02:00")
    dt3 = datetime.fromisoformat("2021-01-01T00:03:00")
    dt4 = datetime.fromisoformat("2021-01-01T00:04:00")
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
    db.add_discretization_item("i1_pos_switch", float("-inf"), 0, True, True, "off")
    db.add_discretization_item("i1_pos_switch", 0, float("inf"), False, True, "on")
    sp = SiddhiProducer([AllHighLevelPatternQuery()],
                        activity,
                        db.build(),
                        mock_dag_conf)
    assert sp._get_header() == f'@App:name(\'DetectpaperactApp\')'
    assert sp._get_sources() == (
        "@source(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act', topic = 'mockpref/station1', username = 'mockuser', password = 'mockpw',\n"
        "@map(type = 'json', @attributes(timestamp = '$.timestamp', i1_pos_switch = '$.i1_pos_switch', i2_pos_switch = '$.i2_pos_switch', i5_light_barrier = '$.i5_light_barrier', m1_speed = '$.m1_speed', o7valve = '$.o7_valve', o8_compressor = '$.o8_compressor')))\n"
        "define stream station1Stream(timestamp string, i1_pos_switch int, i2_pos_switch int, i5_light_barrier int, m1_speed int, o7valve int, o8_compressor int);\n\n"
        "define stream station1StreamDisc(timestamp string, i1_pos_switch string, i2_pos_switch int, i5_light_barrier int, m1_speed int, o7valve int, o8_compressor int);")
    assert sp._get_sinks() == """@sink(type = 'log', prefix = 'LowLevel Log', priority = 'INFO')
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.ll', topic = 'ActivityEvents/LowLevel', username = 'mockuser', password = 'mockpw', @map(type = 'json'))
define Stream DetectedLowLevelActivityEvents(event string, activity string, ts_first string, ts_second string, ll_pattern_num int);

@sink(type = 'log', prefix = 'HighLevel Log', priority = 'INFO')
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.hl', topic = 'ActivityEvents/HighLevel', username = 'mockuser', password = 'mockpw', @map(type = 'json'))
define Stream DetectedHighLevelActivityEvents(event string, next_pattern string, activity string, ts_first string, ts_second string);

@sink(type = 'log', prefix = 'InstanceLevel Log', priority = 'INFO')
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.il.start', topic = 'DefaultBase/DefaultSource/DefaultCase/paper act', username = 'mockuser', password = 'mockpw', @map(type = 'json', enclosing.element = '$.event', validate.json = 'true', @payload(""\"{"lifecycle:transition":"start","time:timestamp":"{{ts_start}}", "detection:type": "{{detection_type}}"}""\")))
@sink(type = 'mqtt', url = 'mockurl', client.id = 'mqtt.station1.paper act.il.complete', topic = 'DefaultBase/DefaultSource/DefaultCase/paper act', username = 'mockuser', password = 'mockpw', @map(type = 'json', enclosing.element = '$.event', validate.json = 'true', @payload(""\"{"lifecycle:transition":"complete","time:timestamp":"{{ts_end}}", "detection:type": "{{detection_type}}"}""\")))
define Stream DetectedInstanceLevelActivities(activity string, detection_type string, ts_start string, ts_start_unix long, ts_end string, ts_end_unix long);"""
    assert sp._get_discretization_helpers() == """@info(name = 'station1DiscSourceMapper')
from station1Stream
select timestamp as timestamp, ifThenElse(i1_pos_switch <= 0, 'off', ifThenElse(i1_pos_switch > 0, 'on', 'ERROR')) as i1_pos_switch, i2_pos_switch as i2_pos_switch, i5_light_barrier as i5_light_barrier, m1_speed as m1_speed, o7valve as o7valve, o8_compressor as o8_compressor
insert into station1StreamDisc;"""
    assert sp._get_change_queries() == """@info(name="HighToLow-Helper")
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
from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] -> not HelperStream[event == "HighToLow"] and e2 = DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-2" and time:timestampInMilliseconds(e1.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') <= time:timestampInMilliseconds(ts_second, 'yyyy-MM-dd HH:mm:ss.SS')]
select "HighLevel-Pattern-2" as event,  "LowLevel-Pattern-3" as next_pattern, "paper act" as activity, e2.ts_first as ts_first, e2.ts_second as ts_second
insert into DetectedHighLevelActivityEvents;

@info(name="Detect-LowLevel-Pattern-3")
from every e1 = station1StreamDisc, e2 = station1StreamDisc[(e1.o7valve==512 and e2.o7valve==0)]
select "LowLevel-Pattern-3" as event, "paper act" as activity, e1.timestamp as ts_first, e2.timestamp as ts_second, 3 as ll_pattern_num
insert into DetectedLowLevelActivityEvents;

@info(name="Detect-HighLevel-Pattern-3")
from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-2"] -> not HelperStream[event == "HighToLow"] and e2 = DetectedLowLevelActivityEvents[event == "LowLevel-Pattern-3" and time:timestampInMilliseconds(e1.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') <= time:timestampInMilliseconds(ts_second, 'yyyy-MM-dd HH:mm:ss.SS')]
select "HighLevel-Pattern-3" as event,  "LowLevel-Pattern-4" as next_pattern, "paper act" as activity, e2.ts_first as ts_first, e2.ts_second as ts_second
insert into DetectedHighLevelActivityEvents;"""
    assert sp._get_instance_level_detection_queries() == """@info(name="Detect-AllHighLevelPattern-InstanceLevelActivity")
from every e1 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-1"] -> not HelperStream[event == "HighToLow"] and e2 = DetectedHighLevelActivityEvents[event == "HighLevel-Pattern-3"]
select "paper act" as activity, "AllHighLevelPattern" as detection_type, e1.ts_second as ts_start, time:timestampInMilliseconds(e1.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') as ts_start_unix, e2.ts_second as ts_end, time:timestampInMilliseconds(e2.ts_second, 'yyyy-MM-dd HH:mm:ss.SS') as ts_end_unix
insert into DetectedInstanceLevelActivities;"""
