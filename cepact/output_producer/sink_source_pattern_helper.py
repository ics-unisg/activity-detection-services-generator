""" Various functions to create the source, sink, pattern and helper queries for the siddhi app. """
from datetime import datetime
from typing import Dict, List, Tuple

from ..conf import SiddhiConfig
from ..representations import Activity, Discretization, Changes, AnnotationParams, Change


def _map_sensor_name_iot_to_mqtt(sensor_name: str, sensor_name_mapping: Dict[str, str]) -> str:
    """ Map the sensor name from iot to mqtt """
    return sensor_name if sensor_name not in sensor_name_mapping else \
        sensor_name_mapping[sensor_name]


def _stringify_sensors_types(sensor_types: Dict[str, type],
                             sensor_name_mapping: Dict[str, str]) -> str:
    """return (sensorname type, ....) string"""
    namemap: Dict[type, str] = {int: 'int', float: 'float', str: 'string'}
    return '(' + ', '.join([
        f"{_map_sensor_name_iot_to_mqtt(sensor, sensor_name_mapping)} "
        f"{namemap[sensor_types[sensor]]}"
        for sensor in sensor_types]) + ')'


def define_source(station: str,
                  activityname: str,
                  sensor_types: Dict[str, type],
                  sensor_discr_types: Dict[str, type],
                  siddhi_config: SiddhiConfig) -> str:
    """ Define the source for the siddhi app """
    map_anno = ''
    if len(sensor_types) == 0:
        map_anno += "@map(type = 'json')"
    else:
        map_anno += "@map(type = 'json', @attributes("
        for idx, sensor in enumerate(sensor_types):
            map_anno += f"{sensor if sensor not in siddhi_config.map_sensor_name_data_to_mqtt
            else siddhi_config.map_sensor_name_data_to_mqtt[sensor]} = '$.{sensor}'"
            # if not last sensor, add comma
            if idx != len(sensor_types) - 1:
                map_anno += ", "
        map_anno += "))"
    source_anno = (f"@source(type = 'mqtt', url = '{siddhi_config.mqtt_url}',"
                   f" client.id = 'mqtt.{station}.{activityname}', "
                   f"topic = '{siddhi_config.topic_prefix}/{station}', "
                   f"username = '{siddhi_config.mqtt_user}', "
                   f"password = '{siddhi_config.mqtt_pwd}',\n"
                   f"{map_anno})")
    source_stream = f"{source_anno}\ndefine stream {station}Stream{_stringify_sensors_types(
        sensor_types,
        siddhi_config.map_sensor_name_data_to_mqtt)};"
    # create a stream that maps the source to the discretized version
    source_stream += f"\n\ndefine stream {station}StreamDisc{_stringify_sensors_types(
        sensor_discr_types,
        siddhi_config.map_sensor_name_data_to_mqtt)};"
    return source_stream


def define_discretization_query(station: str,
                                discretization: Discretization,
                                sensor_discr_types: Dict[str, type],
                                siddhi_config: SiddhiConfig) -> str:
    """ Define the discretization query for the siddhi app """
    discretization_query = ""
    discretization_query += (f"@info(name = '{station}DiscSourceMapper')\n"
                             f"from {station}Stream\n"
                             "select ")
    for sensor in sensor_discr_types:
        sensor = sensor if sensor not in siddhi_config.map_sensor_name_data_to_mqtt else \
            siddhi_config.map_sensor_name_data_to_mqtt[sensor]
        if not discretization.is_discretized(sensor):
            discretization_query += f"{sensor} as {sensor}, "
        else:
            sensor_mapping = discretization.get_mapping_for_sensor(sensor)
            # order by the first element of the tuple
            ordered_mapping = dict(sorted(sensor_mapping.items(), key=lambda item: item[0]))
            for key, value in ordered_mapping.items():
                if key[0][0] == float("-inf"):
                    discretization_query += f"ifThenElse({sensor} {
                    '<=' if key[1][1] == 'incl' else '<'} {key[0][1]
                    }, {
                    "'" + value + "'" if isinstance(value, str) else value
                    }, "
                elif key[0][1] == float("inf"):
                    discretization_query += f"ifThenElse({sensor} {
                    '>=' if key[1][0] == 'incl' else '>'
                    } {key[0][0]}, {
                    "'" + value + "'" if isinstance(value, str) else value
                    }, "
                else:
                    discretization_query += f"ifThenElse({sensor} {
                    '>=' if key[1][0] == 'incl' else '>'
                    } {key[0][0]} and {sensor} {
                    '<=' if key[1][1] == 'incl' else '<'
                    } {key[0][1]}, {
                    "'" + value + "'" if isinstance(value, str) else value
                    }, "
            discretization_query += "'ERROR'"
            for _ in range(len(ordered_mapping)):
                discretization_query += ")"
            discretization_query += f" as {sensor}, "
    discretization_query = discretization_query[:-2] + "\n"
    discretization_query += f"insert into {station}StreamDisc;"
    return discretization_query


def define_sink(activity: Activity,
                siddhi_config: SiddhiConfig,
                base_source_case_topic: Tuple[str, str, str] = ('DefaultBase',
                                                                'DefaultSource',
                                                                'DefaultCase')) -> str:
    """ Define the sink for the siddhi app """
    act_params = activity.get_annotation_params()
    activityname = act_params.activity_name
    sink = (f"@sink(type = 'log', prefix = 'LowLevel Log', priority = 'INFO')\n"
            f"@sink(type = 'mqtt', url = '{siddhi_config.mqtt_url}', "
            f"client.id = 'mqtt.{act_params.stations_str}.{activityname}.ll', "
            f"topic = 'ActivityEvents/LowLevel', username = '{siddhi_config.mqtt_user}', "
            f"password = '{siddhi_config.mqtt_pwd}', "
            f"@map(type = 'json'))\n"
            f"define Stream DetectedLowLevelActivityEvents(event string, "
            f"activity string, ts_first string, ts_second "
            f"string, ll_pattern_num int);\n\n")

    sink += (f"@sink(type = 'log', prefix = 'HighLevel Log', priority = 'INFO')\n"
             f"@sink(type = 'mqtt', url = '{siddhi_config.mqtt_url}', "
             f"client.id = 'mqtt.{act_params.stations_str}.{activityname}.hl', "
             f"topic = 'ActivityEvents/HighLevel', username = '{siddhi_config.mqtt_user}', "
             f"password = '{siddhi_config.mqtt_pwd}', "
             f"@map(type = 'json'))\n"
             f"define Stream DetectedHighLevelActivityEvents(event string, "
             f"next_pattern string, activity string, "
             f"ts_first string, ts_second string);\n\n")

    mqtt_xes_topic = (f'{base_source_case_topic[0]}/'
                      f'{base_source_case_topic[1]}/'
                      f'{base_source_case_topic[2]}/'
                      f'{activityname}')

    def mqtt_xes_sink(option: str) -> str:
        """ Create the mqtt xes sink for the siddhi app """
        if option not in ['start', 'complete']:
            raise ValueError("Type must be either 'start' or 'complete'")
        return (f"@sink(type = 'mqtt', url = '{siddhi_config.mqtt_url}', "
                f"client.id = 'mqtt.{act_params.stations_str}.{activityname}.il.{option}', "
                f"topic = '{mqtt_xes_topic}', username = '{siddhi_config.mqtt_user}', "
                f"password = '{siddhi_config.mqtt_pwd}', "
                '@map(type = \'json\', enclosing.element = \'$.event\', validate.json = \'true\', '
                '@payload("""{"lifecycle:transition":"'
                f'{option}'
                '","time:timestamp":"{{'
                f'{"ts_start" if option == "start" else "ts_end"}'
                '}}", "detection:type": "{{detection_type}}"}""")))\n')

    sink += (f"@sink(type = 'log', prefix = 'InstanceLevel Log', priority = 'INFO')\n"
             f"{mqtt_xes_sink('start')}"
             f"{mqtt_xes_sink('complete')}"
             f"define Stream DetectedInstanceLevelActivities(activity string, "
             f"detection_type string, "
             f"ts_start string, "
             f"ts_start_unix long, "
             f"ts_end string, "
             f"ts_end_unix long);")
    return sink


def create_high_to_low_helper() -> str:
    """ Create the high to low helper query for the siddhi app """
    htl_helper = ('@info(name="HighToLow-Helper")\n'
                  'from every e1 = DetectedLowLevelActivityEvents, '
                  'e2 = DetectedLowLevelActivityEvents['
                  'e1.ll_pattern_num >= '
                  'e2.ll_pattern_num]\n'
                  'select "HighToLow" as event\n'
                  'insert into HelperStream;')
    return htl_helper


def low_high_level_pattern_case_1(llchanges: List[Change],
                                  activity_params: AnnotationParams,
                                  counter: int,
                                  siddhi_config: SiddhiConfig) -> str:
    """ Create the low and high level pattern queries for the siddhi app at timestamps
        where only one station is active """
    query = (f'@info(name="Detect-LowLevel-Pattern-{counter}")\n'
             f'from every '
             f'e1 = {llchanges[0].station}StreamDisc, '
             f'e2 = {llchanges[0].station}StreamDisc[')
    for z, llchange in enumerate(llchanges):
        sensorname = _map_sensor_name_iot_to_mqtt(llchange.sensor,
                                                  siddhi_config.map_sensor_name_data_to_mqtt)
        query += (
            f'(e1.{sensorname}=={("'" + llchange.prev_value + "'") if
            isinstance(llchange.prev_value, str) else llchange.prev_value} '
            f'and e2.{sensorname}=={("'" + llchange.value + "'")
            if isinstance(llchange.value, str) else llchange.value})'
        )
        if z == (len(llchanges) - 1):
            query += ']\n'
        else:
            query += ' and '
    query += (
        f'select "LowLevel-Pattern-{counter}" as event, "{activity_params.activity_name}" '
        f'as activity, e1.timestamp as '
        f'ts_first, e2.timestamp as ts_second, {counter} as ll_pattern_num\n')
    query += "insert into DetectedLowLevelActivityEvents;\n"
    return query


def query_hlstream(activity_params: AnnotationParams,
                   counter: int,
                   is_last: bool) -> str:
    """ Create the high level pattern queries for the siddhi app """
    quer = f'@info(name="Detect-HighLevel-Pattern-{counter}")\n'
    if counter > 1:
        quer += (f'from every e1 = DetectedHighLevelActivityEvents[event == '
                 f'"HighLevel-Pattern-{counter - 1}"] '
                 f'-> not HelperStream[event == "HighToLow"] and '
                 f'e2 = DetectedLowLevelActivityEvents[event '
                 f'== "LowLevel-Pattern-{counter}" '
                 f'and time:timestampInMilliseconds(e1.ts_second, \'yyyy-MM-dd HH:mm:ss.SS\') '
                 f'<= time:timestampInMilliseconds(ts_second, \'yyyy-MM-dd HH:mm:ss.SS\')]\n')
        quer += (
            f'select "HighLevel-Pattern-{counter}" as event,  '
            f'"LowLevel-Pattern-{counter + 1}" as '
            f'next_pattern, "{activity_params.activity_name}" '
            f'as activity, e2.ts_first as ts_first, e2.ts_second as '
            f'ts_second\n')
    else:
        quer += (f'from DetectedLowLevelActivityEvents[event '
                 f'== "LowLevel-Pattern-{counter}"]\n')
        quer += (
            f'select "HighLevel-Pattern-{counter}" as event,  '
            f'"LowLevel-Pattern-{counter + 1}" as '
            f'next_pattern, "{activity_params.activity_name}" as activity, '
            f'ts_first, ts_second\n')
    quer += ("insert into DetectedHighLevelActivityEvents;")
    if not is_last:
        quer += "\n"
    return quer


def get_time_from_hz(sampling_freq: float) -> str:
    """ Get the time in the Siddhi format from the frequency """
    # time as datetime.timedelta
    time = (1 / sampling_freq) / 3
    # if more than one hour raise error
    if time > 3600:
        raise ValueError("Sampling frequency is too low.")
    # return time as x minutes y seconds z milliseconds (only if not 0), using datetime features
    if time >= 60:
        return str(datetime.fromtimestamp(time).strftime("%M minutes %S seconds %f milliseconds"))
    if time >= 1:
        return str(datetime.fromtimestamp(time).strftime("%S seconds %f milliseconds"))
    return str(datetime.fromtimestamp(time).strftime("%f milliseconds"))


# pylint: disable=too-many-arguments
def low_high_level_pattern_case_multi(timestamp: datetime,
                                      changes: Changes,
                                      activity_params: AnnotationParams,
                                      counter: int,
                                      sampling_freq: float,
                                      siddhi_config: SiddhiConfig) -> str:
    """ Create the low and high level pattern queries in case of multiple stations active """
    # multiple stations active at the same time
    res_at_ts = changes.get_stations_at_ts(timestamp)
    query = ""
    for station in res_at_ts:
        llchanges = changes.get_changes_at_ts_station(timestamp, station)
        if len(llchanges) == 0:
            continue
        query += f'@info(name="Detect-PartLowLevel-Pattern-{counter}-{station}")\n'
        query += 'from every '
        query += f'e1 = {station}StreamDisc, '
        query += f'e2 = {station}StreamDisc['
        for z, llchange in enumerate(llchanges):
            sensorname = _map_sensor_name_iot_to_mqtt(
                llchange.sensor,
                siddhi_config.map_sensor_name_data_to_mqtt)
            query += (
                f'(e1.{sensorname} == {("'" + llchange.prev_value + "'")
                if isinstance(llchange.prev_value, str) else llchange.prev_value} '
                f'and e2.{sensorname} == {("'" + llchange.value + "'")
                if isinstance(llchange.value, str) else llchange.value})')
            if z == (len(llchanges) - 1):
                query += ']\n'
            else:
                query += ' and '
        query += (
            f'select "PartLowLevel-Pattern-{counter}-{station}" as event, '
            f'"{activity_params.activity_name}" as activity, e1.timestamp as '
            f'ts_first, e2.timestamp as ts_second, {counter} as ll_pattern_num\n')
        query += f"insert into PartLowLevelPattern{station}Events;"
        query += "\n\n"
    query += f'@info(name="Detect-LowLevel-Pattern-{counter}")\n'
    query += 'from every ('

    for station in res_at_ts:
        query += (f'e{station} = PartLowLevelPattern{station}Events[ll_pattern_num '
                  f'== {counter}] and ')
    query = query[:-5]
    query += f') within {get_time_from_hz(sampling_freq)}\n'
    # choose smallest start timestamp and largest end timestamp as
    # overall start and end timestamp
    query += (f'select "LowLevel-Pattern-{counter}" as event, '
              f'"{activity_params.activity_name}" as activity, ')
    if len(res_at_ts) > 2:
        raise ValueError("More than two stations active at the same time is not supported (yet).")
    query += (f"ifThenElse(time:timestampInMilliseconds(e{res_at_ts[0]}.ts_first, "
              f"'yyyy-MM-dd HH:mm:ss.SS') "
              f"<= time:timestampInMilliseconds(e{res_at_ts[1]}.ts_first, "
              f"'yyyy-MM-dd HH:mm:ss.SS'), e{res_at_ts[0]}.ts_first, "
              f"e{res_at_ts[1]}.ts_first) as ts_first, "
              f"ifThenElse(time:timestampInMilliseconds(e{res_at_ts[0]}.ts_second, "
              f"'yyyy-MM-dd HH:mm:ss.SS') > "
              f"time:timestampInMilliseconds(e{res_at_ts[1]}.ts_second, "
              f"'yyyy-MM-dd HH:mm:ss.SS'), e{res_at_ts[0]}.ts_second, "
              f"e{res_at_ts[1]}.ts_second) as ts_second, ")
    query += f'{counter} as ll_pattern_num\n'
    query += "insert into DetectedLowLevelActivityEvents;\n"
    return query


def create_low_high_level_pattern_queries(changes: Changes,
                                          activity_params: AnnotationParams,
                                          sampling_freq: float,
                                          siddhi_config: SiddhiConfig) -> List[str]:
    """ Create the low and high level pattern queries for the siddhi app """
    queries = []
    counter = 1
    for idx, timestamp in enumerate(changes.timestamps):
        res_at_ts = changes.get_stations_at_ts(timestamp)
        if len(res_at_ts) <= 0:
            raise ValueError(
                f"No resources found at timestamp {res_at_ts}. This is a bug. Please report it.")
        if len(res_at_ts) == 1:
            # only one station active at the time
            llchanges = changes.get_changes_at_ts_station(timestamp, next(iter(res_at_ts)))
            queries.append(low_high_level_pattern_case_1(llchanges,
                                                         activity_params,
                                                         counter,
                                                         siddhi_config))
        else:
            queries.append(low_high_level_pattern_case_multi(timestamp,
                                                             changes,
                                                             activity_params,
                                                             counter,
                                                             sampling_freq,
                                                             siddhi_config))

        # High-Level Pattern queries
        queries.append(query_hlstream(
            activity_params,
            counter,
            idx == len(changes.timestamps) - 1)
        )

        if counter < len(changes):
            counter += 1

    return queries
