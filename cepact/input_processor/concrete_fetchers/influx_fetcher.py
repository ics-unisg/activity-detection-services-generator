""" The influx fetcher, that fetches the iot data from the influx db. """
from typing import List, Dict

from influxdb_client import InfluxDBClient  # type: ignore

from ..abstract_fetchers import SignatureFetcher
from ...representations import AnnotationParams, Signature, SignatureItem, SignatureBuilder


class InfluxFetcher(SignatureFetcher):
    """ Fetches the iot data from the influxDB.

    The IoT time series data to extract the activity signature from can be stored on InfluxDB.
    The data should be stored in a bucket with and the naming should be specified in
    the `influx_station_bucket_map` in the `DAGConfig`."""

    def __init__(self,
                 url: str,
                 auth: str,
                 org: str,
                 station_bucket_map: Dict[str, str]) -> None:
        self._influx_url = url
        self._influx_auth = auth
        self._influx_org = org
        self._influx_station_bucket_map = station_bucket_map

    def get_signature(self, annotation_params: AnnotationParams,
                      ignore_sensors: Dict[str, List[str]],
                      sampling_freq: float) -> Signature:
        """
        Extract activity signatures for given activity from InfluxDB
        :param details: enable console printouts, default off
        :param activity: Relevant activity
        :return: Signature of the activity
        """
        signature_builder: SignatureBuilder = SignatureBuilder(
            activity_name=annotation_params.activity_name,
            annotation_id=annotation_params.annotation_id,
            sampling_freq=sampling_freq
        )
        rel_sensors_ignore = ignore_sensors["general"].copy()
        if annotation_params.activity_name in ignore_sensors:
            rel_sensors_ignore += ignore_sensors[annotation_params.activity_name]
        rel_sensors_ignore = list(set(rel_sensors_ignore))

        with InfluxDBClient(url=self._influx_url,
                            token=self._influx_auth,
                            org=self._influx_org) as client:
            for resource in annotation_params.stations:
                query_api = client.query_api()
                query = (
                    f'from(bucket:"'
                    f'{self._influx_station_bucket_map[resource]}")'
                    f'|> range(start: {annotation_params.start.strftime("%Y-%m-%dT%H:%M:%SZ")}, '
                    f'stop: {annotation_params.end.strftime("%Y-%m-%dT%H:%M:%SZ")})'
                    f'|> yield()')
                result = query_api.query(org=self._influx_org,
                                         query=query)
                for table in result:
                    for record in table.records:
                        if record.get_field() not in rel_sensors_ignore:
                            signature_builder.add_signature_item(SignatureItem(
                                station=resource,
                                timestamp=record['_time'],
                                sensor=record.get_field(),
                                value=record.get_value())
                            )
        client.close()

        return signature_builder.build()
