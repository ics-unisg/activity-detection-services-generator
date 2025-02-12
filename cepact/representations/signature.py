""" Relevant sensor values in annotated data. """
import json
import os
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any, List, Dict, Union, Set

from .discretization import Discretization


@dataclass(frozen=True, kw_only=True)
class SignatureItem:
    """ Represents an item of an annotation signature """
    station: str
    timestamp: datetime
    sensor: str
    value: Any


class Signature():
    """ Represents the signature of an annotation. """

    def __init__(self,
                 activity_name: str,
                 annotation_id: str,
                 signature: List[SignatureItem],
                 sensors_types_per_station: Dict[str, Dict[str, type]]):
        self._signature: List[SignatureItem] = signature
        self.activity_name = activity_name
        self.annotation_id = annotation_id
        self._sensors_types_per_station: Dict[str, Dict[str, type]] = sensors_types_per_station

    def __len__(self) -> int:
        return len(self._signature)

    @cached_property
    def timestamps(self) -> List[datetime]:
        """ Get all timestamps """
        return sorted({sig.timestamp for sig in self._signature})

    def get_sensors_with_types_per_station(self) -> Dict[str, Dict[str, type]]:
        """ Get sensor types per station """
        return self._sensors_types_per_station

    def get_sensors_with_discretized_types_per_station(self, discretization: Discretization) -> \
            Dict[str, Dict[str, type]]:
        """ Get sensor types per station """
        sensors_with_discretized_types_per_station: Dict[str, Dict[str, type]] = {}
        for station, sensors_with_types in self._sensors_types_per_station.items():
            sensors_with_discretized_types_per_station[station] = {}
            for sensor, sensor_type in sensors_with_types.items():
                if discretization.is_discretized(sensor):
                    sensors_with_discretized_types_per_station[station][
                        sensor] = discretization.discretized_type(sensor)
                else:
                    sensors_with_discretized_types_per_station[station][sensor] = sensor_type
        return sensors_with_discretized_types_per_station

    def get_signature_items(self) -> List[SignatureItem]:
        """ Get all signature items, sorted by timestamp """
        return sorted(self._signature, key=lambda x: x.timestamp)

    def get_sigs_at_ts(self, timestamp: datetime) -> List[SignatureItem]:
        """ Get all signature items at timestamp """
        return [sig for sig in self._signature if sig.timestamp == timestamp]

    def get_sigs_at_ts_station(self, timestamp: datetime, station: str) -> List[SignatureItem]:
        """ Get all signature items at timestamp and station """
        return [sig for sig in self._signature
                if sig.timestamp == timestamp and sig.station == station]

    def get_sigs_by_ts_station(self) -> Dict[datetime, Dict[str, List[SignatureItem]]]:
        """
        Get all signature items (sensor readings) by timestamp, separated by station.
        :param signature:
        :return:
        """
        sigs_by_ts: Dict[datetime, Dict[str, List[SignatureItem]]] = {}
        for sig_item in self._signature:
            if sig_item.timestamp not in sigs_by_ts:
                sigs_by_ts[sig_item.timestamp] = {}
            if sig_item.station not in sigs_by_ts[sig_item.timestamp].keys():
                sigs_by_ts[sig_item.timestamp][sig_item.station] = []
            sigs_by_ts[sig_item.timestamp][sig_item.station].append(sig_item)
        return sigs_by_ts

    @cached_property
    def stations(self) -> List[str]:
        """ Get all stations """
        stations = []
        for sig_item in self._signature:
            if sig_item.station not in stations:
                stations.append(sig_item.station)
        stations.sort()
        return stations

    def create_signature_file(self,
                              path: str) -> None:
        """
        Create a signature file for the annotation.
        :param path: Path to the directory where the signature file is to be created.
        """
        # empty the destination file
        filename = (self.activity_name.replace(" ", "") + "_"
                    + self.annotation_id.replace(" ", "") + "_signature.jsonl")
        sigs_by_ts_res = self.get_sigs_by_ts_station()
        sen_dict_keys_prior: Dict[str, List[str]] = {}
        ts: datetime
        stations: Dict[str, List[SignatureItem]]
        for ts, stations in sigs_by_ts_res.items():
            # turn timestamp to string with format "2023-02-06 14:00:05.04", only two decimals
            ts_dict: Dict[str, Union[datetime, str, List[Dict[str, Any]]]] = {
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4],
                "measurements": []
            }
            station: str
            signatures: List[SignatureItem]
            for station, signatures in stations.items():
                sen_dict = {}
                sig: SignatureItem  # annotating the type of sig
                for sig in signatures:
                    sen_dict[sig.sensor] = sig.value
                    if sig.station not in sen_dict:
                        sen_dict_keys_prior[sig.station] = list(sen_dict.keys())
                    if sen_dict_keys_prior[sig.station] != list(sen_dict.keys()):
                        raise ValueError(f"Sensor values for {sig.station} are not consistent")
                    sen_dict_keys_prior[sig.station] = list(sen_dict.keys())
                s = {
                    "station": station,
                    **sen_dict
                }
                if isinstance(ts_dict["measurements"], list):
                    ts_dict["measurements"].append(s)
                else:
                    raise ValueError("Measurements must be a list")
            # create directory if not exists
            if not os.path.exists(os.path.join(path, "signatures")):
                os.makedirs(os.path.join(path, "signatures"))
            with open(os.path.join(path, "signatures", filename), 'a',
                      encoding="utf-8") as outfile:
                json.dump(ts_dict, outfile)
                outfile.write('\n')

    def get_exemplary_datapoints_per_resource(self) -> Dict[str, Dict[str, Any]]:
        """ Get exemplary data points per station. """
        data_points: Dict[str, Dict[str, Any]] = {}
        sen_dict_keys_prior: Dict[str, List[str]] = {}
        sigs_by_ts_res = self.get_sigs_by_ts_station()
        ts: datetime
        stations: Dict[str, List[SignatureItem]]
        for ts, stations in sigs_by_ts_res.items():
            station: str
            signatures: List[SignatureItem]
            for station, signatures in stations.items():
                sen_dict = {}
                sig: SignatureItem  # annotating the type of sig
                for sig in signatures:
                    sen_dict[sig.sensor] = sig.value
                    if sig.station not in sen_dict:
                        sen_dict_keys_prior[sig.station] = list(sen_dict.keys())
                    if sen_dict_keys_prior[sig.station] != list(sen_dict.keys()):
                        raise ValueError(f"Sensor values for {sig.station} are not consistent")
                    sen_dict_keys_prior[sig.station] = list(sen_dict.keys())
                s = {
                    "station": station,
                    **sen_dict
                }
                if station not in data_points:
                    data_points[station] = {
                        "timestamp": ts,
                        **s
                    }
        return data_points


class SignatureBuilder():
    """ Builder for signature of an annotation. """

    def __init__(self, activity_name: str, annotation_id: str, sampling_freq: float) -> None:
        self._signature: List[SignatureItem] = []
        self.activity_name = activity_name
        self.annotation_id = annotation_id
        self._sampling_freq = sampling_freq
        self._sensors_types_per_station: Dict[str, Dict[str, type]] = {}

    def __len__(self) -> int:
        return len(self._signature)

    def add_signature_item(self, sig_item: SignatureItem) -> None:
        """ Add a signature item to the signature.

        Raises: ValueError if sensor value type is not consistent with
            previously added signature items."""
        self._signature.append(sig_item)
        if sig_item.station not in self._sensors_types_per_station:
            self._sensors_types_per_station[sig_item.station] = {
                "timestamp": str
            }
        if sig_item.sensor not in self._sensors_types_per_station[sig_item.station]:
            self._sensors_types_per_station[sig_item.station][sig_item.sensor] = type(
                sig_item.value)
        if not isinstance(sig_item.value,
                          self._sensors_types_per_station[sig_item.station][sig_item.sensor]):
            raise ValueError(
                f"Sensor value type for sensor {sig_item.sensor} at "
                f"station {sig_item.station} is not consistent.")
        self._signature.sort(key=lambda x: x.timestamp)

    def _check_if_stations_have_consistent_sensors(self) -> None:
        """ Check if all stations have the same sensors at all timestamps they are present.

         Note that consistent sensor types are already validated during adding signature items.
         Raises: ValueError if sensors are not consistent."""
        station_sensors: Dict[str, Set[str]] = {}
        timestamps = {sig.timestamp for sig in self._signature}
        for timestamp in timestamps:
            stations = {sig.station for sig in self._signature if sig.timestamp == timestamp}
            for station in stations:
                sensors_at_station_timestamp = {sig.sensor for sig in self._signature if
                                                sig.station == station
                                                and sig.timestamp == timestamp}
                if station not in station_sensors:
                    station_sensors[station] = sensors_at_station_timestamp
                    continue
                if station_sensors[station] != sensors_at_station_timestamp:
                    raise ValueError(
                        f"Stations do not have the same sensors at timestamp {timestamp}")

    def _get_all_ts_wo_station(self) -> Dict[str, List[datetime]]:
        """ Get all timestamps without those only associated with a station. """
        all_timestamps_without_station: Dict[str, List[datetime]] = {}
        # create empty lists for all stations
        for sig in self._signature:
            if sig.station not in all_timestamps_without_station:
                all_timestamps_without_station[sig.station] = []
        # add timestamps to lists
        for sig in self._signature:
            for station, ts_wo_curr_st in all_timestamps_without_station.items():
                if sig.station != station:
                    ts_wo_curr_st.append(sig.timestamp)
        for station, timestamps in all_timestamps_without_station.items():
            all_timestamps_without_station[station] = sorted(timestamps)
        return all_timestamps_without_station

    def _get_all_ts_for_station(self) -> Dict[str, List[datetime]]:
        all_timestamps_for_station: Dict[str, List[datetime]] = {}
        for sig in self._signature:
            if sig.station not in all_timestamps_for_station:
                all_timestamps_for_station[sig.station] = []
            all_timestamps_for_station[sig.station].append(sig.timestamp)
        return all_timestamps_for_station

    def _combine_close_timestamps(self) -> None:
        """ Combines close timestamps to one timestamp, based on the sampling frequency. """
        # only if stations are different,
        # we can combine timestamps (otherwise we might "erase" changes)
        # get all timestamps, ascending; create mapping to new timestamps
        all_timestamps_without_station: Dict[str, List[datetime]] = self._get_all_ts_wo_station()
        all_timestamps_for_station: Dict[str, List[datetime]] = self._get_all_ts_for_station()
        new_timestamps_per_station: Dict[str, Dict[datetime, datetime]] = {}
        # for each timestamp, if distance to previous timestamp in other stations
        # is less than half the sampling frequency, combine them (make the current timestamp the
        # same as the previous one
        for station, timestamps in all_timestamps_without_station.items():
            new_timestamps_per_station[station] = {}
            curr_idx = 0
            for i, ts in enumerate(all_timestamps_for_station[station]):
                if i == 0 or len(timestamps) == 0:
                    new_timestamps_per_station[station][ts] = ts
                    continue
                while (ts - timestamps[curr_idx]).total_seconds() > (1 / self._sampling_freq) / 2:
                    if curr_idx < len(timestamps) - 1:
                        curr_idx += 1
                    else:
                        break
                if (0 <= (ts - timestamps[curr_idx]).total_seconds()
                        <= (1 / self._sampling_freq) / 3):
                    new_timestamps_per_station[station][ts] = timestamps[curr_idx]
                else:
                    new_timestamps_per_station[station][ts] = ts

        # remap the timestamps in the signature items (we need to create new signature items)
        new_signature = []
        for sig in self._signature:
            new_signature.append(SignatureItem(
                station=sig.station,
                timestamp=new_timestamps_per_station[sig.station][sig.timestamp],
                sensor=sig.sensor,
                value=sig.value
            ))
        self._signature = new_signature

    def build(self) -> Signature:
        """ Complete the signature. """
        self._combine_close_timestamps()
        self._check_if_stations_have_consistent_sensors()
        sensor_types_per_station = self._sensors_types_per_station.copy()
        for station, sensors in sensor_types_per_station.items():
            # sort by sensor name
            # add timestamp as first element
            sensor_types_per_station[station] = {'timestamp': str} | dict(sorted(sensors.items()))
        return Signature(activity_name=self.activity_name,
                         annotation_id=self.annotation_id,
                         signature=self._signature.copy(),
                         sensors_types_per_station=sensor_types_per_station.copy())
