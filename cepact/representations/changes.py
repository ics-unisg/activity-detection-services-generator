""" Changes represent the changes in the signature, used later for detection. """
import os.path
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import List, Any, Dict

from .discretization import Discretization
from .signature import Signature, SignatureItem


@dataclass(kw_only=True, frozen=True)
class Change:
    """ Represents a change in a low-level pattern """
    station: str
    sensor: str
    timestamp: datetime
    prev_value: Any
    value: Any


class Changes():
    """ Changes class definition """

    def __init__(self, signatures: List[Signature], discretization: Discretization) -> None:
        self._signature_analyzed = False
        self._signatures = signatures
        self._changes: List[Change] = []
        self._analyze_signatures(discretization)

    def __len__(self) -> int:
        return len(self._changes)

    @cached_property
    def timestamps(self) -> List[datetime]:
        """ Get all distinct timestamps """
        return sorted({change.timestamp for change in self._changes})

    def _add_change(self, change: Change) -> None:
        self._changes.append(change)

    @cached_property
    def changes(self) -> List[Change]:
        """ Get all changes, sorted by timestamp """
        self._changes.sort(key=lambda x: x.timestamp)
        return self._changes

    def get_stations_at_ts(self, timestamp: datetime) -> List[str]:
        """ Get all stations for a specific timestamp, sorted by station """
        station_list = list({change.station for change
                             in self._changes if change.timestamp == timestamp})
        station_list.sort()
        return station_list

    def get_changes_at_ts_station(self, timestamp: datetime, station: str) -> List[Change]:
        """ Get all changes for a specific timestamp and station, sorted by sensor """
        chs = [change for change in self._changes if
               change.station == station and change.timestamp == timestamp]
        chs.sort(key=lambda x: x.sensor)
        return chs

    def get_changes_at_ts(self, timestamp: datetime) -> List[Change]:
        """ Get all changes for a specific ts, sorted by station and sensor """
        chs = [change for change in self._changes if change.timestamp == timestamp]
        chs.sort(key=lambda x: (x.station, x.sensor))
        return chs

    def create_changes_file(self, path: str, activity_name: str) -> None:
        """ Create a file with the changes.
        JSONL, each line is a JSON object with the following fields:
        - timestamp: timestamp of the change
        - changes: list of changes, all that happened at the same time
        The changes each have the fields:
        - station: station of the change
        - sensor: sensor of the change
        - prev_value: previous value of the sensor
        - value: new value of the sensor
        """
        filename = activity_name.replace(" ", "") + "_changes.jsonl"
        if not os.path.exists(os.path.join(path, "changes")):
            os.makedirs(os.path.join(path, "changes"))
        with open(os.path.join(path, "changes", filename), "w", encoding="utf-8") as file:
            for timestamp in self.timestamps:
                relevant_changes = [change for change in self._changes
                                    if change.timestamp == timestamp]
                file.write(f'{{"timestamp": "{
                timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
                }", "changes": [')
                for i, change in enumerate(relevant_changes):
                    file.write(f'{{"station": "{change.station}", "sensor": "{change.sensor}", '
                               f'"prev_value": {
                               change.prev_value if (isinstance(change.prev_value, (float, int)))
                               else f'"{change.prev_value}"'
                               }, '
                               f'"value": '
                               f'{change.value if (isinstance(change.value, (float, int)))
                               else f'"{change.value}"'}}}')
                    if i != len(relevant_changes) - 1:
                        file.write(", ")
                file.write("]}\n")

    def _analyze_signatures(self, discretization: Discretization) -> None:
        """ Analyze the signature for changes """
        if len(self._signatures) != 1:
            raise ValueError("Currently only one annotation/signature is supported per activity")
        if self._signature_analyzed:
            raise ValueError("Signature already analyzed")
        self._signature_analyzed = True
        sig = self._signatures[0]
        sig_dict = sig.get_sigs_by_ts_station()
        timestamps = list(sig_dict.keys())
        prev_entries_stat_sens: Dict[str, Dict[str, List[SignatureItem]]] = {"": {}}
        timestamps.sort()
        for ts in timestamps:
            res: str
            current_list_sigs: List[SignatureItem]
            for res, current_list_sigs in sig_dict[ts].items():
                if res not in prev_entries_stat_sens:
                    prev_entries_stat_sens[res] = {}
                sig_item: SignatureItem
                for sig_item in current_list_sigs:
                    if sig_item.sensor not in prev_entries_stat_sens[res]:
                        prev_entries_stat_sens[res][sig_item.sensor] = [sig_item]
                        continue
                    prev = prev_entries_stat_sens[res][sig_item.sensor][-1] if len(
                        prev_entries_stat_sens[res][sig_item.sensor]) > 0 else None
                    if prev is None:
                        raise ValueError("Previous entry not found")
                    if (sig_item.value != prev.value
                            and not discretization.is_discretized(sig_item.sensor)):
                        self._add_change(Change(station=res,
                                                sensor=sig_item.sensor,
                                                timestamp=sig_item.timestamp,
                                                prev_value=prev.value,
                                                value=sig_item.value))
                    if (sig_item.value != prev.value and sig_item.sensor
                            and discretization.is_discretized(sig_item.sensor)
                            and (discretization.discretize(sig_item.sensor, prev.value)
                                 != discretization.discretize(sig_item.sensor, sig_item.value))):
                        # the last part above checks if the change is
                        # from one discretization to another
                        self._add_change(Change(station=res,
                                                sensor=sig_item.sensor,
                                                timestamp=sig_item.timestamp,
                                                prev_value=discretization.discretize(
                                                    sig_item.sensor,
                                                    prev.value),
                                                value=discretization.discretize(
                                                    sig_item.sensor,
                                                    sig_item.value)))
                    prev_entries_stat_sens[res][sig_item.sensor].append(sig_item)
