""" The grafana fetcher that fetcher both the general activity params and ignore sensors. """
import datetime
from datetime import datetime as dt
from typing import List, Dict, Tuple, Any, Union

import requests
from requests.structures import CaseInsensitiveDict

from ..abstract_fetchers import AnnotationParamFetcher, IgnoreSensorFetcher
from ...representations import AnnotationParams


def _handle_text_annotation(text: str) -> Tuple[str, str]:
    """ Get activity name and lifecycle from the text annotation.

    Args: Text annotation from Grafana.
    Returns: Tuple of lifecycle and activity name, in that order.
        """
    stat_pos = text.find("_")  # Naming scheme for Text: START/END_Activity name
    return text[:stat_pos], text[stat_pos + 1:].replace("-", " ")


def _handle_ignore_tags(tags: List[str], act_id: str) -> Tuple[
    List[str], Dict[str, List[str]]]:
    sensors_to_ignore_general = []
    sensors_to_ignore_specific = {}
    for tag in tags:
        sensor_to_ignore = tag.split("-")[1]
        if tag.split("-")[0] == "ignoregen":
            if sensor_to_ignore not in sensors_to_ignore_general:
                sensors_to_ignore_general.append(sensor_to_ignore)
        if tag.split("-")[0] == "ignore":
            if act_id not in sensors_to_ignore_specific:
                sensors_to_ignore_specific[act_id] = [sensor_to_ignore]
            elif sensor_to_ignore not in sensors_to_ignore_specific[act_id]:
                sensors_to_ignore_specific[act_id].append(sensor_to_ignore)
    return sensors_to_ignore_general, sensors_to_ignore_specific


def _extend_sensor_ignore_dicts(original_gen: List[str],
                                original_spec: Dict[str, List[str]],
                                tags: List[str],
                                act_id: str) \
        -> Tuple[List[str], Dict[str, List[str]]]:
    """ Merge two sensor ignore dicts.

    Returns the merged ignore sensors.
    """
    new_gen, new_spec = _handle_ignore_tags(tags, act_id)
    original_gen += new_gen
    original_gen = list(set(original_gen))
    for key, value in new_spec.items():
        if key in original_spec:
            original_spec[key] += value
            original_spec[key] = list(set(original_spec[key]))
        else:
            original_spec[key] = value
    return original_gen, original_spec


class GrafanaFetcher(AnnotationParamFetcher, IgnoreSensorFetcher):
    """ Fetches the general activity parameters and ignore sensors from Grafana.
    Generates CEP apps and signatures based on Grafana annotations.
    Annotation rules:
    - Name of _start_ annotation `START_Activity-Name-Here`
      - Note, that the activity name must match the activity name in the Camunda log for evaluation
    - Analogous for `END_`
    - First tag must be some unique key
    - Second tag must be `activity`
    - Third tag must be _station_ code, e.g. `OV_1`, `HYGIENE_STATION`
      - For detection apps spanning multiple components/stations/resources, this
      tag needs to include all of them separated by a hyphen, e.g. `HYGIENE_STATION-LEFT_DONATION`
    - Fourth tag and all others behind can be used to _ignore_ certain _sensors_ in the activity
    signature and for the creation
    of the CEP Siddhi apps (can be either at START or END annotation):
      - add `ignore-SENSORNAME` to the tag to ignore the sensor for only this
      activity-signature/-app, e.g. `ignore-light_l5`
      - if you want to ignore a certain sensor in general, i.e. for all activities, and don't
      want to repeat the tag, just
      tag it with `ignoregen-SENSORENAME`, e.g. `ignoregen-movepos_x`
  """

    def __init__(self,
                 url: str,
                 auth: str) -> None:
        self._grafana_url = url
        self._grafana_auth = auth
        self._activity_params: Union[None, List[AnnotationParams]] = None
        self._sensors_to_ignore_general: Union[None, List[str]] = None
        self._sensors_to_ignore_specific: Union[None, Dict[str, List[str]]] = None

    def get_annotation_params(self) -> List[AnnotationParams]:
        """ Fetch the general activity parameters. """
        if self._activity_params is None:
            (self._activity_params,
             self._sensors_to_ignore_general,
             self._sensors_to_ignore_specific) = self._get_grafana_data()
        return self._activity_params

    def get_ignore_sensors(self) -> Dict[str, List[str]]:
        """ Fetch the ignore sensors. """
        if self._sensors_to_ignore_general is None or self._sensors_to_ignore_specific is None:
            (self._activity_params,
             self._sensors_to_ignore_general,
             self._sensors_to_ignore_specific) = self._get_grafana_data()
        return {"general": self._sensors_to_ignore_general,
                **self._sensors_to_ignore_specific}

    def _get_grafana_data(self) -> Tuple[List[AnnotationParams], List[str], Dict[str, List[str]]]:
        """
        Get annotations from Grafana (activities)
        :return: activities, in a dict with their IDs as a key (id from Grafana annotation)
        """
        annotations = requests.get(self._grafana_url + '/api/annotations?tags=activity',
                                   headers=CaseInsensitiveDict(
                                       {"Accept": "application/json",
                                        "Authorization": "Bearer " + self._grafana_auth}),
                                   timeout=5).json()
        activities: Dict[str, Dict[str, Any]] = {}
        activities_list: List[AnnotationParams] = []
        sensors_to_ignore_general: List[str] = []
        sensors_to_ignore_specific: Dict[str, List[str]] = {}

        for anno in annotations:
            if "det_activity" in anno['tags']:
                continue
            lifecycle, activity_name = _handle_text_annotation(anno['text'])
            time = dt.fromtimestamp(anno['time'] / 1000, datetime.UTC)
            act_id = anno['tags'][
                0]  # Tags for two events belonging together need to have the same
            # unique tag (id); tag[0] = id; tag[1] = "activity"; tag[2] = component
            anno_type = anno['tags'][1]
            sensors_to_ignore_general, sensors_to_ignore_specific = _extend_sensor_ignore_dicts(
                sensors_to_ignore_general,
                sensors_to_ignore_specific,
                anno['tags'][3:],
                act_id)
            if anno_type == "activity":
                resources = anno['tags'][2].split("-")
                resources = [resource.upper() for resource in resources]
                if act_id in activities:
                    activity = activities[act_id]
                    if lifecycle == "START":
                        activity["start"] = time
                    elif lifecycle == "END":
                        if activity["start"] == time:
                            raise ValueError(f"Annotated lifecycle {lifecycle} "
                                             f"has the same timestamp as the start.")
                        activity["end"] = time
                    else:
                        raise ValueError(f"Annotated lifecycle {lifecycle} not recognized."
                                         f"Lifecycle must be either START or END.")
                    if (activity["start"] is None) or (activity["end"] is None):
                        raise ValueError(f"Annotation {activity['annotationId']} "
                                         f"has no start or end.")
                    activities_list.append(AnnotationParams(activity_name=activity["name"],
                                                            start=activity["start"],
                                                            end=activity["end"],
                                                            stations=activity["stations"],
                                                            annotation_id=activity[
                                                                "annotation_id"]))
                    # remove the activity from the dict
                    del activities[act_id]
                else:
                    activities[act_id] = {"annotation_id": act_id,
                                          "name": activity_name,
                                          "start": time if lifecycle == "START" else None,
                                          "end": time if lifecycle == "END" else None,
                                          "stations": resources,
                                          "grafana_dashboard": (
                                              anno['dashboardUID'], anno['panelId'])}

        # raise error of activities are not empty
        if activities:
            raise ValueError(f"Activities not properly annotated: {activities}.")
        return activities_list, sensors_to_ignore_general, sensors_to_ignore_specific
