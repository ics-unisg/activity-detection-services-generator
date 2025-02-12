""" This module contains the InputProcessor class. """
from typing import List, TYPE_CHECKING, Dict

from ..representations import Activity, Discretization, Signature

if TYPE_CHECKING:
    from ..conf import DAGConfig


class InputProcessor():
    """ Extract important information from the input data. """

    def __init__(self, dag_conf: 'DAGConfig') -> None:
        self._dag_conf = dag_conf
        self._annotation_param_fetcher = self._dag_conf.annotation_param_fetcher
        self._ignore_sensor_fetcher = self._dag_conf.ignore_sensor_fetcher
        self._discretization_fetcher = self._dag_conf.discretization_fetcher
        self._signature_fetcher = self._dag_conf.signature_fetcher

    def get_activities(self) -> List[Activity]:
        """ Get activities from the input data. """
        # create suitable concrete fetchers
        # call annotation processor and timeseries processor
        # first call annotation processor overall
        ann_params = self._annotation_param_fetcher.get_annotation_params()
        sensor_ignores = self._ignore_sensor_fetcher.get_ignore_sensors()
        # then call timeseries processor (somehow related to the annotated activities)

        signatures: Dict[str, List[Signature]] = {}
        for annotation in ann_params:
            if annotation.activity_name not in signatures:
                signatures[annotation.activity_name] = []
            signatures[annotation.activity_name].append(
                self._signature_fetcher.get_signature(annotation,
                                                      sensor_ignores,
                                                      self._dag_conf.sampling_freq)
            )
        # create activity objects --> use annotations and ts data to create signatures and changes
        activities = []
        general_sensors_to_ignore = sensor_ignores["general"]
        for annotation in ann_params:
            act_name = annotation.activity_name
            sensor_to_ignore_act = sensor_ignores[act_name] if act_name in sensor_ignores else []
            signature = signatures[act_name]
            activity = Activity(annotation_params=annotation,
                                signatures=signature,
                                sensors_to_ignore=general_sensors_to_ignore + sensor_to_ignore_act)
            activities.append(activity)
        # return list of activities
        return activities

    def get_discretization(self) -> Discretization:
        """ Extract the discretization. """
        return self._discretization_fetcher.get_discretization()
