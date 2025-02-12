""" This module is responsible for orchestrating the generation of the Siddhi apps. """
import os.path
from typing import List

from .abstract_instance_level_det_query import InstanceLevelDetQuery
from .sink_source_pattern_helper import define_source, define_sink, define_discretization_query, \
    create_high_to_low_helper, create_low_high_level_pattern_queries
from ..conf import DAGConfig
from ..representations import Activity, Signature, Discretization


class SiddhiProducer():
    """ Orchestrates the generation of Siddhi apps. """

    def __init__(self,
                 il_queries: List[InstanceLevelDetQuery],
                 activity: Activity,
                 discretization: Discretization,
                 dag_conf: DAGConfig):
        self._il_queries = il_queries
        self._act = activity
        self._conf = dag_conf
        self._discretization = discretization

    def _get_header(self) -> str:
        """ Get the header of the Siddhi app. (AppName)"""
        return f'@App:name(\'Detect{self._act.get_annotation_params()
        .activity_name.replace(" ", "")}App\')'

    def _get_sources(self) -> str:
        """ Get the sources of the Siddhi app. """
        signature: Signature = self._act.get_signature()[0]
        relevant_stations = signature.stations
        source_str = ""
        for idx, station in enumerate(relevant_stations):
            source_str += define_source(station,
                                        self._act.get_annotation_params().activity_name,
                                        signature.get_sensors_with_types_per_station()[station],
                                        signature.get_sensors_with_discretized_types_per_station(
                                            discretization=self._discretization)[
                                            station],
                                        self._conf.siddhi_config)
            if not idx == len(relevant_stations) - 1:
                source_str += "\n\n"
        return source_str

    def _get_sinks(self) -> str:
        """ Get the sinks of the Siddhi app. """
        return define_sink(self._act,
                           self._conf.siddhi_config)

    def _get_discretization_helpers(self) -> str:
        """ Get the discretization helpers of the Siddhi app. """
        discr_helper_str = ""
        for idx, station in enumerate(self._act.get_signature()[0].stations):
            discr_helper_str += define_discretization_query(
                station,
                self._discretization,
                self._act.get_signature()[0].get_sensors_with_discretized_types_per_station(
                    discretization=self._discretization)[station],
                self._conf.siddhi_config)
            if not idx == len(self._act.get_signature()[0].stations) - 1:
                discr_helper_str += "\n\n"
        return discr_helper_str

    def _get_change_queries(self) -> str:
        """ Get the change queries of the Siddhi app and the accompanying high to low helper. """
        queries = ""
        queries += create_high_to_low_helper() + "\n"
        l_h_quers = create_low_high_level_pattern_queries(
            self._act.get_changes(self._discretization), self._act.get_annotation_params(),
            self._conf.sampling_freq,
            self._conf.siddhi_config)
        for query in l_h_quers:
            queries += "\n" + query
        return queries

    def _get_instance_level_detection_queries(self) -> str:
        """ Get the instance level detection queries of the Siddhi app. """
        num_changes = len(self._act.get_changes(self._discretization).timestamps)
        act_name = self._act.get_annotation_params().activity_name
        queries = ""
        for idx, il_query in enumerate(self._il_queries):
            queries += il_query.generate(num_changes, act_name)
            if not idx == len(self._il_queries) - 1:
                queries += "\n\n"
        return queries

    def write_siddhi_app(self) -> None:
        """ Write the Siddhi app to the output directory. """
        filename = ("Detect" + self._act.get_annotation_params()
                    .activity_name.replace(" ", "") + "App.siddhi")
        # create directory if not exists
        if not os.path.exists(os.path.join(self._conf.out_dir, "apps")):
            os.makedirs(os.path.join(self._conf.out_dir, "apps"))
        with open(os.path.join(self._conf.out_dir, "apps", filename), "w",
                  encoding="utf-8") as f:
            f.write(self._get_header())
            f.write("\n\n")
            f.write(self._get_sources())
            f.write("\n\n")
            f.write(self._get_sinks())
            f.write("\n\n")
            f.write(self._get_discretization_helpers())
            f.write("\n\n")
            f.write(self._get_change_queries())
            f.write("\n\n")
            f.write(self._get_instance_level_detection_queries())
            f.write("\n")
