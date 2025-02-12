""" Here we orchestrate the generation of the output files of the application. """
import os
from datetime import datetime
from typing import List

from .abstract_instance_level_det_query import InstanceLevelDetQuery
from .siddhi_producer import SiddhiProducer
from ..conf import DAGConfig
from ..representations import Activity, Discretization


class OutputProducer():
    """ Orchestrates the generation of the output files of the application. """

    def __init__(self, discretization: Discretization, conf: DAGConfig):
        # establish logging for debugging
        self._log_store: List[str] = []
        self._log('OutputProducer initialized')
        self._discretization = discretization
        self._conf = conf
        self._check_instance_level_detection_queries()
        self._active_il_queries: List[InstanceLevelDetQuery] = self._conf.det_methods
        self._prep_output_dir()

    def _log(self, msg: str) -> None:
        """ Log a message. """
        current_time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self._log_store.append(f'{current_time}: {msg}')

    def _prep_output_dir(self) -> None:
        """ Prepare the output directory. """
        if not os.path.exists(self._conf.out_dir):
            os.makedirs(self._conf.out_dir)
        # make sure the output directory is empty, delete any files and also files in nested dirs
        for root, dirs, files in os.walk(self._conf.out_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    def _check_instance_level_detection_queries(self) -> None:
        # check that self._conf.det_methods does not
        # contain multiple instances of the same query (class)
        query_classes = [query.__class__ for query in self._conf.det_methods]
        if len(query_classes) != len(set(query_classes)):
            raise ValueError("Multiple instances of the same detection query class.")

    def write_signature(self, activity: Activity) -> None:
        """ Write the signature to the output directory. """
        if not self._enough_change_ts(activity):
            self._log(
                f'Writing of signature failed, not enough distinct change '
                f'timestamps {activity.get_annotation_params().activity_name}')
            return
        signatures = activity.get_signature()
        for signature in signatures:
            signature.create_signature_file(self._conf.out_dir)

    def write_changes(self, activity: Activity) -> None:
        """ Write the changes to the output directory. """
        if not self._enough_change_ts(activity):
            self._log(
                f'Writing of changes failed, not enough distinct change '
                f'timestamps {activity.get_annotation_params().activity_name}')
            return
        changes = activity.get_changes(self._discretization)
        changes.create_changes_file(self._conf.out_dir,
                                    activity.get_annotation_params().activity_name)

    def write_app(self, activity: Activity) -> None:
        """ Write the application to the output directory. """
        if not self._enough_change_ts(activity):
            self._log(
                f'Writing of Siddhi App failed, not enough '
                f'distinct change timestamps {activity.get_annotation_params().activity_name}')
            return
        siddhi_producer = SiddhiProducer(il_queries=self._active_il_queries,
                                         activity=activity,
                                         discretization=self._discretization,
                                         dag_conf=self._conf)
        siddhi_producer.write_siddhi_app()

    def _enough_change_ts(self, activity: Activity) -> bool:
        """ Check if there are enough change timestamps to detect the activity. """
        return activity.get_number_distinct_change_timestamps(self._discretization) > 0

    def write_log(self) -> None:
        """ Write the log to the output directory. """
        with open(os.path.join(self._conf.out_dir, "log.txt"),
                  'w',
                  encoding="utf-8") as file:
            for line in self._log_store:
                file.write(f'{line}\n')
