""" Activity class definition """
from typing import List

from .annotation_params import AnnotationParams
from .changes import Changes
from .discretization import Discretization
from .signature import Signature


class Activity():
    """ Activity class definition """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 annotation_params: AnnotationParams,
                 signatures: List[Signature],
                 sensors_to_ignore: List[str]):
        """ Initialize Activity object """
        self._annotation_params = annotation_params
        self._signature = signatures
        self._sensors_to_ignore = sensors_to_ignore

    def get_changes(self, discretization: Discretization) -> Changes:
        """ Get changes """
        return Changes(self._signature, discretization)

    def get_annotation_params(self) -> AnnotationParams:
        """ Get annotation parameters """
        return self._annotation_params

    def get_signature(self) -> List[Signature]:
        """ Get signature """
        return self._signature

    def get_number_distinct_change_timestamps(self, discretization: Discretization) -> int:
        """ Get number of distinct change timestamps """
        return len(self.get_changes(discretization).timestamps)
