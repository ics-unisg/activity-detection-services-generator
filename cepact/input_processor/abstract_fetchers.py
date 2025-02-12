""" This module contains abstract classes for fetchers. """
from abc import ABC, abstractmethod
from typing import List, Dict

from ..representations import AnnotationParams, Discretization, Signature


class SignatureFetcher(ABC):
    """ Abstract class for fetching time series data. """

    @abstractmethod
    def get_signature(self,
                      annotation_params: AnnotationParams,
                      ignore_sensors: Dict[str, List[str]],
                      sampling_freq: float) -> Signature:
        """ Fetch the signature for a certain activity annotation. """


class AnnotationParamFetcher(ABC):
    """ Abstract class for fetching activity parameters. """

    @abstractmethod
    def get_annotation_params(self) -> List[AnnotationParams]:
        """ Fetch the annotation parameters. """


class DiscretizationFetcher(ABC):
    """ Abstract class for fetching discretization. """

    @abstractmethod
    def get_discretization(self) -> Discretization:
        """ Fetch the discretization. """


class IgnoreSensorFetcher(ABC):
    """ Abstract class for fetching sensors to ignore. """

    @abstractmethod
    def get_ignore_sensors(self) -> Dict[str, List[str]]:
        """ Fetch the ignore sensors.

        Structure of return dict:
        {
            "general": List[str],
            "activitaName1": List[str],
            "activitaName2": List[str],
            ...
         """
