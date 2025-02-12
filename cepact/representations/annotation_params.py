""" This module contains the ActivityParams class. """
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import List


@dataclass(frozen=True, kw_only=True)
class AnnotationParams():
    """ ActivityParams class definition

    Attributes:
        annotation_id: str: unique identifier for the annotation
            (there might be more than one annotation for the same activity)
    """
    annotation_id: str
    activity_name: str
    start: datetime
    end: datetime
    stations: List[str]

    @cached_property
    def stations_str(self) -> str:
        """ Get the stations as a string. """
        self.stations.sort()
        return "-".join(self.stations)
