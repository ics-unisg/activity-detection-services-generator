""" Fetch annotation parameters from local files. """
import os
from typing import List

import pandas as pd

from ..abstract_fetchers import AnnotationParamFetcher
from ...representations import AnnotationParams


class LocalAnnotationParamFetcher(AnnotationParamFetcher):
    """ Get annotation params from local files.

    Imports the annotation parameters from a local file. The file should be in the format of
    a CSV file with the following columns:
    - annotation_id: unique identifier for the annotation
    - activity_name: name of the activity
    - start: start time of the annotation
    - end: end time of the annotation
    - stations: stations that are relevant for the activity
    """

    def __init__(self, local_in_dir: str) -> None:
        self._local_in_dir = local_in_dir

    def _validate_annotation_params(self, annotation_params: pd.DataFrame) -> None:
        """ Check if the annotation parameters are valid.

        | Column        | Constraints                       |
        | ------------- | --------------------------------- |
        | annotation_id | must be unique                    |
        | activity_name | can be repeated                   |
        | start         | datetime                          |
        | end           | datetime                          |
        | stations      | could be multiple, separated by - |

        """

        if annotation_params["annotation_id"].nunique() != len(annotation_params):
            raise ValueError("Annotation IDs must be unique.")
        if not all(isinstance(x, str) for x in annotation_params["annotation_id"]):
            raise ValueError("Annotation IDs must be strings.")
        if not all(isinstance(x, str) for x in annotation_params["activity_name"]):
            raise ValueError("Activity names must be strings.")
        if not all(isinstance(x, pd.Timestamp) for x in annotation_params["start"]):
            raise ValueError("Start must be a datetime.")
        if not all(isinstance(x, pd.Timestamp) for x in annotation_params["end"]):
            raise ValueError("End must be a datetime.")
        if not all(isinstance(x, str) for x in annotation_params["stations"]):
            raise ValueError("Stations must be strings.")

    def get_annotation_params(self) -> List[AnnotationParams]:
        """ Fetch the annotation parameters. """
        ign_sens_path = self._local_in_dir + "/anno_params.csv"
        if ign_sens_path is None or not os.path.exists(ign_sens_path):
            raise ValueError("Annotation parameters file not found.")
        # open file
        annotation_params = pd.read_csv(ign_sens_path)
        annotation_params['start'] = pd.to_datetime(annotation_params['start'])
        annotation_params['end'] = pd.to_datetime(annotation_params['end'])
        if not all(isinstance(x, str) for x in annotation_params["annotation_id"]):
            annotation_params["annotation_id"] = annotation_params["annotation_id"].astype(str)
        self._validate_annotation_params(annotation_params)
        return [AnnotationParams(
            annotation_id=row["annotation_id"],
            activity_name=row["activity_name"],
            start=row["start"],
            end=row["end"],
            stations=row["stations"].split("-")
        ) for _, row in annotation_params.iterrows()]
