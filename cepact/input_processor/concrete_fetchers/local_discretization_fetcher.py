""" The local fetcher, that fetches the discretization annotation. """
import os
import re
from typing import Any, Union

import pandas as pd

from ..abstract_fetchers import DiscretizationFetcher
from ...representations import Discretization, DiscretizationBuilder


class LocalDiscretizationFetcher(DiscretizationFetcher):
    """ Fetches the discretization annotation.

    Imports the discretization annotation from a local file. The file should be in the format of a
    CSV file with the following columns:
    - sensor: name of the sensor
    - source: what range of the sensor values should be mapped to the target
    - target: target of the sensor

    Note that whenever a sensor is included, the source should cover the whole range of the
    sensor values.
    Possible source values are:
    - <x: values less than x
    - >x: values greater than x
    - <=x: values less than or equal to x
    - >=x: values greater than or equal to x
    - [x,y]: values between x and y
    - [x,y[: values between x and y, excluding y
    - ]x,y]: values between x and y, excluding x
    - ]x,y[: values between x and y, excluding x and y
    """

    def __init__(self, local_in_dir: str) -> None:
        self._local_in_dir = local_in_dir

    def _get_sorted_sensor_mapping(self) -> Union[pd.DataFrame, None]:
        """ Get the sorted sensor mapping. """
        sensor_mapping_path = self._local_in_dir + "/discr_mapping.csv"
        if sensor_mapping_path is None or not os.path.exists(sensor_mapping_path):
            raise ValueError("Sensor mapping file not found.")
        # check if file is empty, if so return None
        if os.stat(sensor_mapping_path).st_size == 0:
            return None
        # open file
        sensor_mapping = pd.read_csv(sensor_mapping_path)
        # check whether file has columns sensor, source, target
        if not all(col in sensor_mapping.columns for col in ["sensor", "source", "target"]):
            raise ValueError("Sensor mapping file must have columns 'sensor', 'source', 'target'")
        # check whether all sensors have all areas covered
        # check whether for each sensor there is a source entry starting with < and >
        for sensor in sensor_mapping["sensor"].unique():
            sensor_map = sensor_mapping[sensor_mapping["sensor"] == sensor]
            if not all(area in "".join(list(sensor_map["source"].unique())) for area in ["<", ">"]):
                raise ValueError(f"Sensor {sensor} must have areas '<' and '>' covered")
        return sensor_mapping.sort_values(by="sensor")

    def _get_casted_target(self, target: Any) -> Any:
        """ Cast the target to the most suitable type. """
        try:
            return int(target)
        except ValueError:
            try:
                return float(target)
            except ValueError:
                if target == "True":
                    return True
                if target == "False":
                    return False
                return target

    def get_discretization(self) -> Discretization:
        """ Fetch the discretization. """
        # import sensor discretization mapping from file, file of csv format
        # some things that need to be done:
        # - validate that all sensors mentioned have all areas covered
        # - convert from semantics to valid python format as seen above
        # validate that there are no overlapping ranges
        discretization_builder = DiscretizationBuilder()
        # sort by sensor
        sensor_mapping = self._get_sorted_sensor_mapping()
        if sensor_mapping is None:
            return discretization_builder.build()
        # go through the sorted dataframe row by row
        for _, row in sensor_mapping.iterrows():
            sensor = row["sensor"]
            source = row["source"]
            target = row["target"]
            target_casted = self._get_casted_target(target)
            # check wether source matches some regex
            if not re.fullmatch(r"^([<>]=?\s*-?\d+(\.\d+)?|"
                                r"[\[\]]\s*-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?\s*[\[\]]|)$", source):
                raise ValueError(f"Source {source} for sensor {sensor} is not valid.")
            # remove all whitespaces from source
            source = source.replace(" ", "")
            if source[:2] == "<=":
                l_bound = float("-inf")
                u_bound = float(source[2:])
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=l_bound,
                                                               to=u_bound,
                                                               beg_incl=True,
                                                               to_incl=True,
                                                               target_value=target_casted)
            elif source[:2] == ">=":
                l_bound = float(source[2:])
                u_bound = float("inf")
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=l_bound,
                                                               to=u_bound,
                                                               beg_incl=True,
                                                               to_incl=True,
                                                               target_value=target_casted)
            elif source[0] == "<":
                l_bound = float("-inf")
                u_bound = float(source[1:])
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=l_bound,
                                                               to=u_bound,
                                                               beg_incl=True,
                                                               to_incl=False,
                                                               target_value=target_casted)
            elif source[0] == ">":
                l_bound = float(source[1:])
                u_bound = float("inf")
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=l_bound,
                                                               to=u_bound,
                                                               beg_incl=False,
                                                               to_incl=True,
                                                               target_value=target_casted)
            elif source[0] == "[" and source[-1] == "]":
                bounds = source[1:-1].split(",")
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=float(bounds[0]),
                                                               to=float(bounds[1]),
                                                               beg_incl=True,
                                                               to_incl=True,
                                                               target_value=target_casted)
            elif source[0] == "[" and source[-1] == "[":
                bounds = source[1:-1].split(",")
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=float(bounds[0]),
                                                               to=float(bounds[1]),
                                                               beg_incl=True,
                                                               to_incl=False,
                                                               target_value=target_casted)
            elif source[0] == "]" and source[-1] == "]":
                bounds = source[1:-1].split(",")
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=float(bounds[0]),
                                                               to=float(bounds[1]),
                                                               beg_incl=False,
                                                               to_incl=True,
                                                               target_value=target_casted)
            elif source[0] == "]" and source[-1] == "[":
                bounds = source[1:-1].split(",")
                discretization_builder.add_discretization_item(sensor=sensor,
                                                               beg=float(bounds[0]),
                                                               to=float(bounds[1]),
                                                               beg_incl=False,
                                                               to_incl=False,
                                                               target_value=target_casted)
            else:
                raise ValueError(f"Source {source} for sensor {sensor} is not valid.")
        # make sure that the whole range is covered
        return discretization_builder.build()
