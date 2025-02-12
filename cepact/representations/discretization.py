""" Class representing the mapping from continuous to discrete values. """
from typing import Union, Dict, Tuple, Any


def _discretize(sensor: str, value: Any, discretization_map: Dict[str, \
        Dict[Tuple[Tuple[float, float], Tuple[str, str]], Any]]) -> Any:
    """ Return discretized value if in some range. If not, return None."""
    if sensor in discretization_map:
        for interval, discretization in discretization_map[sensor].items():
            if interval[1][0] == "incl" and interval[1][1] == "incl":
                if interval[0][0] <= value <= interval[0][1]:
                    return discretization
            elif interval[1][0] == "incl" and interval[1][1] == "excl":
                if interval[0][0] <= value < interval[0][1]:
                    return discretization
            elif interval[1][0] == "excl" and interval[1][1] == "incl":
                if interval[0][0] < value <= interval[0][1]:
                    return discretization
            elif interval[1][0] == "excl" and interval[1][1] == "excl":
                if interval[0][0] < value < interval[0][1]:
                    return discretization
    return None


class Discretization():
    """ Class representing the mapping from continuous to discrete values. """

    def __init__(self, discretization_map: Dict[str, \
            Dict[Tuple[Tuple[float, float], Tuple[str, str]], Any]]) -> None:
        self._discretization_map: Dict[str, \
            Dict[Tuple[Tuple[float, float], Tuple[str, str]], Any]] = discretization_map

    def discretize(self, sensor: str, value: Any) -> Any:
        """ Return discretized value if in some range. If not, return None."""
        return _discretize(sensor, value, self._discretization_map)

    def discretized_type(self, sensor: str) -> type:
        """ Return the type of the discretized values for a sensor. """
        if sensor in self._discretization_map:
            sensor_map = self._discretization_map[sensor]
            if all(isinstance(key, str) for key in sensor_map.values()):
                return str
            if all(isinstance(key, int) for key in sensor_map.values()):
                return int
            if all(isinstance(key, float) for key in sensor_map.values()):
                return float
            if all(isinstance(key, bool) for key in sensor_map.values()):
                return bool
            raise ValueError("Discretization map values must be of the same type")
        raise ValueError(f"Sensor {sensor} not in sensor discretization map")

    def is_discretized(self, sensor: str) -> bool:
        """ Check if the sensor is discretized. """
        return sensor in self._discretization_map

    def get_mapping_for_sensor(self, sensor: str) -> Dict[
        Tuple[Tuple[float, float], Tuple[str, str]], Any]:
        """ Get the mapping for a sensor. """
        return self._discretization_map[sensor]


class DiscretizationBuilder():
    """ Class for building immutable discretization object. """

    def __init__(self) -> None:
        self._discretization_map: Dict[str, \
            Dict[Tuple[Tuple[float, float], Tuple[str, str]], Any]] = {}

    # pylint: disable=too-many-arguments
    def _check_overlapping_mapping_range(self,
                                         sensor: str,
                                         l_bound: float,
                                         u_bound: float,
                                         l_bound_type: str,
                                         u_bound_type: str) -> None:
        """ Check if the mapping range overlaps with an existing one.

         In which case, raise an error (since not permissible). """
        if _discretize(sensor, l_bound, self._discretization_map) is not None:
            if l_bound_type == "incl":
                raise ValueError(f"Sensor {sensor} already has a mapping for {l_bound}")
            for interval, _ in self._discretization_map[sensor].items():
                if interval[0][1] == l_bound:
                    return
            raise ValueError(f"Sensor {sensor} already has a mapping for {l_bound}")
        if _discretize(sensor, u_bound, self._discretization_map) is not None:
            if u_bound_type == "incl":
                raise ValueError(f"Sensor {sensor} already has a mapping for {u_bound}")
            for interval, _ in self._discretization_map[sensor].items():
                if interval[0][0] == u_bound:
                    return
            raise ValueError(f"Sensor {sensor} already has a mapping for {u_bound}")

    # pylint: disable=too-many-arguments
    def add_discretization_item(self, sensor: str,
                                beg: float, to: float,
                                beg_incl: bool, to_incl: bool,
                                target_value: Union[str, int, float, bool]) -> None:
        """ Add an item to the discretization mapping. """
        self._check_overlapping_mapping_range(sensor, beg, to, "incl" if beg_incl else "excl",
                                              "incl" if to_incl else "excl")
        if sensor not in self._discretization_map:
            self._discretization_map[sensor] = {}
        beg_incl_str = "incl" if beg_incl else "excl"
        to_incl_str = "incl" if to_incl else "excl"
        self._discretization_map[sensor][(beg, to), (beg_incl_str, to_incl_str)] = target_value

    def _check_overall_discretization_validity(self) -> bool:
        """ Check if the discretization is valid. """
        for sensor, curr_discr in self._discretization_map.items():
            if float("-inf") not in [key[0][0] for key in curr_discr.keys()]:
                raise ValueError(f"Sensor {sensor} must have a mapping for the whole range.")
            if float("inf") not in [key[0][1] for key in curr_discr.keys()]:
                raise ValueError(f"Sensor {sensor} must have a mapping for the whole range.")
            # sort by the first element of the tuple
            ordered_mapping = dict(
                sorted(curr_discr.items(), key=lambda item: item[0]))
            # make sure that the previous upper bound is equal to the next lower bound
            previous_upper_bound = float("-inf")
            for key in ordered_mapping.keys():
                if key[0][0] != previous_upper_bound:
                    raise ValueError(f"Undefined source range in sensor {sensor} mapping.")
                previous_upper_bound = key[0][1]
        return True

    def build(self) -> Discretization:
        """ Mark the discretization as complete (and then immutable), checking for validity. """
        self._check_overall_discretization_validity()
        return Discretization(self._discretization_map.copy())
