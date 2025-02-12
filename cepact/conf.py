""" Configuration classes for detection app generation. """
from dataclasses import dataclass
from typing import List, Dict, TYPE_CHECKING
from .input_processor.abstract_fetchers import (IgnoreSensorFetcher, SignatureFetcher,
                                                AnnotationParamFetcher, DiscretizationFetcher)

if TYPE_CHECKING:
    from .output_producer import InstanceLevelDetQuery

@dataclass(frozen=True, kw_only=True)
class SiddhiConfig():
    """ Configurations for Siddhi app generation.

    Attributes:
    - mqtt_url: URL of the MQTT broker.
    - mqtt_user: Username for the MQTT broker.
    - mqtt_pwd: Password for the MQTT broker.
    - topic_prefix: Prefix for the MQTT topics.
    - map_sensor_name_data_to_mqtt: Mapping of sensor names to MQTT topics.
    """
    mqtt_url: str
    mqtt_user: str
    mqtt_pwd: str
    topic_prefix: str
    map_sensor_name_data_to_mqtt: Dict[str, str]


# pylint: disable=too-many-instance-attributes
@dataclass(frozen=True, kw_only=True)
class DAGConfig():
    """ Configurations for creation of detection apps.

    Attributes:
    - det_methods: List of detection methods to use.
    - siddhi_config: Configuration for Siddhi apps.
    - out_dir: Output directory for the generated apps and files.
    - sampling_freq: Sampling frequency of the IoT data (important for cross-station activities).
    - signature_fetcher: Fetcher for the activity signatures.
    - annotation_param_fetcher: Fetcher for the activity annotations.
    - ignore_sensor_fetcher: Fetcher for the ignore sensors.
    - discretization_fetcher: Fetcher for the discretization of the IoT data.
    """
    det_methods: List['InstanceLevelDetQuery']
    siddhi_config: SiddhiConfig
    out_dir: str
    sampling_freq: float
    signature_fetcher: SignatureFetcher
    annotation_param_fetcher: AnnotationParamFetcher
    ignore_sensor_fetcher: IgnoreSensorFetcher
    discretization_fetcher: DiscretizationFetcher

    # validate the configuration
    def __post_init__(self) -> None:
        """ Validate the configuration. """
        if len(self.det_methods) == 0:
            raise ValueError("No detection method selected.")
