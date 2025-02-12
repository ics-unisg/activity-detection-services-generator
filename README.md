[![FAIR checklist badge](https://fairsoftwarechecklist.net/badge.svg)](https://fairsoftwarechecklist.net/v0.2?f=31&a=31112&i=32321&r=133)

# CEPAct

Generate complex event processing (Siddhi) activity detection apps based on IoT data and expert annotations. The general input/output and flow of the application can be seen in the following UML sequence diagram:

![UML sequence diagram](./misc/seq_uml_simpl.png)

The application is generally structured as seen in the UML class diagram below.

![UML class diagram](./misc/uml_class.png)

## Usage

An exemplary usage of the application can be seen in the following code snippet:

```python
from cepact import (SiddhiConfig, DAGConfig, DetServiceGenerator, LocalIgnoreSensorFetcher, InfluxFetcher, GrafanaFetcher, LocalDiscretizationFetcher, AllHighLevelPatternQuery, First50HighLevelPatternQuery)

siddhi_conf = SiddhiConfig(mqtt_url = "url", mqtt_user = "usr", mqtt_pwd = "pwd", topic_prefix = "SOMEPREFIX", map_sensor_name_data_to_mqtt = {})

dag_conf = DAGConfig(
    det_methods = [AllHighLevelPatternQuery(), First50HighLevelPatternQuery()],
    out_dir = "out",
    sampling_freq = 1,
    siddhi_config = siddhi_conf,
    signature_fetcher = InfluxFetcher(url = "url", auth = "authkey", org = "org", influx_station_bucket_map = {}),
    annotation_param_fetcher = GrafanaFetcher(url = "url", auth = "authkey"),
    ignore_sensor_fetcher = LocalIgnoreSensorFetcher("in"),
    discretization_fetcher = LocalDiscretizationFetcher("in"))

generator = DetServiceGenerator(dag_conf)
generator.run()
```
As can be seen in the usage example, the features include:
- Annotations and IoT data can be provided from **various input sources**; users can implement and provide **custom fetchers** for required inputs or use **included fetchers**.
- Generated detection services apps and supporting files are written to a user-specified **output directory** in the local file system.
- Users can customize the detection service generation using a **configuration** object.
- Selection of included **detection methods** are supported, and custom implementations can be added by implementing provided interfaces.
- The package generates output according to the provided **Siddhi configuration**, ensuring compatibility with downstream systems.

There are potentially multiple concrete instantiations of the abstract data fetchers, meaning that the input of IoT data and annotations can be done in different ways. For the local input fetchers, please refer to the exemplary files in `tests/local_input_mocks/.../in`, as well as the documentation accompanying the fetcher implementations.

For the input of annotations from Grafana and the data coming from InfluxDB, we provide some guidelines for the annotations below.

## Contributing
Contributions are welcome. Both PRs from forks and issues in this repository are appreciated.
Possible (non-exhaustive list) contributions include:
- Adding new data fetchers for different data sources
- Adding new detection methods
- Improving the existing codebase
- Adding new tests
- Improving the documentation
- Adding new features (e.g. support for more complex patterns, multiple signatures per activity, etc.)
- Adding new output writers besides Siddhi (for other CEP platforms)

For more information on contributing, please refer to the [CONTRIBUTING.md](./CONTRIBUTING.md) file.

## Grafana Annotation Guidelines
Generates CEP apps and signatures based on Grafana annotations.
Annotation rules:
- Name of _start_ annotation `START_Activity-Name-Here`
  - Note, that the activity name must match the activity name in the Camunda log for evaluation
- Analogous for `END_`
- First tag must be some unique key
- Second tag must be `activity`
- Third tag must be _station_ code, e.g. `OV_1`, `HYGIENE_STATION`
  - For detection apps spanning multiple components/stations/resources, this tag needs to include all of them separated by a hyphen, e.g. `HYGIENE_STATION-LEFT_DONATION`
- Fourth tag and all others behind can be used to _ignore_ certain _sensors_ in the activity signature and for the creation
of the CEP Siddhi apps (can be either at START or END annotation):
  - add `ignore-SENSORNAME` to the tag to ignore the sensor for only this activity-signature/-app, e.g. `ignore-light_l5`
  - if you want to ignore a certain sensor in general, i.e. for all activities, and don't want to repeat the tag, just 
  tag it with `ignoregen-SENSORENAME`, e.g. `ignoregen-movepos_x`

## InfluxDB Guidelines
The IoT time series data to extract the activity signature from can be stored on InfluxDB. The data should be stored in a bucket with and the naming 
should be specified in the `influx_station_bucket_map` in the `DAGConfig`.
