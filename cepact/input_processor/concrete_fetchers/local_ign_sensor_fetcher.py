""" The local fetcher, that fetches the sensor ignore annotation. """
import json
import os
from typing import Dict, List

import jsonschema

from ..abstract_fetchers import IgnoreSensorFetcher


class LocalIgnoreSensorFetcher(IgnoreSensorFetcher):
    """ Fetches the ignore sensor annotation.

    Imports the ignore sensor annotation from a local file. The format of the file is
    given by the following JSON Schema:
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "required": [
        "general"
      ],
      "properties": {
        "general": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "patternProperties": {
        "^.*$": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "additionalProperties": false
    }

    This schema enforces that the JSON file has a "general" key with a list of strings
    as values, and any other key should have a list of strings as values.
    This means that all the sensors that are to be ignored for all activities should be in the
    "general" key, and the sensors that are to be ignored for a specific activity should be in a
    key with the name of the activity.
        """

    def __init__(self, local_in_dir: str) -> None:
        self._local_in_dir = local_in_dir

    def get_ignore_sensors(self) -> Dict[str, List[str]]:
        """ Fetch the ignore sensors.

        Structure of return dict:
        {
            "general": List[str],
            "activityName1": List[str],
            "activityName2": List[str]
        }
        """
        ign_sens_path = self._local_in_dir + "/ignore_sensors.json"
        if ign_sens_path is None or not os.path.exists(ign_sens_path):
            raise ValueError("Ignore sensor file not found.")
        # open file
        ignore_sensors: Dict[str, List[str]]
        with open(ign_sens_path, "r", encoding="utf-8") as f:
            ignore_sensors = json.load(f)
        # validate schema
        with open(os.path.join(os.path.dirname(__file__), "static", "ignore_sensors_schema.json"),
                  "r",
                  encoding="utf-8") as f:
            schema = json.load(f)
        jsonschema.validate(ignore_sensors, schema)
        return ignore_sensors
