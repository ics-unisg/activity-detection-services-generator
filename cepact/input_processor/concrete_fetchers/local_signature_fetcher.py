""" Fetches the signature from local files. """
import json
import os
from typing import Dict, List

import jsonschema
import pandas as pd

from ..abstract_fetchers import SignatureFetcher
from ...representations import AnnotationParams, Signature, SignatureBuilder, SignatureItem


class LocalSignatureFetcher(SignatureFetcher):
    """ Fetches the signature from local files.

    Imports the signature from a local file. The file should be in the format of a JSONL file with
    the following JSON schema for each line:
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "required": [
        "station",
        "timestamp"
      ],
      "properties": {
        "station": {
          "type": "string"
        },
        "timestamp": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
    """

    def __init__(self, local_in_dir: str) -> None:
        self._local_in_dir = local_in_dir
        with open(
                os.path.join(os.path.dirname(__file__), "static", "timeseries_schema.json"),
                "r",
                encoding="utf-8") as schema_file:
            self._schema = json.load(schema_file)

    def get_signature(self,
                      annotation_params: AnnotationParams,
                      ignore_sensors: Dict[str, List[str]],
                      sampling_freq: float) -> Signature:
        """ Fetch the signature for a certain activity annotation from local file. """
        overall_ignore = ignore_sensors["general"].copy()
        signature_builder: SignatureBuilder = SignatureBuilder(
            activity_name=annotation_params.activity_name,
            annotation_id=annotation_params.annotation_id,
            sampling_freq=sampling_freq
        )
        if annotation_params.activity_name in ignore_sensors:
            overall_ignore += ignore_sensors[annotation_params.activity_name]
        ts_path = self._local_in_dir + "/timeseries.jsonl"
        if ts_path is None or not os.path.exists(ts_path):
            raise ValueError("Timeseries file not found.")
        with open(ts_path, "r", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                # validate schema
                jsonschema.validate(record, self._schema)
                station = record['station']
                timestamp = pd.to_datetime(record['timestamp']).to_pydatetime()
                if (timestamp < annotation_params.start) or (timestamp > annotation_params.end):
                    continue
                if station not in annotation_params.stations:
                    continue
                # iterate through all other fields
                for field, value in record.items():
                    if field not in overall_ignore and field not in ["station", "timestamp", "id"]:
                        signature_builder.add_signature_item(
                            SignatureItem(
                                station=station,
                                timestamp=timestamp,
                                sensor=field,
                                value=value
                            )
                        )
        return signature_builder.build()
