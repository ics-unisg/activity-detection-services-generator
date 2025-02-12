""" Test grafana fetcher """
import datetime
from datetime import datetime as dt
from typing import List
from unittest import mock
from unittest.mock import Mock

import pytest
from cepact.input_processor.concrete_fetchers import GrafanaFetcher
from cepact.representations import AnnotationParams

response_mock = Mock()
response_mock.json.return_value = [
    {'time': 1679309568446, 'text': 'END_Move-to-DPS', 'tags': ['vgr5', 'activity', 'VGR_1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309542198, 'text': 'START_Move-to-DPS', 'tags': ['vgr5', 'activity', 'VGR_1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309528344, 'text': 'END_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1'], 'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309498989, 'text': 'START_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1'], 'dashboardUID': 'mock', 'panelId': 'mock'}]

response_ignoregen_mock = Mock()
response_ignoregen_mock.json.return_value = [
    {'time': 1679309568446, 'text': 'END_Move-to-DPS',
     'tags': ['vgr5', 'activity', 'VGR_1', 'ignoregen'
                                           '-new_sensor_to_ignore1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309542198, 'text': 'START_Move-to-DPS', 'tags': ['vgr5', 'activity', 'VGR_1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309528344, 'text': 'END_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1']
        , 'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309498989, 'text': 'START_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1', 'ignoregen-new_sensor_to_ignore2',
              'ignoregen-new_sensor_to_ignore3'], 'dashboardUID': 'mock', 'panelId': 'mock'}]

response_ignorespec_mock = Mock()
response_ignorespec_mock.json.return_value = [
    {'time': 1679309568446, 'text': 'END_Move-to-DPS',
     'tags': ['vgr5', 'activity', 'VGR_1', 'ignore'
                                           '-new_sensor_to_ignore1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309542198, 'text': 'START_Move-to-DPS', 'tags': ['vgr5', 'activity', 'VGR_1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309528344, 'text': 'END_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1'], 'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309498989, 'text': 'START_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1', 'ignore-new_sensor_to_ignore2',
              'ignore-new_sensor_to_ignore3'], 'dashboardUID': 'mock', 'panelId': 'mock'}]

response_ignoreboth_mock = Mock()
response_ignoreboth_mock.json.return_value = [
    {'time': 1679309568446, 'text': 'END_Move-to-DPS',
     'tags': ['vgr5', 'activity', 'VGR_1', 'ignore'
                                           '-new_sensor_to_ignore1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309542198, 'text': 'START_Move-to-DPS', 'tags': ['vgr5', 'activity', 'VGR_1'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309528344, 'text': 'END_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1', 'ignoregen-new_sensor_to_ignore4'],
     'dashboardUID': 'mock', 'panelId': 'mock'},
    {'time': 1679309498989, 'text': 'START_Get-Workpiece-from-Pickup-Station',
     'tags': ['vgr4', 'activity', 'VGR_1', 'ignore-new_sensor_to_ignore2',
              'ignore-new_sensor_to_ignore3'], 'dashboardUID': 'mock', 'panelId': 'mock'}]


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_without_ignore_tags():
    """ Grafana has no ignore/gen- tags """
    with mock.patch('requests.get', return_value=response_mock):
        grafana_fetcher = GrafanaFetcher(url="http://localhost:3000",
                                         auth="mock")
        activity_params: List[AnnotationParams] = grafana_fetcher.get_annotation_params()
        assert grafana_fetcher.get_ignore_sensors() == {"general": []}
        assert "vgr5" in [act_param.annotation_id for act_param in activity_params]
        assert "vgr4" in [act_param.annotation_id for act_param in activity_params]
        assert len(activity_params) == 2
        vgr4_4_act_param = \
            [act_param for act_param in activity_params if act_param.annotation_id == "vgr4"][0]
        vgr5_5_act_param = \
            [act_param for act_param in activity_params if act_param.annotation_id == "vgr5"][0]
        assert vgr4_4_act_param.activity_name == "Get Workpiece from Pickup Station"
        assert vgr5_5_act_param.activity_name == "Move to DPS"
        assert abs(vgr4_4_act_param.start - dt.fromtimestamp(1679309498989 / 1000,
                                                             datetime.UTC)) <= datetime.timedelta(
            seconds=1)
        assert abs(vgr4_4_act_param.end - dt.fromtimestamp(1679309528344 / 1000,
                                                           datetime.UTC)) <= datetime.timedelta(
            seconds=1)
        assert abs(vgr5_5_act_param.start - dt.fromtimestamp(1679309542198 / 1000,
                                                             datetime.UTC)) <= datetime.timedelta(
            seconds=1)
        assert abs(vgr5_5_act_param.end - dt.fromtimestamp(1679309568446 / 1000,
                                                           datetime.UTC)) <= datetime.timedelta(
            seconds=1)


def test_with_general_ignore_tags():
    """ Grafana has ignoregen- tags"""
    with mock.patch('requests.get', return_value=response_ignoregen_mock):
        grafana_fetcher = GrafanaFetcher(url="http://localhost:3000",
                                         auth="mock")
        activity_params: List[AnnotationParams] = grafana_fetcher.get_annotation_params()
        ignore_sensors: dict = grafana_fetcher.get_ignore_sensors()
        assert "vgr5" in [act_param.annotation_id for act_param in activity_params]
        assert "vgr4" in [act_param.annotation_id for act_param in activity_params]
        assert len(activity_params) == 2
        assert 3 == len(ignore_sensors["general"])
        assert "new_sensor_to_ignore1" in ignore_sensors["general"]
        assert "new_sensor_to_ignore2" in ignore_sensors["general"]
        assert "new_sensor_to_ignore3" in ignore_sensors["general"]


def test_with_specific_ignore_tags():
    """ Grafana has ignore- tags"""
    with mock.patch('requests.get', return_value=response_ignorespec_mock):
        grafana_fetcher = GrafanaFetcher(url="http://localhost:3000",
                                         auth="mock")
        activity_params: List[AnnotationParams] = grafana_fetcher.get_annotation_params()
        ignore_sensors: dict = grafana_fetcher.get_ignore_sensors()
        assert "vgr5" in [act_param.annotation_id for act_param in activity_params]
        assert "vgr4" in [act_param.annotation_id for act_param in activity_params]
        assert len(activity_params) == 2
        assert len(ignore_sensors["general"]) == 0
        assert ignore_sensors["vgr4"] == ["new_sensor_to_ignore2", "new_sensor_to_ignore3"]
        assert ignore_sensors["vgr5"] == ["new_sensor_to_ignore1"]


def test_with_both_ignore_tags():
    """ Grafana has both ignore- and ignoregen- tags """
    with mock.patch('requests.get', return_value=response_ignoreboth_mock):
        grafana_fetcher = GrafanaFetcher(url="http://localhost:3000",
                                         auth="mock")
        activity_params: List[AnnotationParams] = grafana_fetcher.get_annotation_params()
        ignore_sensors: dict = grafana_fetcher.get_ignore_sensors()
        assert "vgr5" in [act_param.annotation_id for act_param in activity_params]
        assert "vgr4" in [act_param.annotation_id for act_param in activity_params]
        assert len(activity_params) == 2
        assert "new_sensor_to_ignore4" in ignore_sensors["general"]
        assert ignore_sensors["vgr4"] == ["new_sensor_to_ignore2", "new_sensor_to_ignore3"]
        assert ignore_sensors["vgr5"] == ["new_sensor_to_ignore1"]
