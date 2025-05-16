""" Test discretization feature """
import pytest
from cepact.representations import DiscretizationBuilder


@pytest.fixture(scope='module', autouse=True)
def before_all():
    """ Preparations
    """
    # ^ Will be executed before the first test
    yield
    # v Will be executed after the last test


def test_basic():
    """ Test adding general discretization item. """
    discr_builder = DiscretizationBuilder()
    discr_builder.add_discretization_item(sensor="sensor1", beg=0, to=float("inf"),
                                          beg_incl=True, to_incl=True, target_value=1)
    with pytest.raises(ValueError):
        discr_builder.build()
    discr_builder.add_discretization_item(sensor="sensor1", beg=float("-inf"), to=0,
                                          beg_incl=True, to_incl=False, target_value=2)
    discr1 = discr_builder.build()
    assert discr1.discretize("sensor1", 0) == 1
    assert discr1.discretize("sensor1", 1) == 1
    assert discr1.discretize("sensor1", -1) == 2
    discr_builder.add_discretization_item(sensor="sensor2", beg=0, to=float("inf"),
                                          beg_incl=True, to_incl=True, target_value="goat")
    discr_builder.add_discretization_item(sensor="sensor2", beg=float("-inf"), to=0,
                                          beg_incl=True, to_incl=False, target_value="sheep")
    discr2 = discr_builder.build()
    assert discr2.discretize("sensor2", 0) == "goat"
    assert discr2.discretize("sensor2", 1) == "goat"
    assert discr2.discretize("sensor2", -1) == "sheep"
    assert discr2.discretized_type("sensor1") == int
    assert discr2.discretized_type("sensor2") == str
    discr_builder.add_discretization_item(sensor="sensor3", beg=float("-inf"), to=0,
                                          beg_incl=True, to_incl=True, target_value="goat")
    discr_builder.add_discretization_item(sensor="sensor3", beg=0, to=5,
                                          beg_incl=False, to_incl=True, target_value="sheep")
    discr_builder.add_discretization_item(sensor="sensor3", beg=5, to=float("inf"),
                                          beg_incl=False, to_incl=True, target_value="cow")
    discr3 = discr_builder.build()
    assert discr3.discretize("sensor3", 0) == "goat"
    assert discr3.discretize("sensor3", 5) == "sheep"
    assert discr3.discretize("sensor3", 6) == "cow"
    with pytest.raises(ValueError):
        discr_builder.add_discretization_item(sensor="sensor3", beg=0.5, to=1,
                                              beg_incl=False, to_incl=True, target_value="newAnimal")
    with pytest.raises(AttributeError):
        discr3.add_discretization_item(sensor="sensor4", beg=0, to=float("inf"),
                                       beg_incl=True, to_incl=True, target_value="newAnimal2")
