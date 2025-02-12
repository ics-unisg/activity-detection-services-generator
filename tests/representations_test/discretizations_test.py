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
    discr_builder.add_discretization_item("sensor1", 0, float("inf"), True, True, 1)
    with pytest.raises(ValueError):
        discr_builder.build()
    discr_builder.add_discretization_item("sensor1", float("-inf"), 0, True, False, 2)
    discr1 = discr_builder.build()
    assert discr1.discretize("sensor1", 0) == 1
    assert discr1.discretize("sensor1", 1) == 1
    assert discr1.discretize("sensor1", -1) == 2
    discr_builder.add_discretization_item("sensor2", 0, float("inf"), True, True, "goat")
    discr_builder.add_discretization_item("sensor2", float("-inf"), 0, True, False, "sheep")
    discr2 = discr_builder.build()
    assert discr2.discretize("sensor2", 0) == "goat"
    assert discr2.discretize("sensor2", 1) == "goat"
    assert discr2.discretize("sensor2", -1) == "sheep"
    assert discr2.discretized_type("sensor1") == int
    assert discr2.discretized_type("sensor2") == str
    discr_builder.add_discretization_item("sensor3", float("-inf"), 0, True, True, "goat")
    discr_builder.add_discretization_item("sensor3", 0, 5, False, True, "sheep")
    discr_builder.add_discretization_item("sensor3", 5, float("inf"), False, True, "cow")
    discr3 = discr_builder.build()
    assert discr3.discretize("sensor3", 0) == "goat"
    assert discr3.discretize("sensor3", 5) == "sheep"
    assert discr3.discretize("sensor3", 6) == "cow"
    with pytest.raises(ValueError):
        discr_builder.add_discretization_item("sensor3", 0.5, 1, False, True, "newAnimal")
    with pytest.raises(AttributeError):
        discr3.add_discretization_item("sensor4", 0, float("inf"), True, True, "newAnimal2")
