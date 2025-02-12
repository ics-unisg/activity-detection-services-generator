""" Abstract instance level query for strategy pattern. """
from abc import ABC, abstractmethod


class InstanceLevelDetQuery(ABC):
    """ Abstract instance level query for strategy pattern. """

    @abstractmethod
    def generate(self, num_changes: int, act_name: str) -> str:
        """ Generate the query. """
