from abc import ABC, abstractmethod
from .scenario import Scenario

class Plugin(ABC):
    """
    Implement this class to create a Nexus-e plugin.
    Name the class NexusePlugin.
    """

    @abstractmethod
    def __init__(self, scenario: Scenario, config: dict): ...

    @abstractmethod
    def run(self)-> None: ...

    @abstractmethod
    def get_default_config(self) -> dict: ...