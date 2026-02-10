from abc import ABC, abstractmethod
from typing import Any
from .scenario import Scenario

class Plugin(ABC):
    """
    Implement this class to create a Nexus-e plugin.
    Name the class NexusePlugin.
    """

    @abstractmethod
    def __init__(self, scenario: Scenario | None, parameters: dict): ...

    @abstractmethod
    def run(self) -> dict[str, Any]: ...

    @classmethod
    @abstractmethod
    def get_default_parameters(cls) -> dict: ...