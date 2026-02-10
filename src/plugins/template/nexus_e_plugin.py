"""
Docstring for plugins.template.nexus_e_plugin
Replace or delete this docstring.
"""

from dataclasses import asdict, dataclass
from typing import Any

from nexus_e_interface.plugin import Plugin
from nexus_e_interface.scenario import Scenario


@dataclass
class Parameters:
    """
    Add as many parameters as needed.
    Don't keep the replace_me parameter.
    Update this docstring.
    Try to add docstrings to every parameter.
    """
    replace_me: str = "replace me"
    """replace me"""
    
class NexusePlugin(Plugin):
    """
    NexusePlugin template.
    Replace this docstring with plugin description.
    """

    @classmethod
    def get_default_parameters(cls) -> dict:
        return asdict(Parameters())
    
    def __init__(self, parameters: dict, scenario: Scenario | None = None):
        self.__parameters = Parameters(**parameters)
        self.__scenario = scenario

    def run(self) -> dict[str, Any]:
        """
        Implement this function to make a usable plugin.
        Update this docstring accordingly.
        """
        raise NotImplementedError