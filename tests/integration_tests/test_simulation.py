import pytest
import src.nexus_e.config as config
import src.plugins.centiv.cgep.create_scenario_fast as centiv
import src.nexus_e.simulation as simulation


def get_default_test_config() -> config.Config:
    return config.Config()

class TestSimulationModule:
        
    def test_raise_exception_when_unknown_module_is_demanded(self):
        # Arrange
        module_config = config.Module(name=None, parameters={})
        test_config = get_default_test_config()
        sut = simulation.CoreModuleFactory(test_config)

        # Assert
        with pytest.raises(simulation.UnknownModule, match="None"):
            # Act
            sut.get_module(module_config)


class MockedModule(simulation.Module):
    def __init__(self):
        self.run_count = 0

    def run(self):
        self.run_count += 1


class MockedModuleFactory(simulation.ModuleFactory):
    def __init__(self, settings: config.Config = None):
        self.modules = {}

    def get_module(
        self,
        module_config: config.Module,
    ) -> simulation.Module:
        new_module = MockedModule()
        self.modules[module_config.name] = new_module
        return new_module
