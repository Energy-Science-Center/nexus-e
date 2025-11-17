import pytest
import src.nexus_e.config as config
import src.nexus_e.simulation as simulation


def get_default_test_config() -> config.Config:
    return config.Config()

class TestSimulationModule:
        
    def test_raise_exception_when_unknown_module_is_demanded(self):
        # Arrange
        module_config = config.Module(name=None, parameters={})
        test_config = get_default_test_config()
        sut = simulation.CorePluginFactory(test_config)

        # Assert
        with pytest.raises(simulation.UnknownModule, match="None"):
            # Act
            sut.get_module(module_config)