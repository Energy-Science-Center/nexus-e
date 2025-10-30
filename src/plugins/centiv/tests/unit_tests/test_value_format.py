import pytest
from value_format import ValueFormatter


class TestValueFormat:
    truncate_value_data = [
        (0.12345, 3, 0.123),
        (0.1234567, 6, 0.123456),
        (0.12, 3, 0.12),
    ]

    @pytest.mark.parametrize(
        "value, decimal, expected_result", truncate_value_data
    )
    def test_truncate_value_to_decimal(self, value, decimal, expected_result):
        # Arrange
        sut = ValueFormatter(value)

        # Act
        result = sut.truncate(decimal=decimal).get_formatted_value()

        # Assert
        assert result == expected_result

    round_up_value_data = [
        (0.12345, 1, 0.2),
        (0.12345, 5, 0.12345),
        (0.1234567, 6, 0.123457),
        (0.12, 3, 0.12),
    ]

    @pytest.mark.parametrize(
        "value, decimal, expected_result", round_up_value_data
    )
    def test_round_up_to_decimal(self, value, decimal, expected_result):
        # Arrange
        sut = ValueFormatter(value)

        # Act
        result = sut.round_up(decimal=decimal).get_formatted_value()

        # Assert
        assert result == expected_result

    truncate_and_round_up_value_data = [
        (0.12345, 2, 1, 0.2),
        (0.1234567, 6, 5, 0.12346),
        (0.12, 4, 3, 0.12),
        (0.1111119, 6, 6, 0.111111),
        (0.1000009, 6, 6, 0.1),
        (1.10000004, 3, 2, 1.1),
        (0.28000004, 3, 2, 0.28),
        (2.4300001, 3, 2, 2.43),
        (0.1209, 3, 2, 0.12),
    ]

    @pytest.mark.parametrize(
        "value, truncate_decimal, round_up_decimal, expected_result",
        truncate_and_round_up_value_data,
    )
    def test_truncate_and_round_up_value(
        self, value, truncate_decimal, round_up_decimal, expected_result
    ):
        # Arrange
        sut = ValueFormatter(value)

        # Act
        result = (
            sut.truncate(decimal=truncate_decimal)
            .round_up(decimal=round_up_decimal)
            .get_formatted_value()
        )

        # Assert
        assert result == expected_result
