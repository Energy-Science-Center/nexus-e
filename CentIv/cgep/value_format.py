import decimal as dec

class ValueFormatter:
    def __init__(self, value: float):
        self.__value = dec.Decimal(str(value))

    def truncate(self, decimal: int):
        shift = 10**decimal
        self.__value = self.__value.quantize(
            exp=dec.Decimal(1)/shift,
            rounding=dec.ROUND_FLOOR
        )
        return self

    def round_up(self, decimal: int):
        shift = 10**decimal
        self.__value = self.__value.quantize(
            exp=dec.Decimal(1)/shift,
            rounding=dec.ROUND_CEILING
        )
        return self

    def get_formatted_value(self) -> float:
        return float(self.__value)