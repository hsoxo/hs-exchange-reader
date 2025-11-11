from decimal import Decimal


def precision(x):
    if x is None:
        return None
    s = str(x).rstrip("0")
    return len(s.split(".")[-1]) if "." in s else 0


def to_decimal_str(precision: int) -> str:
    """
    将数值转换为固定小数位字符串，避免科学计数法。
    """
    d = Decimal(1) / (Decimal(10) ** precision)
    return f"{d:.{precision}f}"
