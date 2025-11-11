from enum import IntEnum

class SymbolStatus(IntEnum):
    ACTIVE = 0     # 交易中
    HALTED = 1     # 暂停
    PENDING = 2    # 待上线
    CLOSED = 3     # 已下线
