from typing import Optional
import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKeyConstraint, Index, String, text
from sqlalchemy.dialects.mysql import SMALLINT, TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class ExchangeInfo(Base):
    __tablename__ = 'exchange_info'
    __table_args__ = {'comment': '交易所信息表'}

    id: Mapped[int] = mapped_column(SMALLINT, primary_key=True, comment='交易所ID')
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment='英文标识')
    venue_type: Mapped[int] = mapped_column(TINYINT, nullable=False, server_default=text("'0'"), comment='交易所类型：0=CEX中心化交易所, 1=DEX去中心化交易所')
    display_name: Mapped[Optional[str]] = mapped_column(String(100), comment='展示名称')

    exchange_symbol: Mapped[list['ExchangeSymbol']] = relationship('ExchangeSymbol', back_populates='exchange')


class ExchangeSymbol(Base):
    __tablename__ = 'exchange_symbol'
    __table_args__ = (
        ForeignKeyConstraint(['exchange_id'], ['exchange_info.id'], ondelete='CASCADE', onupdate='CASCADE', name='fk_exchange_symbol_exchange'),
        Index('idx_exchange_inst', 'exchange_id', 'inst_type'),
        Index('idx_status', 'status'),
        {'comment': '各交易所交易对元数据表'}
    )

    exchange_id: Mapped[int] = mapped_column(SMALLINT, primary_key=True, comment='交易所标识')
    symbol: Mapped[str] = mapped_column(String(100), primary_key=True, comment='交易对唯一标识')
    inst_type: Mapped[int] = mapped_column(TINYINT, primary_key=True, comment='产品类别：0:SPOT现货, 1:perpetual永续合约, 2:FUTURES交割合约, 3:OPTION期权')
    base_asset: Mapped[Optional[str]] = mapped_column(String(30), comment='基础资产')
    quote_asset: Mapped[Optional[str]] = mapped_column(String(20), comment='计价资产')
    price_precision: Mapped[Optional[int]] = mapped_column(TINYINT, comment='价格精度')
    quantity_precision: Mapped[Optional[int]] = mapped_column(TINYINT, comment='数量精度')
    tick_size: Mapped[Optional[str]] = mapped_column(String(24), comment='价格最小变动步长')
    step_size: Mapped[Optional[str]] = mapped_column(String(24), comment='数量最小变动步长')
    status: Mapped[Optional[int]] = mapped_column(TINYINT, comment='交易状态：0=ACTIVE(交易中), 1=HALTED(暂停), 2=PENDING(待上线),3 =CLOSED(已下线)')
    onboard_time: Mapped[Optional[int]] = mapped_column(BigInteger, comment='上架时间，即该交易对首次可交易的时间')
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, server_default=text('CURRENT_TIMESTAMP'), comment='创建时间')
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='最后更新时间')

    exchange: Mapped['ExchangeInfo'] = relationship('ExchangeInfo', back_populates='exchange_symbol')
