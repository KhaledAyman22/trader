from sqlalchemy import Column, Integer, String, Float, DateTime, JSON
from datetime import datetime
from .postgres import Base

class SignalHistory(Base):
    __tablename__ = "signal_history"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    price = Column(Float)
    signal_type = Column(String)
    technical_indicators = Column(JSON)
    market_depth = Column(JSON)
    trade_flow = Column(JSON)
    signal_strength = Column(Float)
    market_cap = Column(Float)
    sector = Column(String)
    target = Column(Float)
    buy_price = Column(Float)
    stop_loss = Column(Float)
    
class Subscriber(Base):
    __tablename__ = "subscribers"
    
    chat_id = Column(String, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)