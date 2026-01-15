import pytest
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock

from src.portfolio.manager import PortfolioManager
from src.database import create_schema, init_db
from src.config import Config, PortfolioConfig, TradingConfig

@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    create_schema(conn)
    return conn

@pytest.fixture
def mock_config():
    config = MagicMock(spec=Config)
    config.portfolio = PortfolioConfig(
        max_alerts_per_day=2,
        max_correlation=0.7,
        cooldown_minutes=60,
        daily_loss_limit_r=2.0
    )
    config.trading = TradingConfig()
    return config

@pytest.fixture
def mock_exchange():
    exchange = MagicMock()
    exchange.fetch_ohlcv = AsyncMock()
    return exchange

@pytest.mark.asyncio
async def test_max_alerts_per_day(db_conn, mock_config):
    manager = PortfolioManager(mock_config, db_conn)
    
    signal = {"symbol": "BTC/USDT:USDT", "confidence": 0.8}
    
    # First signal - Approved
    decision = await manager.add_signal(signal)
    assert decision["status"] == "APPROVED"
    assert manager.signals_today_count == 1
    
    # Second signal - Approved
    decision = await manager.add_signal(signal)
    assert decision["status"] == "APPROVED"
    assert manager.signals_today_count == 2
    
    # Third signal - Rejected
    decision = await manager.add_signal(signal)
    assert decision["status"] == "REJECTED"
    assert "Max alerts per day" in decision["reason"]
    assert manager.signals_today_count == 2

@pytest.mark.asyncio
async def test_cooldown_enforcement(db_conn, mock_config):
    manager = PortfolioManager(mock_config, db_conn)
    
    signal = {"symbol": "ETH/USDT:USDT", "confidence": 0.8}
    
    # First signal
    await manager.add_signal(signal)
    
    # Immediate second signal for same symbol - Rejected
    decision = await manager.add_signal(signal)
    assert decision["status"] == "REJECTED"
    assert "is in cooldown" in decision["reason"]

@pytest.mark.asyncio
async def test_daily_loss_limit(db_conn, mock_config):
    manager = PortfolioManager(mock_config, db_conn)
    
    # Manually insert a closed position with 2.5R loss
    db_conn.execute("INSERT INTO signals (id, symbol, side, entry_price, stop_loss) VALUES (?, ?, ?, ?, ?)", 
                   (1, "BTC/USDT:USDT", "LONG", 100.0, 90.0))
    db_conn.execute("INSERT INTO paper_positions (signal_id, symbol, status, entry_price, exit_price, pnl_r, exit_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (1, "BTC/USDT:USDT", "CLOSED", 100.0, 75.0, -2.5, datetime.now(timezone.utc).isoformat()))
    
    manager.update_state()
    assert manager.daily_pnl_r == -2.5
    
    signal = {"symbol": "SOL/USDT:USDT", "confidence": 0.8}
    decision = await manager.add_signal(signal)
    
    assert decision["status"] == "REJECTED"
    assert "Daily loss limit" in decision["reason"]

@pytest.mark.asyncio
async def test_correlation_gating(db_conn, mock_config, mock_exchange):
    manager = PortfolioManager(mock_config, db_conn, mock_exchange)
    
    # Add an active position
    db_conn.execute("INSERT INTO signals (id, symbol) VALUES (1, 'BTC/USDT:USDT')")
    db_conn.execute("INSERT INTO paper_positions (signal_id, symbol, status) VALUES (1, 'BTC/USDT:USDT', 'OPEN')")
    manager.update_state()
    
    # Mock OHLCV data
    ohlcv_data = [
        [0, 0, 0, 0, 100],
        [0, 0, 0, 0, 105],
        [0, 0, 0, 0, 110],
        [0, 0, 0, 0, 115],
        [0, 0, 0, 0, 120]
    ]
    mock_exchange.fetch_ohlcv.side_effect = [ohlcv_data, ohlcv_data]
    
    signal = {"symbol": "ETH/USDT:USDT", "confidence": 0.8}
    decision = await manager.add_signal(signal)
    
    assert decision["status"] == "REJECTED"
    assert "Average correlation" in decision["reason"]

@pytest.mark.asyncio
async def test_day_boundary_reset(db_conn, mock_config):
    manager = PortfolioManager(mock_config, db_conn)
    manager.signals_today_count = 5
    manager.daily_pnl_r = -3.0
    manager.last_reset_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    
    manager._check_day_boundary()
    
    assert manager.signals_today_count == 0
    assert manager.daily_pnl_r == 0.0
    assert manager.last_reset_date == datetime.now(timezone.utc).date()
