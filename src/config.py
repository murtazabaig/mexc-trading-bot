"""Configuration management for MEXC Futures Signal Bot."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Union

from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator
from loguru import logger


class SignalConfig(BaseModel):
    """Signal generation and scanning configuration."""
    scan_intervals: List[str] = Field(default_factory=lambda: ["1h", "4h", "1d"])
    max_spread_percent: float = Field(default=0.5, gt=0)
    min_volume_usdt: float = Field(default=10000.0, gt=0)
    atr_percent_min: float = Field(default=1.0, gt=0)
    atr_percent_max: float = Field(default=50.0, gt=0)
    min_price: float = Field(default=0.01, gt=0)
    max_price: float = Field(default=1000000.0, gt=0)

    @validator("scan_intervals")
    def validate_timeframes(cls, v: List[str]) -> List[str]:
        """Validate timeframe format."""
        valid_intervals = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "1w", "1M"}
        invalid = set(v) - valid_intervals
        if invalid:
            raise ValueError(f"Invalid timeframe(s): {invalid}. Valid: {valid_intervals}")
        return v


class TradingConfig(BaseModel):
    """Trading and risk management configuration."""
    paper_trading: bool = Field(default=True)
    max_concurrent_positions: int = Field(default=5, gt=0)
    position_size_pct: float = Field(default=2.0, ge=0.1, le=100.0)
    stop_loss_pct: float = Field(default=2.0, gt=0)
    take_profit_pct: float = Field(default=4.0, gt=0)
    max_drawdown_pct: float = Field(default=10.0, gt=0)


class UniverseConfig(BaseModel):
    """Market universe filtering configuration."""
    # Volume filter
    min_volume_usd: float = Field(default=1_000_000, gt=0)
    
    # Spread filter (max bid-ask spread percentage)
    max_spread_percent: float = Field(default=0.05, gt=0)
    
    # Exclusion patterns (stablecoins, leverage tokens, etc.)
    exclude_patterns: List[str] = Field(
        default_factory=lambda: [
            "BUSD",       # BUSD stablecoin
            "UPUSDT",     # Bull leverage tokens (e.g., BTCUP)
            "DOWNUSDT",   # Bear leverage tokens (e.g., BTCDOWN)
            "BEAR",       # Bear tokens
            "BULL",       # Bull tokens
            "3L$",        # 3x leverage long (e.g., BTC3L)
            "3S$",        # 3x leverage short (e.g., BTC3S)
            "5L$",        # 5x leverage long (e.g., BTC5L)
            "5S$",        # 5x leverage short (e.g., BTC5S)
        ]
    )
    
    # Explicit symbol exclusions (including stablecoins)
    exclude_symbols: List[str] = Field(
        default_factory=lambda: [
            "USDTUSDT"   # USDT perpetual against itself
        ]
    )
    
    # Minimum order size
    min_notional: float = Field(default=10, gt=0)
    
    # Minimum price
    min_price: float = Field(default=0.0001, gt=0)
    
    # Maximum price (optional)
    max_price: Optional[float] = Field(default=None)
    
    # Refresh interval in hours
    refresh_interval_hours: float = Field(default=1.0, gt=0)


@dataclass(frozen=True)
class Config:
    """Main configuration container for the bot."""
    
    # Telegram Configuration (required - no defaults)
    telegram_bot_token: str
    telegram_admin_chat_id: str
    telegram_polling_timeout: int = 30
    
    # MEXC API Configuration (optional for public endpoints)
    mexc_api_key: Optional[str] = None
    mexc_api_secret: Optional[str] = None
    mexc_testnet: bool = False
    
    # Database Configuration
    database_path: Union[str, Path] = "data/signals.db"
    
    # Logging Configuration
    log_directory: Union[str, Path] = "logs"
    log_level: str = "INFO"
    
    # Signal Configuration
    signals: SignalConfig = Field(default_factory=SignalConfig)
    
    # Trading Configuration
    trading: TradingConfig = Field(default_factory=TradingConfig)
    
    # Universe Configuration
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    
    # Application Settings
    environment: str = "production"
    debug: bool = False
    
    @classmethod
    def from_env(cls, env_file: Optional[Union[str, Path]] = None) -> "Config":
        """Load configuration from environment variables and .env file."""
        
        # Load .env file if provided or .env in project root
        if env_file:
            load_dotenv(env_file)
        else:
            project_root = Path(__file__).parent.parent
            env_path = project_root / ".env"
            if env_path.exists():
                load_dotenv(env_path)
            else:
                load_dotenv()
        
        # Required Configuration
        required_vars = {
            "TELEGRAM_BOT_TOKEN": "telegram_bot_token",
            "TELEGRAM_ADMIN_CHAT_ID": "telegram_admin_chat_id",
        }
        
        config_data = {}
        missing_required = []
        
        for env_var, field_name in required_vars.items():
            value = os.getenv(env_var)
            if not value:
                missing_required.append(env_var)
            else:
                config_data[field_name] = value
        
        if missing_required:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_required)}"
            )
        
        # Optional Configuration
        config_data.update({
            "mexc_api_key": os.getenv("MEXC_API_KEY"),
            "mexc_api_secret": os.getenv("MEXC_API_SECRET"),
            "mexc_testnet": os.getenv("MEXC_TESTNET", "false").lower() == "true",
            "database_path": os.getenv("DATABASE_PATH", "data/signals.db"),
            "log_directory": os.getenv("LOG_DIRECTORY", "logs"),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "environment": os.getenv("ENVIRONMENT", "production"),
            "debug": os.getenv("DEBUG", "false").lower() == "true",
        })
        
        # Signal Configuration
        signal_config_data = {}
        if scan_intervals := os.getenv("SIGNAL_SCAN_INTERVALS"):
            signal_config_data["scan_intervals"] = [interval.strip() for interval in scan_intervals.split(",")]
        
        for key in ["max_spread_percent", "min_volume_usdt", "atr_percent_min", "atr_percent_max", "min_price", "max_price"]:
            if value := os.getenv(f"SIGNAL_{key.upper()}"):
                signal_config_data[key] = float(value)
        
        config_data["signals"] = SignalConfig(**signal_config_data)
        
        # Trading Configuration
        trading_config_data = {}
        for key in ["paper_trading", "max_concurrent_positions", "position_size_pct", "stop_loss_pct", "take_profit_pct", "max_drawdown_pct"]:
            env_key = f"TRADING_{key.upper()}"
            if value := os.getenv(env_key):
                if key == "paper_trading":
                    trading_config_data[key] = value.lower() == "true"
                else:
                    trading_config_data[key] = float(value) if "." in value else int(value)
        
        config_data["trading"] = TradingConfig(**trading_config_data)
        
        # Universe Configuration
        universe_config_data = {}
        for key in ["min_volume_usd", "max_spread_percent", "min_notional", "min_price", "max_price", "refresh_interval_hours"]:
            env_key = f"UNIVERSE_{key.upper()}"
            if value := os.getenv(env_key):
                if key == "exclude_patterns" or key == "exclude_symbols":
                    # List handling
                    universe_config_data[key] = [item.strip() for item in value.split(",")]
                elif key == "max_price" and value.lower() == "none":
                    universe_config_data[key] = None
                else:
                    universe_config_data[key] = float(value)
        
        # Handle list-type fields
        if exclude_patterns := os.getenv("UNIVERSE_EXCLUDE_PATTERNS"):
            universe_config_data["exclude_patterns"] = [p.strip() for p in exclude_patterns.split(",")]
        
        if exclude_symbols := os.getenv("UNIVERSE_EXCLUDE_SYMBOLS"):
            universe_config_data["exclude_symbols"] = [s.strip() for s in exclude_symbols.split(",")]
        
        config_data["universe"] = UniverseConfig(**universe_config_data)
        
        return cls(**config_data)
    
    def validate(self) -> None:
        """Validate complete configuration."""
        logger.info("Validating configuration...")
        
        # Validate paths exist or can be created
        log_dir = Path(self.log_directory)
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Log directory: {log_dir.resolve()}")
        except Exception as e:
            raise ValueError(f"Invalid log directory: {e}")
        
        data_dir = Path(self.database_path).parent
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Data directory: {data_dir.resolve()}")
        except Exception as e:
            raise ValueError(f"Cannot create data directory: {e}")
        
        # Validate Telegram bot token format
        if not self.telegram_bot_token.startswith(("1234567890:", "10:", "11:")):
            logger.warning("Telegram bot token format looks unusual")
        
        # Validate trading parameters
        if self.trading.stop_loss_pct >= 50.0:
            logger.warning("Stop loss percentage is very high")
        
        if self.trading.take_profit_pct <= self.trading.stop_loss_pct:
            logger.warning("Take profit percentage should be greater than stop loss")
        
        logger.success("Configuration validated successfully")
    
    def __post_init__(self):
        """Convert string paths to Path objects."""
        if isinstance(self.database_path, str):
            object.__setattr__(self, "database_path", Path(self.database_path))
        
        if isinstance(self.log_directory, str):
            object.__setattr__(self, "log_directory", Path(self.log_directory))
        
        if isinstance(self.signals, dict):
            object.__setattr__(self, "signals", SignalConfig(**self.signals))
        
        if isinstance(self.trading, dict):
            object.__setattr__(self, "trading", TradingConfig(**self.trading))
        
        if isinstance(self.universe, dict):
            object.__setattr__(self, "universe", UniverseConfig(**self.universe))