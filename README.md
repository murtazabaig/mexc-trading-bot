# MEXC Futures Signal Bot

Advanced automated trading signal detection system for MEXC futures markets.

## Overview

This bot monitors MEXC futures markets and generates trading signals based on technical analysis, market regime detection, and risk management rules. Signals are delivered via Telegram for manual execution or can trigger paper trading for strategy validation.

## Features

- **Multi-timeframe scanning** (1h, 4h, 1d by default)
- **Market regime detection** (trending, ranging, volatile)
- **Technical indicator analysis**
- **Risk management filters**
- **Telegram integration** with admin commands
- **Paper trading mode** for strategy testing
- **SQLite database** for signal history and performance tracking

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd mexc-futures-signal-bot
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configuration

Copy the example environment file:

```bash
cp config/.env.example config/.env
```

Edit `config/.env` with your credentials:

```bash
# Required
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_ADMIN_CHAT_ID=your_chat_id_here

# Optional (for private MEXC endpoints)
MEXC_API_KEY=your_api_key_here
MEXC_API_SECRET=your_api_secret_here
```

### 3. Run the Bot

```bash
python src/main.py
```

### Docker Deployment

```bash
# Build the image
docker build -t mexc-signal-bot .

# Run with compose
docker-compose up -d

# View logs
docker-compose logs -f mexc-bot
```

## Configuration Options

See `config/.env.example` for all available configuration options:

- **Scanning intervals**: Custom timeframes for signal generation
- **Risk thresholds**: Spread, volume, and volatility filters
- **Trading parameters**: Position sizing, stop-loss, take-profit
- **Database and logging**: Custom paths and log levels

## Architecture

```
src/
├── config.py              # Configuration management
├── logger.py              # Logging setup
├── database.py            # SQLite operations
├── main.py                # Application entry point
├── universe/              # Market universe management
├── scanner/              # Market scanning and signal generation
├── indicators/           # Technical analysis indicators
├── regime/               # Market regime detection
└── telegram_bot/         # Telegram bot commands
```

## Development

### Project Structure

- `requirements.txt` - Python dependencies
- `pyproject.toml` - Package metadata and build configuration
- `Dockerfile` - Multi-stage build for production
- `docker-compose.yml` - Container orchestration

### Testing

```bash
# Run tests
pytest tests/

# Linting
flake8 src/
black src/
```

### Adding New Indicators

Create new indicator classes in `src/indicators/` and register them with the signal scanner.

### Database Schema

The SQLite database includes tables for:
- `signals` - Generated trading signals
- `warnings` - System warnings and errors
- `paper_positions` - Paper trading positions
- `parameters` - Runtime parameters and configuration

## Security

- **No secrets in repository**: All credentials via environment variables
- **Optional API access**: Bot works with public MEXC endpoints
- **Paper trading mode**: Test strategies without real capital
- **Environment isolation**: Docker containers for deployment

## Support

For issues and feature requests, please use GitHub Issues.

## License

MIT License - see LICENSE file for details.