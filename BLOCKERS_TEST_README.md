# Blocker Integration Tests

Comprehensive local test suite that validates all 3 blockers work correctly without requiring live API keys or external services.

## Overview

This test suite validates the three critical blockers in the MEXC Signal Bot:

1. **Blocker 1 - Runtime Orchestration**: PauseState and portfolio manager controls
2. **Blocker 2 - Candle-Close Discipline**: Processed candles state tracking
3. **Blocker 3 - Multi-Timeframe Confluence**: MTF blocking logic

## Quick Start

```bash
# Run all blocker tests
python3 run_tests.py

# Or run directly
python3 tests/test_blockers_integration.py
```

## Test Structure

### Blocker 1: Runtime Orchestration (4 tests)

#### 1. `test_pause_state_singleton`
- Validates `PauseState` class exists and works
- Tests pause/resume functionality
- Verifies reason tracking

#### 2. `test_pause_state_blocks_scanner`
- Tests that scanner respects pause state
- Validates pause prevents scan execution

#### 3. `test_portfolio_manager_integration`
- Tests portfolio manager signal approval/rejection
- Validates config integration
- Tests async signal processing

#### 4. `test_warning_detector_pause_state`
- Tests warning detector can trigger pause state
- Validates CRITICAL warnings pause system

### Blocker 2: Candle-Close Discipline (6 tests)

#### 1. `test_processed_candles_table_exists`
- Validates `processed_candles` table in schema
- Tests table creation

#### 2. `test_candle_close_state_tracking`
- Tests `get_last_processed_candle()` function
- Tests `update_processed_candle()` function
- Validates duplicate detection logic

#### 3. `test_get_last_closed_candle_ts`
- Tests extraction of last closed candle from OHLCV
- Validates second-to-last candle selection (avoiding look-ahead bias)

#### 4. `test_insufficient_data_handling`
- Tests handling of insufficient OHLCV data
- Validates None return for single candle

#### 5. `test_multiple_timeframes_independent_tracking`
- Tests that 5m, 1h, 4h are tracked independently
- Validates per-symbol, per-timeframe state

#### 6. `test_duplicate_candle_detection_logic`
- Tests logic for detecting already-processed candles
- Validates skip/process decisions

### Blocker 3: Multi-Timeframe Confluence (6 tests)

#### 1. `test_mtf_confluence_blocks_long_on_bearish_1h`
- Tests LONG signals blocked when 1h trend is bearish
- Validates -3.0 penalty and aligned=False

#### 2. `test_mtf_confluence_blocks_short_on_bullish_1h`
- Tests SHORT signals blocked when 1h trend is bullish
- Validates -3.0 penalty and aligned=False

#### 3. `test_mtf_confluence_applies_penalty_for_weak_4h`
- Tests -1.5 penalty for weak 4h alignment
- Validates aligned=True with penalty

#### 4. `test_mtf_confluence_allows_strong_alignment`
- Tests signals allowed with strong alignment
- Validates 0.0 penalty when all timeframes aligned

#### 5. `test_signal_scoring_with_mtf_penalty`
- Tests that MTF penalties reduce score correctly
- Validates threshold enforcement

#### 6. `test_mtf_penalty_threshold_enforcement`
- Tests 7.0 score threshold is enforced after penalties
- Validates rejection below threshold

### Integration Tests (2 tests)

#### 1. `test_full_signal_pipeline_with_all_blockers`
- Tests full pipeline with all 3 blockers active
- Validates end-to-end flow

#### 2. `test_critical_warning_blocks_all_signals`
- Tests CRITICAL warning pauses system
- Validates all signals blocked when paused

## Test Design

### No External Dependencies
- All tests use in-memory SQLite databases (`:memory:`)
- No live API keys required
- No network calls made
- Fully isolated and reproducible

### Mock Components
- `exchange=None` - No CCXT exchange needed
- `db_conn=:memory:` - In-memory database
- Mock configurations with realistic values
- Mock OHLCV data arrays

### Test Coverage
- **Total Tests**: 18
- **Blocker 1**: 4 tests
- **Blocker 2**: 6 tests
- **Blocker 3**: 6 tests
- **Integration**: 2 tests

## Expected Output

```
============================================================
MEXC Signal Bot - Blocker Integration Tests
============================================================

TestBlocker1RuntimeOrchestration
------------------------------------------------------------
✓ Test Pause State Blocks Scanner
✓ Test Pause State Singleton
✓ Test Portfolio Manager Integration
✓ Test Warning Detector Pause State

TestBlocker2CandleCloseStateTracking
------------------------------------------------------------
✓ Test Candle Close State Tracking
✓ Test Duplicate Candle Detection Logic
✓ Test Get Last Closed Candle Ts
✓ Test Insufficient Data Handling
✓ Test Multiple Timeframes Independent Tracking
✓ Test Processed Candles Table Exists

TestBlocker3MultiTimeframeConfluence
------------------------------------------------------------
✓ Test Mtf Confluence Allows Strong Alignment
✓ Test Mtf Confluence Applies Penalty For Weak 4H
✓ Test Mtf Confluence Blocks Long On Bearish 1H
✓ Test Mtf Confluence Blocks Short On Bullish 1H
✓ Test Mtf Penalty Threshold Enforcement
✓ Test Signal Scoring With Mtf Penalty

TestBlockersIntegrationScenarios
------------------------------------------------------------
✓ Test Critical Warning Blocks All Signals
✓ Test Full Signal Pipeline With All Blockers

============================================================
Result: 18/18 PASSED
All blockers verified and working correctly! ✅
============================================================
```

## What This Validates

### Blocker 1: Runtime Orchestration
- ✅ PauseState singleton works correctly
- ✅ Pause/resume functionality operational
- ✅ Portfolio manager can approve/reject signals
- ✅ Warning detector can trigger pause state
- ✅ Scanner respects pause state

### Blocker 2: Candle-Close Discipline
- ✅ `processed_candles` table exists and functions
- ✅ Duplicate candle detection prevents re-processing
- ✅ Last closed candle extraction avoids look-ahead bias
- ✅ Multiple timeframes tracked independently
- ✅ State persists across database operations
- ✅ Insufficient data handled gracefully

### Blocker 3: Multi-Timeframe Confluence
- ✅ LONG signals blocked on bearish 1h trend
- ✅ SHORT signals blocked on bullish 1h trend
- ✅ Score penalties applied for weak alignment
- ✅ Strong alignment allows signals (0 penalty)
- ✅ Threshold (7.0) enforced after penalties
- ✅ No false positives in confluence checks

## Implementation Details

### Test File Location
```
tests/test_blockers_integration.py
```

### Runner Script
```
run_tests.py
```

### Key Imports
```python
from src.state.pause import PauseState
from src.database import init_db, create_schema, get_last_processed_candle, update_processed_candle
from src.jobs.scanner import ScannerJob
from src.portfolio.manager import PortfolioManager
from src.warnings.detector import WarningDetector
```

### Mock Configuration Example
```python
class MockConfig:
    class Portfolio:
        max_alerts_per_day = 5
        max_correlation = 0.7
        cooldown_minutes = 60
        daily_loss_limit_r = 10.0
    
    class Trading:
        initial_capital = 10000
        risk_per_trade_pct = 2.0
        max_position_size_pct = 10.0
    
    portfolio = Portfolio()
    trading = Trading()
```

### Mock OHLCV Data Example
```python
ohlcv = [
    [1704067200000, 100.0, 101.0, 99.0, 100.5, 1000],  # Closed candle
    [1704070800000, 100.5, 102.0, 100.0, 101.5, 1500],  # Forming candle
]
```

## Running Specific Tests

To run a specific test class, modify the test file to only execute that class:

```python
if __name__ == "__main__":
    import sys
    success = TestBlocker1RuntimeOrchestration().test_pause_state_singleton()
    sys.exit(0 if success else 1)
```

## Troubleshooting

### ModuleNotFoundError: No module named 'src'
- Ensure you're running from the project root
- The test file automatically adds the project to sys.path

### Database Errors
- Tests use in-memory databases that are cleaned up automatically
- Each test creates its own isolated database

### Async Errors
- Portfolio manager's `add_signal()` is async
- Use `asyncio.run()` to execute async methods in tests

## Continuous Integration

These tests are designed to run in CI/CD environments:

```yaml
# Example GitHub Actions workflow
- name: Run Blocker Tests
  run: python3 run_tests.py
```

## Test Execution Time

- **Full suite**: ~1-2 seconds
- **No network I/O**: Instant execution
- **No API rate limits**: Can run repeatedly

## Maintenance

### Adding New Tests
1. Create test method in appropriate test class
2. Follow naming convention: `test_descriptive_name`
3. Use in-memory database with `init_db(":memory:")`
4. Clean up resources with `db.close()`

### Updating Tests
- When blocker logic changes, update corresponding tests
- Maintain expected vs actual assertions
- Keep test data realistic

## Success Criteria

All tests must pass for the blocker system to be considered functional:

- ✅ 18/18 tests passing
- ✅ No live API dependencies
- ✅ Reproducible results
- ✅ Fast execution (<5 seconds)

## Additional Resources

- **Main Scanner**: `src/jobs/scanner.py`
- **Database Schema**: `src/database.py`
- **Portfolio Manager**: `src/portfolio/manager.py`
- **Warning Detector**: `src/warnings/detector.py`
- **Pause State**: `src/state/pause.py`
