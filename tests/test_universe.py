"""Unit tests for market universe module."""

import json
import pytest
from pathlib import Path

from src.universe import (
    UniverseConfig,
    filter_markets,
    is_above_min_volume,
    is_below_max_spread,
    is_not_excluded,
    meets_notional_requirement,
    meets_price_range,
    compare_universes
)


# Load sample market data
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_sample_markets() -> dict:
    """Load sample market data from fixtures."""
    with open(FIXTURES_DIR / "sample_markets.json", "r") as f:
        return json.load(f)


class TestIsAboveMinVolume:
    """Test volume filter function."""
    
    @pytest.fixture
    def markets(self):
        return load_sample_markets()
    
    def test_high_volume_passes(self, markets):
        """Test that high volume markets pass the filter."""
        result, reason = is_above_min_volume(markets["BTCUSDT"], 1_000_000)
        assert result is True
        assert reason == ""
    
    def test_low_volume_fails(self, markets):
        """Test that low volume markets fail the filter."""
        result, reason = is_above_min_volume(markets["LOWVOLUSDT"], 1_000_000)
        assert result is False
        assert "500,000" in reason
    
    def test_medium_volume_passes(self, markets):
        """Test that volume at threshold passes."""
        result, reason = is_above_min_volume(markets["SOLUSDT"], 500_000_000)
        assert result is True
        assert reason == ""
    
    def test_medium_volume_fails(self, markets):
        """Test that volume just below threshold fails."""
        result, reason = is_above_min_volume(markets["SOLUSDT"], 600_000_000)
        assert result is False
        assert "500,000,000" in reason


class TestIsBelowMaxSpread:
    """Test spread filter function."""
    
    @pytest.fixture
    def markets(self):
        return load_sample_markets()
    
    def test_tight_spread_passes(self, markets):
        """Test that tight spread markets pass."""
        result, reason = is_below_max_spread(markets["BTCUSDT"], 0.05)
        assert result is True
        assert reason == ""
    
    def test_wide_spread_fails(self, markets):
        """Test that wide spread markets fail."""
        # BTC: bid=50000, ask=50002.50, spread = 0.005%
        # HIGHSPREAD: bid=100, ask=100.50, spread = 0.5%
        result, reason = is_below_max_spread(markets["HIGHSPREADUSDT"], 0.05)
        assert result is False
        assert "0.5%" in reason or "Spread 0.5000%" in reason
    
    def test_spread_at_threshold(self, markets):
        """Test spread at exactly the threshold."""
        result, reason = is_below_max_spread(markets["BTCUSDT"], 0.005)
        assert result is True
    
    def test_no_price_data(self, markets):
        """Test handling missing price data."""
        market_no_price = markets["BTCUSDT"].copy()
        market_no_price["info"] = {"bidPrice": None, "askPrice": None}
        result, reason = is_below_max_spread(market_no_price, 0.05)
        assert result is True
        assert "No spread data" in reason


class TestIsNotExcluded:
    """Test exclusion filter function."""
    
    @pytest.fixture
    def config(self):
        return UniverseConfig()
    
    def test_normal_symbol_passes(self, config):
        """Test that normal symbols pass."""
        result, reason = is_not_excluded("BTCUSDT", config.exclude_patterns, config.exclude_symbols)
        assert result is True
        assert reason == ""
    
    def test_usdt_stablecoin_excluded(self, config):
        """Test that USDT base coin is excluded."""
        result, reason = is_not_excluded("USDTUSDT", config.exclude_patterns, config.exclude_symbols)
        assert result is False
        # USDTUSDT is in exclude_symbols
        assert "explicitly excluded" in reason.lower() or "exclusion" in reason.lower()
    
    def test_busd_stablecoin_excluded(self, config):
        """Test that BUSD is excluded."""
        result, reason = is_not_excluded("BUSDUSDT", config.exclude_patterns, config.exclude_symbols)
        assert result is False
        assert "BUSD" in reason
    
    def test_up_token_excluded(self, config):
        """Test that UP leverage tokens are excluded."""
        result, reason = is_not_excluded("BTCUPUSDT", config.exclude_patterns, config.exclude_symbols)
        assert result is False
        assert "UPUSDT" in reason or "UP" in reason
    
    def test_down_token_excluded(self, config):
        """Test that DOWN leverage tokens are excluded."""
        result, reason = is_not_excluded("BTCDOWNUSDT", config.exclude_patterns, config.exclude_symbols)
        assert result is False
        assert "DOWNUSDT" in reason or "DOWN" in reason
    
    def test_explicit_exclusion(self, config):
        """Test explicit symbol exclusion."""
        result, reason = is_not_excluded("BTCUSDT", config.exclude_patterns, ["BTCUSDT"])
        assert result is False
        assert "BTCUSDT" in reason
    
    def test_custom_patterns(self):
        """Test custom exclusion patterns."""
        result, reason = is_not_excluded("TESTBEARUSDT", ["BEAR"], [])
        assert result is False
        assert "BEAR" in reason


class TestMeetsNotionalRequirement:
    """Test notional filter function."""
    
    @pytest.fixture
    def markets(self):
        return load_sample_markets()
    
    def test_standard_notional_passes(self, markets):
        """Test that standard notional passes."""
        result, reason = meets_notional_requirement(markets["BTCUSDT"], 10)
        assert result is True
        assert reason == ""
    
    def test_high_notional_fails(self, markets):
        """Test that markets with high minimum order size fail."""
        # All our sample markets have min cost of 10, so setting max allowed to 5 should fail
        result, reason = meets_notional_requirement(markets["BTCUSDT"], 5)
        assert result is False
        assert "5" in reason


class TestMeetsPriceRange:
    """Test price range filter function."""
    
    @pytest.fixture
    def markets(self):
        return load_sample_markets()
    
    def test_normal_price_passes(self, markets):
        """Test that normal prices pass."""
        result, reason = meets_price_range(markets["BTCUSDT"], 0.0001, None)
        assert result is True
        assert reason == ""
    
    def test_price_too_low(self, markets):
        """Test that prices below minimum fail."""
        result, reason = meets_price_range(markets["DOGEUSDT"], 1.0, None)
        # DOGE price is 0.15
        assert result is False
        assert "0.15" in reason or reason != ""
    
    def test_price_too_high(self, markets):
        """Test that prices above maximum fail."""
        result, reason = meets_price_range(markets["BTCUSDT"], 0.0001, 1000.0)
        # BTC price is 50001.25
        assert result is False
        assert "50001.25" in reason or reason != ""
    
    def test_no_max_limit(self, markets):
        """Test that no max limit works correctly."""
        result, reason = meets_price_range(markets["BTCUSDT"], 0.0001, None)
        assert result is True


class TestFilterMarkets:
    """Test the comprehensive filter_markets function."""
    
    @pytest.fixture
    def markets(self):
        return load_sample_markets()
    
    @pytest.fixture
    def default_config(self):
        return UniverseConfig(
            min_volume_usd=1_000_000,
            max_spread_percent=0.05,
            min_notional=10,
            min_price=0.0001,
        )
    
    def test_filters_all_criteria(self, markets, default_config):
        """Test that all filters are applied correctly."""
        filtered = filter_markets(markets, default_config)
        
        # Should exclude: USDTUSDT (in exclude_symbols), BUSDUSDT (BUSD pattern), 
        # BTCUPUSDT (UPUSDT pattern), BTCDOWNUSDT (DOWNUSDT pattern), LOWVOLUSDT (low volume)
        # Should include: BTCUSDT, ETHUSDT, SOLUSDT, DOGEUSDT
        # HIGHSPREADUSDT has wide spread (0.5%) so it should be excluded
        
        # Verify USDTUSDT is excluded
        assert "USDTUSDT" not in filtered
        
        expected_symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"}
        assert set(filtered.keys()) == expected_symbols
    
    def test_custom_exclude_patterns(self, markets):
        """Test custom exclusion patterns."""
        config = UniverseConfig(
            min_volume_usd=100_000,  # Lower to allow more through
            max_spread_percent=1.0,  # Higher to allow wide spread
            exclude_patterns=["^BTC"],  # Exclude all BTC symbols
            min_notional=10,
        )
        
        filtered = filter_markets(markets, config)
        
        # BTCUSDT should be excluded
        assert "BTCUSDT" not in filtered
        # BTCUP should also be excluded
        assert "BTCUPUSDT" not in filtered
        # BTCDOWN should be excluded
        assert "BTCDOWNUSDT" not in filtered
        # ETHUSDT should be included
        assert "ETHUSDT" in filtered
    
    def test_no_markets_returns_empty(self):
        """Test handling empty input."""
        config = UniverseConfig()
        filtered = filter_markets({}, config)
        assert filtered == {}
    
    def test_high_min_volume(self, markets):
        """Test with very high minimum volume."""
        config = UniverseConfig(
            min_volume_usd=10_000_000_000,  # 10B - very high
            max_spread_percent=0.05,
            min_notional=10,
        )
        
        filtered = filter_markets(markets, config)
        # Only BTC has 5B volume, so none should pass 10B
        assert len(filtered) == 0


class TestCompareUniverses:
    """Test universe comparison function."""
    
    def test_no_changes(self):
        """Test comparison of identical universes."""
        old = {"BTCUSDT": {}, "ETHUSDT": {}}
        new = {"BTCUSDT": {}, "ETHUSDT": {}}
        
        changes = compare_universes(old, new)
        assert changes["added"] == []
        assert changes["removed"] == []
    
    def test_additions(self):
        """Test detection of added markets."""
        old = {"BTCUSDT": {}}
        new = {"BTCUSDT": {}, "ETHUSDT": {}, "SOLUSDT": {}}
        
        changes = compare_universes(old, new)
        assert set(changes["added"]) == {"ETHUSDT", "SOLUSDT"}
        assert changes["removed"] == []
    
    def test_removals(self):
        """Test detection of removed markets."""
        old = {"BTCUSDT": {}, "ETHUSDT": {}, "SOLUSDT": {}}
        new = {"BTCUSDT": {}}
        
        changes = compare_universes(old, new)
        assert changes["added"] == []
        assert set(changes["removed"]) == {"ETHUSDT", "SOLUSDT"}
    
    def test_both_changes(self):
        """Test detection of both additions and removals."""
        old = {"BTCUSDT": {}, "ETHUSDT": {}}
        new = {"BTCUSDT": {}, "SOLUSDT": {}}
        
        changes = compare_universes(old, new)
        assert changes["added"] == ["SOLUSDT"]
        assert changes["removed"] == ["ETHUSDT"]
    
    def test_empty_old_universe(self):
        """Test comparison with empty old universe."""
        old = {}
        new = {"BTCUSDT": {}}
        
        changes = compare_universes(old, new)
        assert changes["added"] == ["BTCUSDT"]
        assert changes["removed"] == []
    
    def test_empty_new_universe(self):
        """Test comparison with empty new universe."""
        old = {"BTCUSDT": {}}
        new = {}
        
        changes = compare_universes(old, new)
        assert changes["added"] == []
        assert changes["removed"] == ["BTCUSDT"]


class TestUniverseConfig:
    """Test UniverseConfig dataclass."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = UniverseConfig()
        assert config.min_volume_usd == 1_000_000
        assert config.max_spread_percent == 0.05
        assert config.min_notional == 10
        assert config.min_price == 0.0001
        assert config.max_price is None
        assert len(config.exclude_patterns) > 0
        assert "BUSD" in config.exclude_patterns
        assert "UPUSDT" in config.exclude_patterns
        assert "DOWNUSDT" in config.exclude_patterns
        assert len(config.exclude_symbols) > 0
        assert "USDTUSDT" in config.exclude_symbols
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = UniverseConfig(
            min_volume_usd=5_000_000,
            max_spread_percent=0.1,
            min_notional=50,
            min_price=0.01,
            max_price=100000.0,
        )
        assert config.min_volume_usd == 5_000_000
        assert config.max_spread_percent == 0.1
        assert config.min_notional == 50
        assert config.min_price == 0.01
        assert config.max_price == 100000.0
    
    def test_custom_exclude_patterns(self):
        """Test custom exclusion patterns."""
        config = UniverseConfig(
            exclude_patterns=["^TEST", "MOCK"],
        )
        assert "^TEST" in config.exclude_patterns
        assert "MOCK" in config.exclude_patterns
    
    def test_custom_exclude_symbols(self):
        """Test custom exclusion symbols."""
        config = UniverseConfig(
            exclude_symbols=["BTCUSDT", "ETHUSDT"],
        )
        assert "BTCUSDT" in config.exclude_symbols
        assert "ETHUSDT" in config.exclude_symbols


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
