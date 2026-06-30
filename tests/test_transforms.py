import json

import pandas as pd
import pytest

from assets.gold_trending_prices import enrich_trending_with_btc_prices
from assets.silver_coins import parse_coin_payload, transform_bronze_coins_to_silver
from assets.silver_trending import parse_trending_payload, transform_bronze_trending_to_silver


def build_coin_json(coin_id: str, price_usd: float | None = 50000.0, cap: float | None = 1e12) -> str:
    return json.dumps({
        "id": coin_id,
        "symbol": coin_id[:3],
        "name": coin_id.capitalize(),
        "market_data": {
            "current_price": {"usd": price_usd},
            "market_cap": {"usd": cap},
            "total_volume": {"usd": 1e9},
            "price_change_percentage_24h": 1.5,
        },
    })


def build_trending_json(coins: list[tuple[str, int | None, float]]) -> str:
    items = [
        {"item": {"id": cid, "name": cid.capitalize(), "symbol": cid[:3], "market_cap_rank": rank, "price_btc": pbtc}}
        for cid, rank, pbtc in coins
    ]
    return json.dumps({"coins": items})


class TestParseCoinPayload:
    def test_valid_coin(self):
        raw = build_coin_json("bitcoin", 50000.0)
        ts = "2026-06-29T10:00:00"
        result = parse_coin_payload(raw, ts)
        assert result["coin_id"] == "bitcoin"
        assert result["price_usd"] == 50000.0
        assert result["market_cap_usd"] == 1e12
        assert result["extracted_at"] == pd.Timestamp(ts)

    def test_null_fields(self):
        raw = json.dumps({"id": "test", "symbol": "tst", "name": "Test", "market_data": {}})
        ts = "2026-06-29T10:00:00"
        result = parse_coin_payload(raw, ts)
        assert result["price_usd"] is None
        assert result["market_cap_usd"] is None
        assert result["price_change_percentage_24h"] is None


class TestParseTrendingPayload:
    def test_multiple_coins(self):
        raw = build_trending_json([
            ("coin-a", 10, 0.01),
            ("coin-b", 20, 0.005),
            ("coin-c", None, 0.001),
        ])
        ts = "2026-06-29T10:00:00"
        result = parse_trending_payload(raw, ts)
        assert len(result) == 3
        assert result[0]["coin_id"] == "coin-a"
        assert result[0]["trending_rank"] == 1
        assert result[1]["trending_rank"] == 2
        assert result[2]["market_cap_rank"] is None

    def test_empty_coins(self):
        raw = json.dumps({"coins": []})
        result = parse_trending_payload(raw, "2026-06-29T10:00:00")
        assert result == []


class TestTransformBronzeCoinsToSilver:
    def test_single_row(self):
        raw = build_coin_json("bitcoin", 50000.0)
        df = pd.DataFrame([{"ingestion_timestamp": "2026-06-29T10:00:00", "raw_payload": raw}])
        result = transform_bronze_coins_to_silver(df)
        assert len(result) == 1
        assert result.iloc[0]["coin_id"] == "bitcoin"

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["ingestion_timestamp", "raw_payload"])
        result = transform_bronze_coins_to_silver(df)
        assert result.empty

    def test_skips_bad_rows(self):
        good_raw = build_coin_json("bitcoin", 50000.0)
        rows = [
            {"ingestion_timestamp": "2026-06-29T10:00:00", "raw_payload": good_raw},
            {"ingestion_timestamp": "2026-06-29T10:00:00", "raw_payload": "invalid-json"},
        ]
        df = pd.DataFrame(rows)
        result = transform_bronze_coins_to_silver(df)
        assert len(result) == 1


class TestTransformBronzeTrendingToSilver:
    def test_multiple_snapshots(self):
        raw1 = build_trending_json([("coin-a", 10, 0.01)])
        raw2 = build_trending_json([("coin-b", 20, 0.005)])
        df = pd.DataFrame([
            {"ingestion_timestamp": "2026-06-29T10:00:00", "raw_payload": raw1},
            {"ingestion_timestamp": "2026-06-29T11:00:00", "raw_payload": raw2},
        ])
        result = transform_bronze_trending_to_silver(df)
        assert len(result) == 2


class TestEnrichTrendingWithBtcPrices:
    def test_basic_enrichment(self):
        df_trending = pd.DataFrame([
            {"coin_id": "coin-a", "name": "Coin A", "symbol": "COA",
             "trending_rank": 1, "market_cap_rank": 10,
             "price_btc": 0.01, "extracted_at": pd.Timestamp("2026-06-29T10:00:00")},
        ])
        df_coins = pd.DataFrame([
            {"coin_id": "bitcoin", "price_usd": 50000.0,
             "extracted_at": pd.Timestamp("2026-06-29T10:00:00")},
        ])
        result = enrich_trending_with_btc_prices(df_trending, df_coins)
        assert len(result) == 1
        assert result.iloc[0]["price_usd_estimated"] == pytest.approx(500.0)
        assert result.iloc[0]["btc_price_usd"] == 50000.0

    def test_missing_btc_raises(self):
        df_trending = pd.DataFrame([
            {"coin_id": "coin-a", "name": "Coin A", "symbol": "COA",
             "trending_rank": 1, "market_cap_rank": 10,
             "price_btc": 0.01, "extracted_at": pd.Timestamp("2026-06-29T10:00:00")},
        ])
        df_coins = pd.DataFrame([
            {"coin_id": "ethereum", "price_usd": 3000.0,
             "extracted_at": pd.Timestamp("2026-06-29T10:00:00")},
        ])
        with pytest.raises(ValueError, match="Bitcoin"):
            enrich_trending_with_btc_prices(df_trending, df_coins)
