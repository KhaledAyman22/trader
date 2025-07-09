import sys
import os
import asyncio
import logging
from typing import Dict, List
from datetime import datetime, time

import aiohttp
from sqlalchemy.sql import text

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.postgres import get_db
from src.services.market_data import MarketDataService
from src.utils.config import load_config
from src.utils.rate_limiter import RateLimiter

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
config = load_config()


class PerformanceAnalyzer:
    """
    Analyzes the performance of trading recommendations for the current day.
    """

    def __init__(self):
        self.db = next(get_db())
        self.rate_limiter = RateLimiter(
            max_concurrent=config.get('max_concurrent', 5),
            requests_per_minute=config['api_settings'].get('rate_limit_requests_per_minute', 60)
        )
        self.market_data_service = MarketDataService(config, self.rate_limiter)
        self.logger = logging.getLogger(__name__)

    async def get_todays_recommendations(self, min_strength: float = 0.7) -> List[Dict]:
        """Fetches all recommendations from the database for the current day."""
        self.logger.info(f"Querying database for today's recommendations with minimum strength {min_strength}...")
        query = text("""
            SELECT
                id,
                symbol,
                timestamp,
                price AS recommended_price,
                signal_type,
                signal_strength,
                target,
                buy_price,
                stop_loss
            FROM
                signal_history
            WHERE
                timestamp >= date_trunc('day', NOW())
                AND signal_type IN ('BUY', 'STRONG_BUY')
                AND signal_strength >= :min_strength
            ORDER BY
                timestamp DESC;
        """)
        try:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, lambda: self.db.execute(query, {'min_strength': min_strength}).fetchall())
            
            recommendations = [
                {
                    "id": row[0], "symbol": row[1], "timestamp": row[2],
                    "recommended_price": row[3], "signal_type": row[4],
                    "signal_strength": row[5], "target": row[6],
                    "buy_price": row[7], "stop_loss": row[8],
                }
                for row in results
            ]
            self.logger.info(f"Found {len(recommendations)} recommendations in the database.")
            return recommendations
        except Exception as e:
            self.logger.error(f"Failed to fetch today's recommendations: {e}")
            return []

    async def get_intraday_high_low_after_time(self, session: aiohttp.ClientSession, asset_id: str, rec_timestamp: datetime) -> Dict:
        """
        Fetches the high and low prices for a stock for the current day, but only
        considers the period *after* the recommendation was made.
        """
        now = datetime.now()
        # Convert recommendation timestamp to milliseconds for the API
        from_timestamp = int(rec_timestamp.timestamp() * 1000)
        to_timestamp = int(now.timestamp() * 1000)

        url = f"https://prod.thndr.app/assets-service/charts/advanced?asset_id={asset_id}&resolution=five_minutes&from_timestamp={from_timestamp}&to_timestamp={to_timestamp}"
        data = await self.market_data_service.fetch_json(session, url, config['api_settings']['headers'])

        if data and data.get("points"):
            points = data["points"]
            
            # Find the highest high and lowest low from the fetched points
            post_rec_high = max(p['high'] for p in points)
            post_rec_low = min(p['low'] for p in points)
            
            return {"high": post_rec_high, "low": post_rec_low}
            
        return {"high": None, "low": None}
        
    async def analyze(self):
        """Main function to run the performance analysis."""
        self.logger.info("Starting performance analysis for today's recommendations...")
        recommendations = await self.get_todays_recommendations(min_strength=0.7)

        if not recommendations:
            self.logger.info("No recommendations found for today. Exiting.")
            return

        # If recommendations are found, print them for debugging
        print("\n--- Fetched Recommendations ---")
        for rec in recommendations:
            print(rec)
        print("-----------------------------\n")

        total_wins = 0
        total_losses = 0
        total_gain = 0.0
        total_loss = 0.0
        
        async with aiohttp.ClientSession() as session:
            # First, get all asset_ids to fetch market data efficiently
            self.logger.info("Fetching market asset data...")
            all_assets = await self.market_data_service.fetch_market_data(session, config['api_settings']['headers'])
            symbol_to_asset_id = {asset['symbol']: asset['asset_id'] for asset in all_assets if 'symbol' in asset and 'asset_id' in asset}
            self.logger.info(f"Found {len(symbol_to_asset_id)} assets.")


            for rec in recommendations:
                symbol = rec['symbol']
                asset_id = symbol_to_asset_id.get(symbol)

                if not asset_id:
                    self.logger.warning(f"Could not find asset_id for {symbol}. Skipping.")
                    continue

                # Fetch the high/low prices that occurred *after* the recommendation time
                day_prices = await self.get_intraday_high_low_after_time(session, asset_id, rec['timestamp'])
                day_high = day_prices.get('high')
                day_low = day_prices.get('low')

                if day_high is None or day_low is None:
                    self.logger.warning(f"Could not fetch post-recommendation high/low for {symbol}. Skipping.")
                    continue

                target_price = rec.get('target')
                stop_loss_price = rec.get('stop_loss')
                buy_price = rec.get('buy_price')
                status = "PENDING"
                pnl = 0.0

                # Check for WIN: Target must be hit after the recommendation
                if target_price and day_high >= target_price:
                    status = "WIN"
                    total_wins += 1
                    pnl = target_price - buy_price
                    total_gain += pnl
                # Check for LOSS: Stop-loss must be hit after the recommendation
                elif stop_loss_price and day_low <= stop_loss_price:
                    status = "LOSS"
                    total_losses += 1
                    pnl = buy_price - stop_loss_price
                    total_loss += pnl
                
                print("-" * 50)
                print(f"Symbol: {symbol} ({rec['signal_type']})")
                print(f"  - Recommendation Time: {rec['timestamp'].strftime('%H:%M:%S')}")
                print(f"  - Entry: {buy_price:.2f} | Target: {target_price:.2f} | Stop: {stop_loss_price:.2f}")
                print(f"  - Post-Rec Range: Low={day_low:.2f}, High={day_high:.2f}")
                print(f"  - Status: {status}")
                if status != "PENDING":
                    print(f"  - P/L: {pnl:.2f} EGP")
        
        # --- Final Summary ---
        net_profit = total_gain - total_loss
        print("\n" + "="*50)
        print("           TODAY'S PERFORMANCE SUMMARY")
        print("="*50)
        print(f"Total Recommendations Analyzed: {len(recommendations)}")
        print(f"Wins: {total_wins}")
        print(f"Losses: {total_losses}")
        print(f"Total Gains: {total_gain:.2f} EGP")
        print(f"Total Losses: {total_loss:.2f} EGP")
        print("-" * 50)
        print(f"NET PROFIT/LOSS: {net_profit:.2f} EGP")
        print("="*50)


if __name__ == "__main__":
    try:
        analyzer = PerformanceAnalyzer()
        asyncio.run(analyzer.analyze())
    except Exception as e:
        logging.critical(f"A critical error occurred: {e}")
        sys.exit(1)
