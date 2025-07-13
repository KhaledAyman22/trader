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
    Analyzes the performance of trading recommendations for the current day
    with more accurate, sequential trade logic.
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
                timestamp >= date_trunc('day', NOW()) AND timestamp < '2025-07-13 14:30:00'
                AND signal_type IN ('BUY', 'STRONG_BUY')
            ORDER BY
                timestamp ASC; -- Order chronologically to process trades in order
        """)
        try:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, lambda: self.db.execute(query, {'min_strength': min_strength}).fetchall())
            
            recommendations = [dict(row._mapping) for row in results]
            self.logger.info(f"Found {len(recommendations)} recommendations in the database.")
            return recommendations
        except Exception as e:
            self.logger.error(f"Failed to fetch today's recommendations: {e}")
            return []

    async def get_intraday_data_after_time(self, session: aiohttp.ClientSession, asset_id: str, rec_timestamp: datetime) -> List[Dict]:
        """
        Fetches the 5-minute candle data for the rest of the day after a recommendation.
        """
        now = datetime.now()
        from_timestamp = int(rec_timestamp.timestamp() * 1000)
        to_timestamp = int(now.timestamp() * 1000)

        url = f"https://prod.thndr.app/assets-service/charts/advanced?asset_id={asset_id}&resolution=five_minutes&from_timestamp={from_timestamp}&to_timestamp={to_timestamp}"
        data = await self.market_data_service.fetch_json(session, url, config['api_settings']['headers'])

        return data.get("points", []) if data else []
        
    async def analyze(self):
        """Main function to run the performance analysis."""
        self.logger.info("Starting performance analysis for today's recommendations...")
        recommendations = await self.get_todays_recommendations(min_strength=0.7)

        if not recommendations:
            self.logger.info("No recommendations found for today. Exiting.")
            return

        total_wins = 0
        total_losses = 0
        total_gain = 0.0
        total_loss = 0.0
        
        async with aiohttp.ClientSession() as session:
            all_assets = await self.market_data_service.fetch_market_data(session, config['api_settings']['headers'])
            symbol_to_asset_id = {asset['symbol']: asset['asset_id'] for asset in all_assets if 'symbol' in asset and 'asset_id' in asset}

            for rec in recommendations:
                symbol = rec['symbol']
                asset_id = symbol_to_asset_id.get(symbol)
                
                if not asset_id:
                    self.logger.warning(f"Could not find asset_id for {symbol}. Skipping.")
                    continue

                intraday_candles = await self.get_intraday_data_after_time(session, asset_id, rec['timestamp'])
                
                if not intraday_candles:
                    self.logger.warning(f"Could not fetch post-recommendation data for {symbol}. Skipping.")
                    continue

                target_price = rec.get('target')
                stop_loss_price = rec.get('stop_loss')
                buy_price = rec.get('buy_price')
                
                status = "PENDING"
                pnl = 0.0
                trade_entered = False

                for candle in intraday_candles:
                    candle_low = candle.get('low', 0)
                    candle_high = candle.get('high', 0)

                    # Step 1: Check if the trade has been entered
                    if not trade_entered and buy_price and candle_low <= buy_price <= candle_high:
                        trade_entered = True
                        status = "ENTERED"
                        # Once entered, we check for win/loss in the *same* candle
                    
                    # Step 2: If trade is live, check for win or loss
                    if trade_entered:
                        # Check for WIN: Did the price hit the target?
                        if target_price and candle_high >= target_price:
                            status = "WIN"
                            pnl = target_price - buy_price
                            total_wins += 1
                            total_gain += pnl
                            break # Exit loop once trade is resolved

                        # Check for LOSS: Did the price hit the stop-loss?
                        if stop_loss_price and candle_low <= stop_loss_price:
                            status = "LOSS"
                            pnl = buy_price - stop_loss_price # Loss is positive for calculation
                            total_losses += 1
                            total_loss += pnl
                            break # Exit loop once trade is resolved
                
                # print("-" * 50)
                # print(f"Symbol: {symbol} ({rec['signal_type']})")
                # print(f"  - Recommendation Time: {rec['timestamp'].strftime('%H:%M:%S')}")
                # print(f"  - Entry: {buy_price:.2f} | Target: {target_price:.2f} | Stop: {stop_loss_price:.2f}")
                # print(f"  - Status: {status}")
                # if status in ["WIN", "LOSS"]:
                #     print(f"  - P/L: {pnl:.2f} EGP")
        
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
