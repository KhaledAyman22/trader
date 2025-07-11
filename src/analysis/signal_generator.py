from typing import Dict, Optional, List, Tuple
import logging
import pandas as pd

class SignalGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.strategy_config = config['strategy']
        self.tech_thresholds = self.strategy_config.get('technical_thresholds', {})
        self.flow_thresholds = self.strategy_config.get('trade_flow_thresholds', {})

    async def analyze_stock(self, stock: Dict, historical_data: List[Dict],
                          market_depth: Dict, trades_data: Dict) -> Optional[Dict]:
        try:
            from .technical import calculate_technical_indicators
            technical_indicators = calculate_technical_indicators(historical_data)

            if not technical_indicators:
                return self._create_empty_analysis(stock.get('symbol'))

            from .trade_flow import analyze_trade_flow
            trade_flow = analyze_trade_flow(trades_data, self.config)

            return self._generate_signal(stock, technical_indicators, trade_flow, market_depth, historical_data)
        except Exception as e:
            print(f"Error analyzing stock {stock.get('symbol')}: {e}")
            return None

    def _generate_signal(self, stock: Dict, technical: Dict, trade_flow: Dict, market_depth: Dict, historical_data: List[Dict]) -> Dict:
        self.stock_symbol_for_logging = stock.get('symbol', 'UNKNOWN')
        
        scores = self._calculate_component_scores(technical, trade_flow, market_depth)
        signal_type = self._determine_signal_type(scores)
        signal_strength = self._calculate_signal_strength(scores)

        signal = {
            'symbol': stock.get('symbol'),
            'timestamp': stock.get('last_update_time'),
            'price': stock.get('last_trade_price'),
            'signal_type': signal_type,
            'signal_strength': round(signal_strength, 4), # Explicitly round for consistent storage
            'component_scores': scores,
            'technical_indicators': technical,
            'trade_flow_metrics': trade_flow,
            'risk_metrics': self._calculate_risk_metrics(stock, technical, market_depth, historical_data),
            'stock_details': stock
        }
        return signal
        
    def _calculate_component_scores(self, technical: Dict, trade_flow: Dict, market_depth: Dict) -> Dict:
        return {
            'technical': self._score_technical_indicators(technical),
            'trade_flow': self._score_trade_flow(trade_flow),
            'market_depth': self._score_market_depth(market_depth, technical.get('close', 0))
        }

    def _score_technical_indicators(self, indicators: Dict) -> int:
        def get_value(key, default=0):
            val = indicators.get(key)
            return default if val is None else val

        macd = get_value('macd')
        macd_signal = get_value('macd_signal')
        macd_hist = get_value('macd_hist')
        rsi = get_value('rsi', 50)
        stoch_k = get_value('stoch_k')
        stoch_d = get_value('stoch_d')
        adx = get_value('adx')
        close = get_value('close')
        bb_mid = get_value('bb_mid')
        sma_20 = get_value('sma_20')
        
        rsi_oversold = self.tech_thresholds.get('rsi_oversold', 30)
        rsi_overbought = self.tech_thresholds.get('rsi_overbought', 70)
        adx_trend_threshold = self.tech_thresholds.get('adx_trend_threshold', 25)
        macd_signal_threshold = self.tech_thresholds.get('macd_signal_threshold', 0)

        conditions = [
            macd > macd_signal,
            macd_hist > macd_signal_threshold,
            rsi_oversold < rsi < rsi_overbought,
            stoch_k > stoch_d,
            adx > adx_trend_threshold,
            close > bb_mid,
            close > sma_20
        ]
        
        return sum(conditions)

    def _score_trade_flow(self, trade_flow: Dict) -> int:
        conditions = [
            trade_flow.get('buy_pressure', 0) > self.flow_thresholds.get('strong_buy_pressure', 0.65),
            trade_flow.get('institutional_ratio', 0) > self.flow_thresholds.get('high_institutional_ratio', 0.60)
        ]
        return sum(conditions)

    def _score_market_depth(self, depth: Dict, current_price: float) -> int:
        bids_vol = depth.get('bids_vol', 0)
        asks_vol = depth.get('asks_vol', 0)
        spread = depth.get('spread', float('inf'))

        if asks_vol == 0 or current_price == 0:
            return 0

        conditions = [
            bids_vol > asks_vol,
            (spread / current_price) < self.strategy_config.get('max_spread_pct', 0.02)
        ]
        return sum(conditions)

    def _determine_signal_type(self, scores: Dict) -> str:
        min_tech_conditions = self.strategy_config.get('min_tech_conditions', 5)
        min_flow_conditions = self.strategy_config.get('min_flow_conditions', 2)
        min_depth_conditions = self.strategy_config.get('min_depth_conditions', 1)

        is_buy = (
            scores.get('technical', 0) >= min_tech_conditions and
            scores.get('trade_flow', 0) >= min_flow_conditions and
            scores.get('market_depth', 0) >= min_depth_conditions
        )

        if self.strategy_config.get('debug_mode', False):
            logging.info(
                f"[DEBUG] Symbol: {self.stock_symbol_for_logging} | "
                f"Tech: {scores.get('technical', 0)}/{min_tech_conditions} | "
                f"Flow: {scores.get('trade_flow', 0)}/{min_flow_conditions} | "
                f"Depth: {scores.get('market_depth', 0)}/{min_depth_conditions} | "
                f"Signal: {'BUY' if is_buy else 'NEUTRAL'}"
            )

        if is_buy:
            total_conditions = sum(scores.values())
            strong_buy_threshold = min_tech_conditions + min_flow_conditions + min_depth_conditions + 1
            if total_conditions >= strong_buy_threshold:
                return 'STRONG_BUY'
            return 'BUY'

        return 'NEUTRAL'
   
    def _calculate_signal_strength(self, scores: Dict) -> float:
        max_possible_score = 7 + 2 + 2
        current_score = sum(scores.values())
        return current_score / max_possible_score if max_possible_score > 0 else 0

    def _calculate_risk_metrics(self, stock: Dict, technical: Dict,
                              market_depth: Dict, historical_data: List[Dict]) -> Dict:
        price = stock.get('last_trade_price', 0)
        atr = technical.get('atr', 0)
        bb_mid = technical.get('bb_mid', 0)

        stop_loss_atr_multiplier = self.strategy_config.get('stop_loss_atr_multiplier', 2.0)
        take_profit_multiplier = self.strategy_config.get('take_profit_atr_multiplier', 5.0)
        lookback_period = self.strategy_config.get('structural_stop_lookback', 5)

        volatility_stop = price - (atr * stop_loss_atr_multiplier) if atr > 0 and price > 0 else 0
        
        structural_stop = 0
        if historical_data and len(historical_data) >= lookback_period:
            try:
                recent_lows = [p['low'] for p in historical_data[-lookback_period:]]
                lowest_low = min(recent_lows)
                structural_stop = lowest_low * 0.995
            except (KeyError, IndexError):
                structural_stop = 0

        if structural_stop > 0 and volatility_stop > 0:
            stop_loss = min(volatility_stop, structural_stop)
        elif volatility_stop > 0:
            stop_loss = volatility_stop
        else:
            stop_loss = price * 0.98

        take_profit = price + (atr * take_profit_multiplier) if atr > 0 and price > 0 else 0

        adjusted_buy_price = price
        if price > 0 and bb_mid > 0 and price > bb_mid:
            adjusted_buy_price = (price + bb_mid) / 2

        # Round financial values to a sensible number of decimal places for cleaner data
        return {
            'volatility': round(atr, 4) if atr else 0,
            'liquidity_risk': self._calculate_liquidity_risk(market_depth),
            'position_size': self._calculate_position_size(stock),
            'stop_loss': round(stop_loss, 3),
            'take_profit': round(take_profit, 3),
            'adjusted_buy_price': round(adjusted_buy_price, 3)
        }
   
    def _calculate_liquidity_risk(self, depth: Dict) -> float:
        bids_vol = depth.get('bids_vol', 0)
        asks_vol = depth.get('asks_vol', 0)
        total_volume = bids_vol + asks_vol
        
        min_volume = self.strategy_config.get('min_daily_volume', 100000)
        return max(0, 1 - min(total_volume / min_volume, 1)) if min_volume > 0 else 1

    def _calculate_position_size(self, stock: Dict) -> float:
        price = stock.get('last_trade_price', 0)
        if not price or price <= 0:
            return 0
            
        feed_data = stock.get('feed_data', {})
        daily_volume = feed_data.get('average_daily_volume', 0)
        if daily_volume <= 0:
            return 0
        
        max_position = min(
            self.strategy_config.get('max_position_size', 100000),
            daily_volume * price * 0.1
        )
        return max(max_position, 0)

    def _calculate_stop_loss(self, technical: Dict) -> float:
        atr = technical.get('atr', 0)
        multiplier = self.strategy_config.get('stop_loss_atr_multiplier', 2)
        return atr * multiplier if atr else 0

    def _create_empty_analysis(self, symbol: Optional[str]) -> Dict:
        return {
            'symbol': symbol, 'timestamp': None, 'price': 0, 'signal_type': 'NEUTRAL',
            'signal_strength': 0, 'component_scores': {}, 'technical_indicators': {},
            'trade_flow_metrics': {}, 'risk_metrics': {},
        }
