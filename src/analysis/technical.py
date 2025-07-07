import pandas as pd
import ta
import numpy as np
from typing import Dict, Optional, List

def calculate_technical_indicators(history: List[Dict]) -> Optional[Dict]:
    """
    Calculate comprehensive technical indicators from historical data.
    
    Args:
        history: List of dictionaries containing OHLCV data
        
    Returns:
        Dictionary of technical indicators or None if insufficient data
    """
    if not history or len(history) < 26:
        return None
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(history)
        
        # Ensure required columns exist and handle different naming conventions
        required_mapping = {
            'close': ['close', 'Close', 'CLOSE'],
            'high': ['high', 'High', 'HIGH'],
            'low': ['low', 'Low', 'LOW'],
            'volume': ['volume', 'Volume', 'VOLUME', 'vol']
        }
        
        # Map columns to standard names
        for standard_name, possible_names in required_mapping.items():
            found = False
            for possible_name in possible_names:
                if possible_name in df.columns:
                    if standard_name != possible_name:
                        df[standard_name] = df[possible_name]
                    found = True
                    break
            if not found:
                print(f"Missing required column: {standard_name}")
                return None
        
        # Convert to numeric and handle missing values
        numeric_columns = ['close', 'high', 'low', 'volume']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove rows with NaN values
        df = df.dropna(subset=numeric_columns)
        
        if len(df) < 26:
            return None
        
        # Sort by date if available
        if 'time' in df.columns:
            date_col = 'time'
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.sort_values(date_col)
        
        # Calculate technical indicators with error handling
        indicators = {}
        
        # Momentum Indicators
        try:
            indicators['rsi'] = ta.momentum.rsi(df['close'], window=14).iloc[-1]
        except:
            indicators['rsi'] = None
            
        try:
            indicators['rsi_fast'] = ta.momentum.rsi(df['close'], window=7).iloc[-1]
        except:
            indicators['rsi_fast'] = None
            
        try:
            indicators['stoch_k'] = ta.momentum.stoch(df['high'], df['low'], df['close']).iloc[-1]
        except:
            indicators['stoch_k'] = None
            
        try:
            indicators['stoch_d'] = ta.momentum.stoch_signal(df['high'], df['low'], df['close']).iloc[-1]
        except:
            indicators['stoch_d'] = None
        
        # Trend Indicators
        try:
            indicators['macd'] = ta.trend.macd(df['close']).iloc[-1]
        except:
            indicators['macd'] = None
            
        try:
            indicators['macd_signal'] = ta.trend.macd_signal(df['close']).iloc[-1]
        except:
            indicators['macd_signal'] = None
            
        try:
            indicators['macd_hist'] = ta.trend.macd_diff(df['close']).iloc[-1]
        except:
            indicators['macd_hist'] = None
            
        try:
            indicators['adx'] = ta.trend.adx(df['high'], df['low'], df['close']).iloc[-1]
        except:
            indicators['adx'] = None
        
        # Volatility Indicators
        try:
            indicators['bb_upper'] = ta.volatility.bollinger_hband(df['close']).iloc[-1]
        except:
            indicators['bb_upper'] = None
            
        try:
            indicators['bb_lower'] = ta.volatility.bollinger_lband(df['close']).iloc[-1]
        except:
            indicators['bb_lower'] = None
            
        try:
            indicators['bb_mid'] = ta.volatility.bollinger_mavg(df['close']).iloc[-1]
        except:
            indicators['bb_mid'] = None
            
        try:
            indicators['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close']).iloc[-1]
        except:
            indicators['atr'] = None
        
        # Volume Indicators
        try:
            indicators['mfi'] = ta.volume.money_flow_index(df['high'], df['low'], df['close'], df['volume']).iloc[-1]
        except:
            indicators['mfi'] = None
            
        try:
            indicators['vwap'] = ta.volume.volume_weighted_average_price(df['high'], df['low'], df['close'], df['volume']).iloc[-1]
        except:
            indicators['vwap'] = None
        
        # Additional useful indicators
        try:
            # Simple Moving Averages
            indicators['sma_20'] = df['close'].rolling(window=20).mean().iloc[-1]
            indicators['sma_50'] = df['close'].rolling(window=50).mean().iloc[-1] if len(df) >= 50 else None
        except:
            indicators['sma_20'] = None
            indicators['sma_50'] = None
            
        try:
            # Exponential Moving Averages
            indicators['ema_12'] = df['close'].ewm(span=12).mean().iloc[-1]
            indicators['ema_26'] = df['close'].ewm(span=26).mean().iloc[-1]
        except:
            indicators['ema_12'] = None
            indicators['ema_26'] = None
        
        # Add current price data
        indicators['close'] = df['close'].iloc[-1]
        indicators['high'] = df['high'].iloc[-1]
        indicators['low'] = df['low'].iloc[-1]
        indicators['volume'] = df['volume'].iloc[-1]
        
        # Clean up NaN values
        cleaned_indicators = {}
        for key, value in indicators.items():
            if pd.isna(value):
                cleaned_indicators[key] = None
            else:
                cleaned_indicators[key] = float(value) if isinstance(value, (int, float, np.number)) else value
        
        return cleaned_indicators
    
    except Exception as e:
        print(f"Error calculating technical indicators: {e}")
        return None

def validate_ohlcv_data(df: pd.DataFrame) -> bool:
    """
    Validate OHLCV data for basic consistency.
    
    Args:
        df: DataFrame with OHLCV data
        
    Returns:
        True if data is valid, False otherwise
    """
    try:
        # Check if high >= low
        if (df['high'] < df['low']).any():
            return False
            
        # Check if close is within high/low range
        if ((df['close'] > df['high']) | (df['close'] < df['low'])).any():
            return False
            
        # Check for negative values
        if (df[['high', 'low', 'close', 'volume']] < 0).any().any():
            return False
            
        return True
    except:
        return False