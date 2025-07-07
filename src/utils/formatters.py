from datetime import datetime
from typing import Union, Optional
import locale

# Set locale for currency formatting
locale.setlocale(locale.LC_ALL, '')

def format_currency(value: Union[float, int], currency: str = 'EGP') -> str:
    """
    Format a number as currency.
    
    Args:
        value: The number to format
        currency: Currency code (default: EGP)
    
    Returns:
        Formatted currency string
    """
    try:
        if abs(value) >= 1_000_000:
            return f"{value/1_000_000:.2f}M {currency}"
        elif abs(value) >= 1_000:
            return f"{value/1_000:.1f}K {currency}"
        else:
            return f"{value:.2f} {currency}"
    except (TypeError, ValueError):
        return f"0.00 {currency}"

def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format a number as percentage.
    
    Args:
        value: The number to format (0.1 = 10%)
        decimals: Number of decimal places
    
    Returns:
        Formatted percentage string
    """
    try:
        return f"{value*100:.{decimals}f}%"
    except (TypeError, ValueError):
        return "0.00%"

def format_date_time(dt: Optional[Union[datetime, int, float]] = None, 
                    format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format datetime object or timestamp.
    
    Args:
        dt: datetime object or timestamp (default: current time)
        format_str: datetime format string
    
    Returns:
        Formatted datetime string
    """
    try:
        if dt is None:
            dt = datetime.now()
        elif isinstance(dt, (int, float)):
            dt = datetime.fromtimestamp(dt/1000 if dt > 1e10 else dt)
        
        return dt.strftime(format_str)
    except (TypeError, ValueError):
        return datetime.now().strftime(format_str)

def format_large_number(value: Union[float, int]) -> str:
    """
    Format large numbers with K/M/B suffixes.
    
    Args:
        value: The number to format
    
    Returns:
        Formatted number string
    """
    try:
        if abs(value) >= 1_000_000_000:
            return f"{value/1_000_000_000:.2f}B"
        elif abs(value) >= 1_000_000:
            return f"{value/1_000_000:.2f}M"
        elif abs(value) >= 1_000:
            return f"{value/1_000:.1f}K"
        else:
            return f"{value:.0f}"
    except (TypeError, ValueError):
        return "0"

def format_signal_strength(value: float) -> str:
    """
    Format signal strength with appropriate emoji.
    
    Args:
        value: Signal strength (0-1)
    
    Returns:
        Formatted signal strength string with emoji
    """
    try:
        if value >= 0.8:
            return f"🔥 Strong ({format_percentage(value)})"
        elif value >= 0.6:
            return f"✨ Good ({format_percentage(value)})"
        elif value >= 0.4:
            return f"⚡ Moderate ({format_percentage(value)})"
        else:
            return f"💫 Weak ({format_percentage(value)})"
    except (TypeError, ValueError):
        return "❓ Unknown"