import sys
import os
import asyncio
import logging

# Add the project root directory (v4) to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.main import TradingApp
from src.utils.config import load_config
from src.database.postgres import engine, Base # Import engine and Base
from src.database.models import SignalHistory, Subscriber # Import models to ensure they are registered with Base

if __name__ == "__main__":
    config = load_config()
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s'
    )
    
    # Create database tables if they don't exist
    logging.info("Creating database tables if they don't exist...")
    Base.metadata.create_all(bind=engine)
    logging.info("Database tables checked/created.")

    app = TradingApp(config)
    asyncio.run(app.run())
        
    