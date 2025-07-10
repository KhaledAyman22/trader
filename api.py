import sys
import os
import asyncio
from typing import Dict, List
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.sql import text
import uvicorn

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.postgres import get_db

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Trading Recommendations API",
    description="Provides real-time trade recommendations from the database.",
    version="1.0.0"
)

# Allow Cross-Origin Resource Sharing (CORS) for your Blazor app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5232", "https://localhost:7287"], # Add your Blazor app's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/recommendations", summary="Fetch Today's Trading Recommendations")
async def get_recommendations() -> List[Dict]:
    """
    Fetches all of today's trading recommendations from the database
    with a signal strength of 0.7 or higher.
    """
    db = next(get_db())
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
            AND signal_strength >= 0.7
        ORDER BY
            timestamp DESC;
    """)
    try:
        results = db.execute(query).fetchall()
        # Convert SQLAlchemy Row objects to dictionaries
        return [dict(row._mapping) for row in results]
    finally:
        db.close()

if __name__ == "__main__":
    print("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
