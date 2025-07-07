import aiohttp
import logging
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from ..database.models import Subscriber
from datetime import datetime

class TelegramService:
    def __init__(self, config: Dict, db: Session):
        self.token = config['telegram_bot_token']
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.db = db
        self.logger = logging.getLogger(__name__)

    async def send_message(self, chat_id: str, message: str, parse_mode: str = 'Markdown') -> bool:
        url = f"{self.base_url}/sendMessage"
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': parse_mode
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    return response.status == 200
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message: {e}")
            return False

    async def broadcast_message(self, message: str) -> Dict[str, bool]:
        results = {}
        subscribers = self.db.query(Subscriber).all()
        
        for subscriber in subscribers:
            success = await self.send_message(subscriber.chat_id, message)
            results[subscriber.chat_id] = success
        
        return results

    async def send_alert(self, alert_type: str, content: str, priority: str = 'normal') -> None:
        emoji_map = {
            'signal': '🚀',
            'error': '⚠️',
            'warning': '⚡',
            'info': 'ℹ️'
        }
        
        emoji = emoji_map.get(alert_type, 'ℹ️')
        formatted_message = (
            f"{emoji} *{alert_type.upper()}*\n\n"
            f"{content}\n\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        if priority == 'high':
            formatted_message = "❗️" + formatted_message
        
        await self.broadcast_message(formatted_message)

    async def process_updates(self) -> None:
        url = f"{self.base_url}/getUpdates"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        await self._handle_updates(data.get('result', []))
        except Exception as e:
            self.logger.error(f"Failed to process Telegram updates: {e}")

    async def _handle_updates(self, updates: List[Dict]) -> None:
        for update in updates:
            message = update.get('message', {})
            if message.get('text') == '/start':
                chat_id = str(message.get('chat', {}).get('id'))
                if chat_id:
                    self._save_subscriber(chat_id)

    def _save_subscriber(self, chat_id: str) -> None:
        try:
            subscriber = Subscriber(chat_id=chat_id)
            self.db.merge(subscriber)
            self.db.commit()
            self.logger.info(f"New subscriber saved: {chat_id}")
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Failed to save subscriber: {e}")