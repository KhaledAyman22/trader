from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from ..utils.config import load_config

config = load_config()
db_config = config['database']

password = quote_plus(db_config['password'])
POSTGRES_URL = f"postgresql://{db_config['user']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

engine = create_engine(POSTGRES_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()