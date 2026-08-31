import os
from sqlmodel import SQLModel, create_engine, Session

DATABASE_URL = "sqlite:///./data/great_sage.db"

# Ensure data directory exists
os.makedirs("./data", exist_ok=True)
os.makedirs("./data/jobs", exist_ok=True)

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
