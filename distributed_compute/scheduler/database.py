# scheduler/database.py

from sqlalchemy import create_engine, Column, String, Text, Float, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "postgresql+psycopg://admin:secret@postgres/distributed_compute"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Define the 'jobs' Table (no changes) ---
class Job(Base):
    __tablename__ = "jobs"
    job_id = Column(String, primary_key=True, index=True)
    image = Column(String)
    command = Column(String)
    status = Column(String, default="queued")
    result = Column(Text, nullable=True)
    # [NEW] Add a column to track which agent did the job
    assigned_agent_id = Column(String, ForeignKey("agents.agent_id"), nullable=True)

# --- [NEW] Define the 'agents' Table ---
class Agent(Base):
    __tablename__ = "agents"
    agent_id = Column(String, primary_key=True, index=True)
    # In a real system, this would link to a 'users' table
    owner = Column(String, default="default_user") 
    cpu_cores = Column(Integer)
    total_credits_earned = Column(Float, default=0.0)

# --- [NEW] Define the 'credit_transactions' Table ---
class CreditTransaction(Base):
    __tablename__ = "credit_transactions"
    tx_id = Column(Integer, primary_key=True, autoincrement=True)
    agent_id = Column(String, ForeignKey("agents.agent_id"))
    job_id = Column(String, ForeignKey("jobs.job_id"))
    credits_awarded = Column(Float)

# --- Create all tables ---
def create_db_and_tables():
    Base.metadata.create_all(bind=engine)