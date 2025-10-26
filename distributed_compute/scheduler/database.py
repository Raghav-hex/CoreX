# scheduler/database.py (Complete for Preemption History feature)

import time
import logging
from sqlalchemy import create_engine, Column, String, Text, Float, Integer, ForeignKey, ARRAY
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError

# --- Basic Setup ---
logger = logging.getLogger(__name__)
DATABASE_URL = "postgresql+psycopg://admin:secret@postgres/distributed_compute"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Table Definitions ---
class Job(Base):
    __tablename__ = "jobs"
    job_id = Column(String, primary_key=True, index=True)
    image = Column(String)
    command = Column(String)
    status = Column(String, default="queued")
    result = Column(Text, nullable=True)
    assigned_agent_id = Column(String, ForeignKey("agents.agent_id"), nullable=True)
    # A list to store IDs of agents that preempted this job.
    preempted_by_agents = Column(ARRAY(String), default=[])

class Agent(Base):
    __tablename__ = "agents"
    agent_id = Column(String, primary_key=True, index=True)
    owner = Column(String, default="default_user")
    cpu_cores = Column(Integer)
    total_credits_earned = Column(Float, default=0.0)

class CreditTransaction(Base):
    __tablename__ = "credit_transactions"
    tx_id = Column(Integer, primary_key=True, autoincrement=True)
    agent_id = Column(String, ForeignKey("agents.agent_id"))
    job_id = Column(String, ForeignKey("jobs.job_id"))
    credits_awarded = Column(Float)

# --- Database Utility Functions ---
def wait_for_db():
    retries = 10
    while retries > 0:
        try:
            with engine.connect():
                logger.info("Database connection successful.")
                return
        except OperationalError:
            logger.warning("Database not ready yet, waiting...")
            retries -= 1
            time.sleep(3)
    
    logger.error("Could not connect to the database after several retries.")
    raise Exception("Database connection failed")

def create_db_and_tables():
    logger.info("Waiting for database to be ready...")
    wait_for_db()
    
    logger.info("Creating database tables if they don't exist...")
    Base.metadata.create_all(bind=engine)
    logger.info("Tables created successfully.")