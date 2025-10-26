# scheduler/database.py

from sqlalchemy import create_engine, Column, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- Database Connection URL ---
# This tells SQLAlchemy how to connect to our Postgres container.
# The format is: postgresql://<user>:<password>@<host>/<database_name>
DATABASE_URL = "postgresql+psycopg://admin:secret@postgres/distributed_compute"
# --- SQLAlchemy Setup ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Define the 'jobs' Table ---
# This class defines the structure of our 'jobs' table in the database.
class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(String, primary_key=True, index=True)
    image = Column(String)
    command = Column(String)
    status = Column(String, default="queued")
    result = Column(Text, nullable=True)

# --- Create the Table ---
# This function will create the 'jobs' table in the database if it doesn't exist.
def create_db_and_tables():
    Base.metadata.create_all(bind=engine)