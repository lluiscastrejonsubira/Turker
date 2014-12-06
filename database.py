from sqlalchemy import Column, String, ForeignKey, Boolean, Float, Text
from sqlalchemy import DateTime, Table, BigInteger, Index, PickleType
from sqlalchemy.orm import aliased, relationship
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.orm import backref
from sqlalchemy.dialects.mysql import INTEGER as Integer
import logging
import settings
from contextlib import contextmanager

logger = logging.getLogger("turker.database")

Base = declarative_base()
config = settings.get_settings()
Session = None
session = None

class Batch(Base):
    __tablename__ = "batches"
    
    id             = Column(Integer(unsigned=True), 
                            primary_key=True,
                            autoincrement=True
                           )    
    name           = Column(String(30), nullable=False)
    sandbox        = Column(Boolean, nullable=False)
    title          = Column(String(50), nullable=False)
    description    = Column(String(500), nullable=False)
    question       = Column(String(100), nullable=False)
    amount         = Column(Float, nullable=False, default=0.0)
    duration       = Column(Integer(unsigned=True),
                            nullable=False,
                            default=3600
                           )
    lifetime       = Column(Integer(unsigned=True),
                            nullable=False,
                            default=604800
                           )
    keywords       = Column(String(100), nullable=False)
    auto_approve   = Column(Integer(unsigned=True),
                            nullable=False,
                            default=604800
                           )
    max_assigs     = Column(Integer(unsigned=True),
                            nullable=False,
                            default=10
                           )
    height         = Column(Integer(unsigned=True),
                            nullable=False,
                            default=800
                           )
    number_of_images = Column(Integer(unsigned=True), 
                            default=0
                           )
    country_code   = Column(String(4))
    min_approved_percent = Column(Integer(unsigned=True))
    min_approved_amount  = Column(Integer(unsigned=True))
    
    hits           = relationship('HIT', backref='batch')
    
class Image(Base):
    __tablename__ = "images"
    
    id             = Column(Integer(unsigned=True), primary_key = True, index=True)
    path           = Column(String(1000), nullable = False)
    group          = Column(String(50))
    
class HIT(Base):
    __tablename__ = "hits"
    
    id             = Column(Integer(unsigned=True), 
                            primary_key = True,
                            autoincrement=True
                           )
    hitId          = Column(String(30), nullable=False, index=True)
    typeId         = Column(String(30), nullable=False)
    completed      = Column(Boolean, nullable=False, default=False)
    batchId        = Column(Integer(unsigned=True),
                            ForeignKey('batches.id'),
                            nullable=False
                           )
    
    assignments    = relationship('Assignment', backref='hit')

class HIT_Image(Base):
    __tablename__ = 'hit_image'
    id             = Column(Integer(unsigned=True), 
                            primary_key = True,
                            autoincrement=True
                           )
    imageId        = Column('image', Integer(unsigned=True), ForeignKey('images.id'), index=True)
    hitId          = Column('hit', Integer(unsigned=True), ForeignKey('hits.id'), nullable=False, index=True)
    
    # Indexes
    Index('images_hit', imageId, hitId)
    
class Assignment(Base):
    __tablename__ = "assignments"
    
    id             = Column(Integer(unsigned=True), 
                            primary_key = True,
                            autoincrement=True
                           )
    assignmentId   = Column(String(30), nullable = False, index=True)
    workerId         = Column(Integer(unsigned=True), ForeignKey('workers.id'), nullable=False)
    hitId            = Column(Integer(unsigned=True), ForeignKey('hits.id'), nullable=False)
    submitted      = Column(Boolean, nullable = False, default = False)
    paid           = Column(Boolean, nullable = False, default = False)
    blocked        = Column(Boolean, nullable = False, default = False)
    serve_time     = Column(DateTime)
    
    tasks          = relationship('Task', backref='assignment')

class Worker(Base):
    __tablename__ = "workers"
    
    id             = Column(Integer(unsigned=True), 
                            primary_key = True,
                            autoincrement=True
                           )
    workerId       = Column(String(30), nullable = False, index=True)
    instructionsId = Column(Integer(unsigned=True), ForeignKey('instructions.id'))
    
    assignments    = relationship('Assignment', backref='worker')
    
    # Functions
    @property
    def points(self):
        points = 0
        
        for assig in self.assignments:
            for task in assig.tasks:
                for result in task.results:
                    points += result.points
                    
        return points
    
    @property
    def results(self):
        ret = []
        for assig in self.assignments:
            for task in assig.tasks:
                for result in task.results:
                    ret.append(result)
        
        return ret
    
class Task(Base):
    __tablename__ = "tasks"
    
    id             = Column(Integer(unsigned=True), primary_key=True,
                            autoincrement=True)
    type           = Column(String(100))
    assignmentId   = Column(Integer(unsigned=True), ForeignKey('assignments.id'), index=True)
    imageId        = Column(Integer(unsigned=True), ForeignKey('images.id'), index=True)
    position       = Column(Integer(unsigned=True))
    submitted      = Column(Boolean, nullable=False, default=False)
    comment        = Column(Text)
   
    # Relations 
    results        = relationship('Result', backref='task')
                     
class Result(Base):
    __tablename__ = "results"
    
    id             = Column(Integer(unsigned=True),
                            primary_key=True,
                            autoincrement=True
                           )
    taskId         = Column(Integer(unsigned=True),
                            ForeignKey('tasks.id'),
                            autoincrement=False,
                            index=True
                           )
    number         = Column(Integer(unsigned=True),
                            nullable=False,
                            autoincrement=False
                           )
    answer         = Column(Text)
    points         = Column(Integer(unsigned=True))
    extra          = Column(Boolean, default=False)
    correct        = Column(Boolean)
    new	           = Column(Boolean) 
    
    
if not config is None:
    logger.debug("Loading database")
    db = config.get('Settings', 'db')
    engine = create_engine(db, pool_recycle=3600)
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def connect():
        """
        Provides a transactional scope around a series of operations.
        """
        session = Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def setup():
        """
        Reinstalls the database by dropping all existing tables. 
        Data is not migrated!
        """
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        
if __name__=='__main__':
    setup()
