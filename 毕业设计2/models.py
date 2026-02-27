from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class DeviceMetrics(Base):
    __tablename__ = 'device_metrics'

    id = Column(Integer, primary_key=True)
    device_id = Column(String(10), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    availability = Column(Float)
    performance = Column(Float)
    quality = Column(Float)
    oee = Column(Float)
    teep = Column(Float)
    utilization = Column(Float)
    downtime = Column(Float)

    def to_dict(self):
        return {
            'device_id': self.device_id,
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'availability': round(self.availability * 100, 2),
            'performance': round(self.performance * 100, 2),
            'quality': round(self.quality * 100, 2),
            'oee': round(self.oee * 100, 2),
            'teep': round(self.teep * 100, 2),
            'utilization': round(self.utilization * 100, 2),
            'downtime': round(self.downtime, 2)
        }

# 数据库连接
engine = create_engine('sqlite:///production.db')
Session = sessionmaker(bind=engine)

def init_db():
    Base.metadata.create_all(engine) 