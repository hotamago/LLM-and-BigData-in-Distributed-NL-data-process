# client/models/account.py
import uuid
from sqlalchemy import Column, String, Enum as SQLAlchemyEnum
from sqlalchemy.dialects.postgresql import UUID
import bcrypt
import enum
# Import Base from the new engine module
from .engine import Base

# ... RoleEnum definition ...
class RoleEnum(enum.Enum):
    user = "user"
    admin = "admin"

class Account(Base):
    __tablename__ = 'account'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String(255), index=True, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(SQLAlchemyEnum(RoleEnum), nullable=False, default=RoleEnum.user)

    def set_password(self, password):
        self.password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def check_password(self, password):
        return bcrypt.checkpw(password.encode('utf-8'), self.password_hash.encode('utf-8'))

# Remove engine, SessionLocal, get_db, and init_db from here
# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# def get_db(): ...
# def init_db(): ...