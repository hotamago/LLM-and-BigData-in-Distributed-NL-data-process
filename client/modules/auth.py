from models.engine import SessionLocal
from models.account import Account, RoleEnum
import jwt
import datetime
import os
from typing import Optional, Dict, Any
from jwt_config import JWT_SECRET_KEY, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_MINUTES, JWT_ALGORITHM

current_user = None

def load_users():
    """Load and return list of user dicts from database."""
    with SessionLocal() as db:
        accounts = db.query(Account).all()
        return [
            {'id': a.id, 'username': a.username, 'is_admin': a.role == RoleEnum.admin}
            for a in accounts
        ]


def create_access_token(data: Dict[str, Any], expires_delta: Optional[datetime.timedelta] = None) -> str:
    """Create a new JWT access token."""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: Dict[str, Any]) -> str:
    """Create a new JWT refresh token with longer expiration."""
    to_encode = data.copy()
    expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "refresh": True})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify a JWT token and return the payload if valid."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return None


def refresh_access_token(refresh_token: str) -> Optional[str]:
    """Generate a new access token using a valid refresh token."""
    payload = verify_token(refresh_token)
    
    if not payload or not payload.get("refresh"):
        return None
    
    # Remove the refresh flag and expiration from payload for the new token
    refresh_data = payload.copy()
    refresh_data.pop("exp", None)
    refresh_data.pop("refresh", None)
    
    # Create a new access token
    return create_access_token(refresh_data)


def authenticate(username, password):
    """Authenticate user and return user info with tokens if successful."""
    with SessionLocal() as db:
        account = db.query(Account).filter(Account.username == username).first()
        if account and account.check_password(password):
            user_data = {
                'id': str(account.id),
                'username': account.username, 
                'is_admin': account.role == RoleEnum.admin
            }
            
            # Create tokens
            access_token = create_access_token(user_data)
            refresh_token = create_refresh_token(user_data)
            
            # Return user data with tokens
            return {
                **user_data,
                'access_token': access_token,
                'refresh_token': refresh_token
            }
        return None


def authenticate_with_token(token: str):
    """Authenticate user using JWT token."""
    payload = verify_token(token)
    if payload:
        return {
            'id': payload.get('id'),
            'username': payload.get('username'),
            'is_admin': payload.get('is_admin')
        }
    return None


def create_user(username, password, is_admin=False):
    """Add a new user to the database."""
    with SessionLocal() as db:
        if db.query(Account).filter(Account.username == username).first():
            raise ValueError('User already exists')
        account = Account(username=username)
        account.set_password(password)
        account.role = RoleEnum.admin if is_admin else RoleEnum.user
        db.add(account)
        db.commit()


def update_user(username, password=None, is_admin=None):
    """Update existing user credentials or admin flag in database."""
    with SessionLocal() as db:
        account = db.query(Account).filter(Account.username == username).first()
        if not account:
            raise ValueError('User not found')
        if password:
            account.set_password(password)
        if is_admin is not None:
            account.role = RoleEnum.admin if is_admin else RoleEnum.user
        db.commit()


def delete_user(username):
    """Remove a user from the database."""
    with SessionLocal() as db:
        account = db.query(Account).filter(Account.username == username).first()
        if account:
            db.delete(account)
            db.commit()


def set_current_user(user):
    """Set the current authenticated user (dict)."""
    global current_user
    current_user = user


def get_current_user():
    """Get the current authenticated user dict."""
    return current_user
