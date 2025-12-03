"""
Authentication module for Planetstore.
Handles user registration, login, and JWT token management.
"""
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy import text

# Configuration
SECRET_KEY = "your-secret-key-change-in-production-please-use-env-variable"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 24 * 60  # 24 hours

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login", auto_error=False)

# Models
class User(BaseModel):
    id: int
    username: str
    email: str

class UserCreate(BaseModel):
    username: str
    email: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

# Password utilities
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash."""
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def get_password_hash(password: str) -> str:
    """Hash a password."""
    # Bcrypt automatically handles the 72 byte limit
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

# Token utilities
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[str]:
    """Verify a JWT token and return username."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return None
        return username
    except JWTError:
        return None

# Database utilities
def get_user_by_username(meta_mgr, username: str) -> Optional[dict]:
    """Get user from database by username."""
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        result = db_session.execute(
            text("SELECT id, username, email, password_hash FROM users WHERE username = :username"),
            {"username": username}
        ).fetchone()
        
        if result:
            return {
                "id": result[0],
                "username": result[1],
                "email": result[2],
                "password_hash": result[3]
            }
        return None
    finally:
        db_session.close()

def create_user(meta_mgr, username: str, email: str, password: str) -> dict:
    """Create a new user in the database."""
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        password_hash = get_password_hash(password)
        
        result = db_session.execute(text("""
            INSERT INTO users (username, email, password_hash)
            VALUES (:username, :email, :password_hash)
            RETURNING id, username, email
        """), {"username": username, "email": email, "password_hash": password_hash})
        
        db_session.commit()
        row = result.fetchone()
        
        return {
            "id": row[0],
            "username": row[1],
            "email": row[2]
        }
    finally:
        db_session.close()

def authenticate_user(meta_mgr, username: str, password: str) -> Optional[dict]:
    """Authenticate a user."""
    user = get_user_by_username(meta_mgr, username)
    if not user:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    return user

def update_last_login(meta_mgr, username: str):
    """Update user's last login timestamp."""
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        db_session.execute(
            text("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE username = :username"),
            {"username": username}
        )
        db_session.commit()
    finally:
        db_session.close()

# Dependency for protected routes (optional)
async def get_current_user(token: str = Depends(oauth2_scheme), meta_mgr=None):
    """Get current user from JWT token. Returns None if not authenticated."""
    if not token:
        return None
    
    username = verify_token(token)
    if username is None:
        return None
    
    if meta_mgr:
        user = get_user_by_username(meta_mgr, username)
        if user:
            return User(id=user["id"], username=user["username"], email=user["email"])
    
    return None
