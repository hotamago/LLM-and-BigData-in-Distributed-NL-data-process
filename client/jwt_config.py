import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# JWT Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "422b9bac548cef8f68b11591d086768f5c68986979f8e2d60055a914adb3e6e1")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 1440))  # 24 hours by default
REFRESH_TOKEN_EXPIRE_MINUTES = int(os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", 10080))  # 7 days by default

# Token configuration for application
JWT_ALGORITHM = "HS256" 