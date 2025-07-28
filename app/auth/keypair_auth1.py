import jwt
import time
import base64
import hashlib
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from typing import Dict, Optional
import httpx
import os

class SnowflakeKeyPair:
    def __init__(self, account: str, username: str, private_key_path: str, passphrase: Optional[str] = None):
        self.account = account
        self.username = username
        self.private_key_path = private_key_path
        self.passphrase = passphrase
        self.base_url = f"https://{account}.snowflakecomputing.com"
        self.private_key = None
        self.public_key_fp = None
        self._validate_key_file()
        
    def _validate_key_file(self):
        """Validate that the private key file exists and is readable"""
        if not os.path.exists(self.private_key_path):
            raise FileNotFoundError(f"Private key file not found: {self.private_key_path}")
        
        if not os.access(self.private_key_path, os.R_OK):
            raise PermissionError(f"Cannot read private key file: {self.private_key_path}")
            
        # Check file permissions (should be 600 or 400 for security)
        file_stat = os.stat(self.private_key_path)
        file_permissions = oct(file_stat.st_mode)[-3:]
        if file_permissions not in ['600', '400']:
            print(f"Warning: Private key file has permissions {file_permissions}. Consider setting to 600 or 400 for security.")
        
    def load_private_key(self):
        """Load and decrypt private key from file with passphrase support"""
        try:
            with open(self.private_key_path, 'rb') as key_file:
                private_key_bytes = key_file.read()
            
            # Convert passphrase to bytes if provided
            passphrase_bytes = None
            if self.passphrase:
                passphrase_bytes = self.passphrase.encode('utf-8')
            
            # Load the encrypted private key
            self.private_key = load_pem_private_key(
                private_key_bytes,
                password=passphrase_bytes,
                backend=default_backend()
            )
            
            # Generate public key fingerprint for Snowflake
            public_key = self.private_key.public_key()
            
            # Serialize public key in DER format
            public_key_der = public_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            
            # Create SHA256 fingerprint and encode as base64 (Snowflake format)
            sha256_hash = hashlib.sha256(public_key_der).digest()
            self.public_key_fp = base64.b64encode(sha256_hash).decode('utf-8')
            
            print(f"Successfully loaded encrypted private key. Public key fingerprint: {self.public_key_fp[:20]}...")
            
        except ValueError as e:
            if "Bad decrypt" in str(e) or "invalid" in str(e).lower():
                raise Exception(f"Failed to decrypt private key. Please check your passphrase: {e}")
            else:
                raise Exception(f"Invalid private key format: {e}")
        except Exception as e:
            raise Exception(f"Failed to load encrypted private key: {e}")
    
    def get_public_key_pem(self) -> str:
        """Get public key in PEM format for Snowflake user configuration"""
        if not self.private_key:
            self.load_private_key()
            
        public_key = self.private_key.public_key()
        
        # Get public key in PEM format
        public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')
        
        # Remove PEM headers and footers, keep only the key content
        lines = public_key_pem.strip().split('\n')[1:-1]
        return ''.join(lines)
    
    def create_jwt_token(self) -> str:
        """Create JWT token for Snowflake authentication"""
        if not self.private_key:
            self.load_private_key()
            
        now = int(time.time())
        
        # Create qualified username with public key fingerprint
        qualified_username = f"{self.account.upper()}.{self.username.upper()}"
        issuer = f"{qualified_username}.SHA256:{self.public_key_fp}"
        
        payload = {
            'iss': issuer,
            'sub': qualified_username,
            'iat': now,
            'exp': now + 3600,  # 1 hour expiration
            'aud': f"{self.account}.snowflakecomputing.com"  # Add audience for better security
        }
        
        # Create JWT token using RS256 algorithm
        token = jwt.encode(payload, self.private_key, algorithm='RS256')
        
        return token
    
    def test_key_decryption(self) -> bool:
        """Test if the key can be successfully decrypted"""
        try:
            self.load_private_key()
            return True
        except Exception as e:
            print(f"Key decryption test failed: {e}")
            return False
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests"""
        try:
            jwt_token = self.create_jwt_token()
            
            return {
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "FastAPI-Snowflake-Client/1.0.0",
                "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
            }
        except Exception as e:
            raise Exception(f"Failed to create authentication headers: {e}")