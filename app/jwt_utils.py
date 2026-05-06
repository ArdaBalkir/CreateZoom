"""
JWT utilities for parsing and extracting user information from tokens.
"""
import base64
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def decode_jwt_payload(token: str) -> Optional[Dict[str, Any]]:
    """
    Decode a JWT token and extract the payload without verification.
    This is used for logging purposes only - actual auth is done by EBRAINS.
    
    Args:
        token: The JWT token string
        
    Returns:
        Decoded payload as a dictionary, or None if decoding fails
    """
    try:
        # JWT format: header.payload.signature
        parts = token.split('.')
        if len(parts) != 3:
            logger.warning("Invalid JWT format - expected 3 parts, got %d", len(parts))
            return None
        
        # Decode the payload (second part)
        payload_b64 = parts[1]
        
        # Add padding if needed (base64 requires padding to be multiple of 4)
        padding_needed = 4 - (len(payload_b64) % 4)
        if padding_needed != 4:
            payload_b64 += '=' * padding_needed
        
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes.decode('utf-8'))
        
        logger.debug("Successfully decoded JWT payload")
        return payload
        
    except Exception as e:
        logger.error("Failed to decode JWT payload: %s", str(e))
        return None


def extract_user_info(token: str) -> Dict[str, Optional[str]]:
    """
    Extract user information from a JWT token.
    
    Args:
        token: The JWT token string
        
    Returns:
        Dictionary containing user info (username, email, name, sub)
    """
    user_info = {
        "username": None,
        "email": None,
        "name": None,
        "given_name": None,
        "family_name": None,
        "sub": None,  # Subject identifier (unique user ID)
    }
    
    payload = decode_jwt_payload(token)
    if not payload:
        logger.warning("Could not extract user info - JWT decode failed")
        return user_info
    
    # Extract common JWT claims
    user_info["username"] = payload.get("preferred_username") or payload.get("username")
    user_info["email"] = payload.get("email")
    user_info["name"] = payload.get("name")
    user_info["given_name"] = payload.get("given_name")
    user_info["family_name"] = payload.get("family_name")
    user_info["sub"] = payload.get("sub")
    
    logger.info(
        "Extracted user info - Username: %s, Email: %s, Name: %s",
        user_info["username"] or "N/A",
        user_info["email"] or "N/A", 
        user_info["name"] or "N/A"
    )
    
    return user_info


def get_user_display_string(user_info: Dict[str, Optional[str]]) -> str:
    """
    Create a human-readable string identifying the user.
    
    Args:
        user_info: Dictionary with user information
        
    Returns:
        A formatted string like "John Doe (john.doe@example.com)"
    """
    name = user_info.get("name") or user_info.get("username") or "Unknown User"
    email = user_info.get("email")
    
    if email:
        return f"{name} ({email})"
    return name
