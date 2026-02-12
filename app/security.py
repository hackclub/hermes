import hashlib
import secrets

# Fixed application-level salt for PBKDF2 key hashing.
# This is NOT a per-key salt, but combined with 600k iterations it makes
# brute-force infeasible even if the DB is leaked (unlike plain SHA-256).
_API_KEY_SALT = b"hermes-api-key-v1"
_API_KEY_ITERATIONS = 600_000


def generate_api_key(length: int = 64) -> str:
    """Generate a secure random API key."""
    return secrets.token_hex(length // 2)


def hash_api_key(api_key: str) -> str:
    """
    Hash an API key using PBKDF2-HMAC-SHA256 with 600k iterations.

    Returns the hex digest of the hash (64 chars, same column size as before).
    """
    return hashlib.pbkdf2_hmac(
        "sha256",
        api_key.encode(),
        _API_KEY_SALT,
        iterations=_API_KEY_ITERATIONS,
    ).hex()


def verify_api_key(api_key: str, api_key_hash: str) -> bool:
    """
    Verify an API key against its stored hash.

    Uses constant-time comparison to prevent timing attacks.
    """
    return secrets.compare_digest(hash_api_key(api_key), api_key_hash)
