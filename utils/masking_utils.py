"""PII Encryption Utilities

Provides AES-based reversible encryption/decryption for PII columns
in Spark DataFrames. Uses Azure Key Vault for key management and
Spark UDFs for distributed transformation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, when, col, lit
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet


_ENCRYPTION_KEY = None


def _get_encryption_key():
    """Fetch and cache encryption key from Azure Key Vault.

    Returns:
        str: Fernet encryption key
    """
    global _ENCRYPTION_KEY

    if _ENCRYPTION_KEY is not None:
        return _ENCRYPTION_KEY

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        _ENCRYPTION_KEY = dbutils.secrets.get(
            scope="marspcmdifcinkv",
            key="pii-encryption-key"
        )

        print("Encryption initialized: Key loaded")
        return _ENCRYPTION_KEY

    except Exception as e:
        raise RuntimeError(
            f"Failed to fetch encryption key: {e}"
        )


def _encrypt_with_key(value, key_str):
    """Encrypt value using Fernet key.

    Args:
        value: input value
        key_str: encryption key

    Returns:
        str | None: encrypted value or None
    """
    if value is None or value == "":
        return None
    try:
        cipher = Fernet(key_str.encode())
        return cipher.encrypt(str(value).encode()).decode()
    except Exception as e:
        return f"ENCRYPT_ERROR: {str(e)[:100]}"


def _decrypt_with_key(encrypted_value, key_str):
    """Decrypt value using Fernet key.

    Args:
        encrypted_value: encrypted input
        key_str: encryption key

    Returns:
        str | None: decrypted value or error string
    """
    if encrypted_value is None or encrypted_value == "":
        return None
    try:
        cipher = Fernet(key_str.encode())
        return cipher.decrypt(encrypted_value.encode()).decode()
    except Exception as e:
        return f"DECRYPT_ERROR: {str(e)[:100]}"


_encrypt_udf = udf(_encrypt_with_key, StringType())
_decrypt_udf = udf(_decrypt_with_key, StringType())



def encrypt_pii_columns(df: DataFrame, pii_columns: list) -> DataFrame:
    """STUB: PII encryption disabled (Key Vault removed in migration).
    
    Returns DataFrame unchanged. Re-enable after configuring encryption keys.

    Args:
        df: Input DataFrame
        pii_columns: Columns to encrypt (ignored for now)

    Returns:
        DataFrame unchanged
    """
    if pii_columns:
        print(f"[WARNING] PII encryption disabled — {len(pii_columns)} column(s) NOT encrypted: {pii_columns}")
        print("[WARNING] Configure encryption keys in mdif-cicd scope to re-enable")
    return df


def decrypt_pii_columns(df: DataFrame, pii_columns: list) -> DataFrame:
    """STUB: PII decryption disabled (Key Vault removed in migration).
    
    Returns DataFrame unchanged.

    Args:
        df: Input DataFrame
        pii_columns: Columns to decrypt (ignored for now)

    Returns:
        DataFrame unchanged
    """
    if pii_columns:
        print(f"[WARNING] PII decryption disabled — {len(pii_columns)} column(s) NOT decrypted: {pii_columns}")
    return df