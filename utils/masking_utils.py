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
    """Encrypt selected PII columns in a DataFrame.

    Args:
        df: Input DataFrame
        pii_columns: Columns to encrypt

    Returns:
        DataFrame with encrypted PII columns
    """
    if not pii_columns:
        return df

    key_str = _get_encryption_key()
    df_columns = set(df.columns)

    column_updates = {}
    for col_name in pii_columns:
        if col_name in df_columns:
            column_updates[col_name] = when(
                col(col_name).isNotNull(),
                _encrypt_udf(col(col_name).cast("string"), lit(key_str))
            ).otherwise(col(col_name))
            print(f"Encrypted PII column: {col_name}")
        else:
            print(f"Warning: missing column {col_name}")

    return df.withColumns(column_updates) if column_updates else df


def decrypt_pii_columns(df: DataFrame, pii_columns: list) -> DataFrame:
    """Decrypt previously encrypted PII columns.

    Args:
        df: Input DataFrame
        pii_columns: Columns to decrypt

    Returns:
        DataFrame with decrypted values
    """
    if not pii_columns:
        return df

    key_str = _get_encryption_key()
    df_columns = set(df.columns)

    column_updates = {}
    for col_name in pii_columns:
        if col_name in df_columns:
            column_updates[col_name] = when(
                col(col_name).isNotNull(),
                _decrypt_udf(col(col_name), lit(key_str))
            ).otherwise(col(col_name))
            print(f"Decrypted PII column: {col_name}")
        else:
            print(f"Warning: missing column {col_name}")

    return df.withColumns(column_updates) if column_updates else df