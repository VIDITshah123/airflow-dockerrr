import os
import base64

from airflow.plugins_manager import AirflowPlugin
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class AESGCMCrypto:

    # 🔐 Constant AES Key (must be 16, 24, or 32 bytes)
    AES_KEY = b"12345678901234567890123456789012"   # AES-256

    @staticmethod
    def encrypt(plaintext: str):

        aesgcm = AESGCM(AESGCMCrypto.AES_KEY)

        # 12 bytes nonce recommended for GCM
        nonce = os.urandom(12)

        ciphertext = aesgcm.encrypt(
            nonce,
            plaintext.encode(),
            None
        )

        # store nonce + ciphertext
        encrypted_data = nonce + ciphertext

        return base64.b64encode(encrypted_data).decode()


    @staticmethod
    def decrypt(encrypted_text: str):

        encrypted_data = base64.b64decode(encrypted_text)

        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]

        aesgcm = AESGCM(AESGCMCrypto.AES_KEY)

        plaintext = aesgcm.decrypt(
            nonce,
            ciphertext,
            None
        )

        return plaintext.decode()
