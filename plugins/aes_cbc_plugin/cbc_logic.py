import os
import base64

from airflow.plugins_manager import AirflowPlugin
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


class AESCrypto:

    # 🔐 Constant AES Key (must be 16 / 24 / 32 bytes)
    AES_KEY = b"1234567890123456"

    def encrypt(plaintext):

        iv = os.urandom(16)

        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(plaintext.encode()) + padder.finalize()

        cipher = Cipher(
            algorithms.AES(AESCrypto.AES_KEY),
            modes.CBC(iv)
        )

        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        return base64.b64encode(iv + ciphertext).decode()

    def decrypt(encrypted_text):

        encrypted_data = base64.b64decode(encrypted_text)

        iv = encrypted_data[:16]
        ciphertext = encrypted_data[16:]

        cipher = Cipher(
            algorithms.AES(AESCrypto.AES_KEY),
            modes.CBC(iv)
        )

        decryptor = cipher.decryptor()

        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

        return plaintext.decode()

