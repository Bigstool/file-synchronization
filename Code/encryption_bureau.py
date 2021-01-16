"""
encryption_bureau provides the encrypt/decrypt functions
"""
import hashlib
from Crypto.Cipher import AES


BLOCK_SIZE = AES.block_size
KEY = hashlib.sha256('TwoDrive'.encode()).digest()
IV = 16 * b'\x00'


def encrypt(data):
    data = pad(data)
    cipher = AES.new(KEY, AES.MODE_CBC, IV)
    encrypted = IV + cipher.encrypt(data)
    return encrypted


def decrypt(data):
    cipher = AES.new(KEY, AES.MODE_CBC, IV)
    decrypted = cipher.decrypt(data[AES.block_size:])
    decrypted = unpad(decrypted)
    return decrypted


def pad(data):
    return data + (BLOCK_SIZE - len(data) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(data) % BLOCK_SIZE).encode()


def unpad(data):
    return data[:-ord(data[len(data) - 1:].decode())]
