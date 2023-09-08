#!/usr/bin/env python3
import json
from cryptography.fernet import Fernet
import sys

class DxConfig():
    """
    """
    def __init__(self):
        self.key = b'mB6l0679MPRgyJ3RNFzFDX9q7FG_rOxioeKgEv3c03e='

    def _encrypt(self, string):
        encode_str = string.encode()
        f = Fernet(self.key)
        return f.encrypt(encode_str)

    def _decrypt(self, encrypt_str):
        """
        """
        f = Fernet(self.key)
        return f.decrypt(encrypt_str).decode()

    def encrypt_json(self, json_obj):
        """
        """
        enc_password = self._encrypt(json_obj["password"]).decode("utf-8")
        enc_username = self._encrypt(json_obj["username"]).decode("utf-8")
        json_obj["password"] = enc_password
        json_obj["username"] = enc_username
        return json_obj

    def decrypt_cred(self, encrypted_cred):
        """
        """
        bytes_cred = encrypted_cred.encode("utf-8")
        return self._decrypt(bytes_cred)

