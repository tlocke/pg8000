from passlib.hash import scram
import hmac
from uuid import uuid4
from passlib.utils import saslprep
from base64 import b64encode, b64decode
import hashlib


# https://tools.ietf.org/html/rfc5802
# https://www.rfc-editor.org/rfc/rfc7677.txt

class Auth(object):
    def __init__(self, mechanisms, username, password, c_nonce=None):
        if 'SCRAM-SHA-256' not in mechanisms:
            raise Exception(
                "The only recognized mechanism is SCRAM-SHA-256, and this "
                "can't be found in " + mechanisms + ".")
        self.mechanisms = mechanisms
        if c_nonce is None:
            self.c_nonce = str(uuid4()).replace('-', '')
        else:
            self.c_nonce = c_nonce
        self.username = username
        self.password = password

    def get_client_first_message(self):
        self.client_first_message_bare = _client_first_message_bare(
            self.username, self.c_nonce)
        return _client_first_message(self.client_first_message_bare)

    def set_server_first_message(self, message):
        self.server_first_message = message
        msg = _parse_message(message)
        self.nonce = msg['r']
        self.salt = msg['s']
        self.iterations = int(msg['i'])

        if not self.nonce.startswith(self.c_nonce):
            raise Exception("Client nonce doesn't match.")

    def get_client_final_message(self):
        server_signature, cfm = _client_final_message(
            self.password, self.salt, self.iterations, self.nonce,
            self.client_first_message_bare, self.server_first_message)

        self.server_signature = server_signature
        return cfm

    def set_server_final_message(self, message):
        msg = _parse_message(message)
        if self.server_signature != msg['v']:
            raise Exception("The server signature doesn't match.")


def _hmac(key, msg):
    return hmac.new(
        key, msg=msg.encode('utf8'), digestmod=hashlib.sha256).digest()


def _h(msg):
    return hashlib.sha256(msg).digest()


def _parse_message(msg):
    return dict((e[0], e[2:]) for e in msg.split(','))


def _client_first_message_bare(username, c_nonce):
    return ','.join(('n=' + saslprep(username), 'r=' + c_nonce))


def _client_first_message(client_first_message_bare):
    return 'n,,' + client_first_message_bare


def _b64enc(binary):
    return b64encode(binary).decode('utf8')


def _b64dec(string):
    return b64decode(string)


def _client_final_message(
        password, salt, iterations, nonce, client_first_message_bare,
        server_first_message):

    salted_password = scram.derive_digest(
        password, _b64dec(salt), iterations, 'sha-256')
    client_key = _hmac(salted_password, "Client Key")
    stored_key = _h(client_key)

    message = ['c=' + _b64enc(b'n,,'), 'r=' + nonce]

    auth_message = ','.join(
        (client_first_message_bare, server_first_message, ','.join(message)))

    client_signature = _hmac(stored_key, auth_message)
    client_proof = bytes(
        a ^ b for a, b in zip(client_key, client_signature))
    server_key = _hmac(salted_password, "Server Key")
    server_signature = _hmac(server_key, auth_message)

    message.append('p=' + _b64enc(client_proof))
    return _b64enc(server_signature), ','.join(message)
