from passlib.hash import scram
import hmac
from uuid import uuid4
from base64 import b64encode, b64decode
import hashlib
import stringprep
import unicodedata


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


def saslprep(source, param="value"):
    if not isinstance(source, str):
        raise TypeError("input must be str, not %s" % (type(source),))

    # mapping stage
    #   - map non-ascii spaces to U+0020 (stringprep C.1.2)
    #   - strip 'commonly mapped to nothing' chars (stringprep B.1)
    in_table_c12 = stringprep.in_table_c12
    in_table_b1 = stringprep.in_table_b1
    data = ''.join(
        ' ' if in_table_c12(c) else c for c in source if not in_table_b1(c))

    # normalize to KC form
    data = unicodedata.normalize('NFKC', data)
    if not data:
        return ''

    # check for invalid bi-directional strings.
    # stringprep requires the following:
    #   - chars in C.8 must be prohibited.
    #   - if any R/AL chars in string:
    #       - no L chars allowed in string
    #       - first and last must be R/AL chars
    # this checks if start/end are R/AL chars. if so, prohibited loop
    # will forbid all L chars. if not, prohibited loop will forbid all
    # R/AL chars instead. in both cases, prohibited loop takes care of C.8.
    is_ral_char = stringprep.in_table_d1
    if is_ral_char(data[0]):
        if not is_ral_char(data[-1]):
            raise ValueError("malformed bidi sequence in " + param)
        # forbid L chars within R/AL sequence.
        is_forbidden_bidi_char = stringprep.in_table_d2
    else:
        # forbid R/AL chars if start not setup correctly; L chars allowed.
        is_forbidden_bidi_char = is_ral_char

    # check for prohibited output
    # stringprep tables A.1, B.1, C.1.2, C.2 - C.9
    in_table_a1 = stringprep.in_table_a1
    in_table_c21_c22 = stringprep.in_table_c21_c22
    in_table_c3 = stringprep.in_table_c3
    in_table_c4 = stringprep.in_table_c4
    in_table_c5 = stringprep.in_table_c5
    in_table_c6 = stringprep.in_table_c6
    in_table_c7 = stringprep.in_table_c7
    in_table_c8 = stringprep.in_table_c8
    in_table_c9 = stringprep.in_table_c9
    for c in data:
        # check for chars mapping stage should have removed
        assert not in_table_b1(c), "failed to strip B.1 in mapping stage"
        assert not in_table_c12(c), "failed to replace C.1.2 in mapping stage"

        # check for forbidden chars
        if in_table_a1(c):
            raise ValueError("unassigned code points forbidden in " + param)
        if in_table_c21_c22(c):
            raise ValueError("control characters forbidden in " + param)
        if in_table_c3(c):
            raise ValueError("private use characters forbidden in " + param)
        if in_table_c4(c):
            raise ValueError("non-char code points forbidden in " + param)
        if in_table_c5(c):
            raise ValueError("surrogate codes forbidden in " + param)
        if in_table_c6(c):
            raise ValueError("non-plaintext chars forbidden in " + param)
        if in_table_c7(c):
            # XXX: should these have been caught by normalize?
            # if so, should change this to an assert
            raise ValueError("non-canonical chars forbidden in " + param)
        if in_table_c8(c):
            raise ValueError("display-modifying / deprecated chars "
                             "forbidden in" + param)
        if in_table_c9(c):
            raise ValueError("tagged characters forbidden in " + param)

        # do bidi constraint check chosen by bidi init, above
        if is_forbidden_bidi_char(c):
            raise ValueError("forbidden bidi character in " + param)

    return data
