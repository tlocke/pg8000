from pg8000 import pg_scram
from os import environ

db_connect = {
    'user': 'postgres',
    'password': 'pw'
}

try:
    db_connect['port'] = int(environ['PGPORT'])
except KeyError:
    pass


# Test scram authentication
def test_client_first_message():
    cfmb = pg_scram._client_first_message_bare(
        'user', 'rOprNGfwEbeRWgbNEkqO')
    cfm = pg_scram._client_first_message(cfmb)
    assert cfm == 'n,,n=user,r=rOprNGfwEbeRWgbNEkqO'


def test_client_final_message():
    server_signature, cfm = pg_scram._client_final_message(
        'pencil', 'W22ZaJ0SNY7soEsUEjb6gQ==', 4096,
        'rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0',
        'n=user,r=rOprNGfwEbeRWgbNEkqO',
        'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,'
        's=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096')

    assert server_signature == '6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4='

    assert cfm == 'c=biws,' \
        'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,' \
        'p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='


def test_auth():
    auth = pg_scram.Auth(
        ['SCRAM-SHA-256'], 'user', 'pencil', c_nonce='rOprNGfwEbeRWgbNEkqO')

    c = auth.get_client_first_message()
    assert c == 'n,,n=user,r=rOprNGfwEbeRWgbNEkqO'

    auth.set_server_first_message(
        'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,'
        's=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096')

    assert auth.get_client_final_message() == 'c=biws,' \
        'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,' \
        'p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='


def test_auth2():
    auth = pg_scram.Auth(
        ['SCRAM-SHA-256'], 'postgres', 'pw',
        c_nonce='937c07c9-cc4d-4814-9ecc-4ce61dca59ec')

    c = auth.get_client_first_message()
    assert c == 'n,,n=postgres,r=937c07c9-cc4d-4814-9ecc-4ce61dca59ec'

    auth.set_server_first_message(
        'r=937c07c9-cc4d-4814-9ecc-4ce61dca59ecui/KxW2DbipGf01/0UGxiSur,'
        's=MrOlCLT9oSSpjWskOZPYuA==,i=409')

    assert auth.get_client_final_message() == 'c=biws,' \
        'r=937c07c9-cc4d-4814-9ecc-4ce61dca59ecui/KxW2DbipGf01/0UGxiSur,' \
        'p=8dE/JQRChRa8E+wT5OUSNNJLAj3WZ50JS1DjdwbzEz4='
