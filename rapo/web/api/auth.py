"""."""

import flask_httpauth

from ...config import config


auth = flask_httpauth.HTTPTokenAuth(scheme='Bearer')
TOKEN = config['API']['token']


@auth.verify_token
def verify_token(token):
    """."""
    if token == TOKEN:
        return token
