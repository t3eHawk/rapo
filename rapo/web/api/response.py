"""Contains default HTTP responses."""

import flask


OK = flask.Response(status=200)
BAD_REQUEST = flask.Response(status=400)
SERVER_ERROR = flask.Response(status=500)
