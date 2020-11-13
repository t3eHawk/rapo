"""Contains web API application and routes."""

import os

import flask

from .auth import auth
from .response import OK

from ...reader import reader

from ...main.control import Control


app = flask.Flask(__name__)


@app.route('/api/help')
@auth.login_required
def help():
    """Get help message."""
    path = os.path.join(os.path.dirname(__file__), 'templates/help.html')
    lines = open(path, 'r').readlines()
    text = ''.join(lines)
    return text


@app.route('/api/run-control', methods=['POST'])
@auth.login_required
def run_control():
    """Run control and get its result in JSON."""
    request = flask.request
    name = request.args['name']
    date = request.args.get('date')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    control = Control(name, date_from=date_from, date_to=date_to, date=date)
    control.process()
    response = flask.jsonify(name=control.name,
                             date_from=control.date_from,
                             date_to=control.date_to,
                             process_id=control.process_id,
                             start_date=control.start_date,
                             end_date=control.end_date,
                             status=control.status,
                             fetched=control.fetched,
                             success=control.success,
                             errors=control.errors,
                             error_level=control.error_level,
                             fetched_a=control.fetched_a,
                             fetched_b=control.fetched_b,
                             success_a=control.success_a,
                             success_b=control.success_b,
                             errors_a=control.errors_a,
                             errors_b=control.errors_b,
                             error_level_a=control.error_level_a,
                             error_level_b=control.error_level_b)
    return response


@app.route('/api/cancel-control', methods=['POST'])
@auth.login_required
def cancel_control():
    """Cancel running control."""
    request = flask.request
    process_id = int(request.args['id'])
    control = Control(process_id=process_id)
    control.cancel()
    return OK


@app.route('/api/get-running-controls')
@auth.login_required
def get_running_controls():
    """Get list of currently running controls in JSON."""
    rows = reader.read_running_controls()
    response = flask.jsonify(rows)
    return response


@app.route('/api/revoke-control-run', methods=['DELETE'])
@auth.login_required
def revoke_control_run():
    """Revoke patricular control run."""
    request = flask.request
    process_id = int(request.args['id'])
    control = Control(process_id=process_id)
    control.revoke()
    return OK
