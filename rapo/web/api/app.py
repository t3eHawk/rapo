"""Contains web API application and routes."""

import os

import flask

from .auth import auth
from .response import OK

from ...logger import logger
from ...reader import reader

from ...core.control import Control


app = flask.Flask(__name__)
app.static_folder = 'ui'
logger.configure(console=False)


@app.route('/favicon.ico')
def serve_ui_favicon():
    return flask.send_from_directory(app.static_folder, 'favicon.ico')


@app.route('/edit-control/<int:control_id>')
def serve_edit_control(control_id):
    return flask.send_from_directory(app.static_folder, 'index.html')


@app.route('/')
def serve_ui_index():
    return flask.send_from_directory(app.static_folder, 'index.html')


@app.route('/<path:filename>')
def serve_ui_files(filename):
    return flask.send_from_directory(app.static_folder, filename)


@app.route('/<path>')
def serve_ui_catch_all(path):
    return flask.send_from_directory(app.static_folder, 'index.html')


@app.route('/api/help')
@auth.login_required
def help():
    """Get help message."""
    path = os.path.join(os.path.dirname(__file__), 'templates/help.html')
    lines = open(path, 'r').readlines()
    text = ''.join(lines)
    return text


@app.route('/api/run-control', methods=['POST', 'OPTIONS'])
@auth.login_required
def run_control():
    """Run control and get its result in JSON."""
    request = flask.request
    name = request.args['name']
    date = request.args.get('date')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    debug_mode = request.args.get('debug_mode', 'false').lower() == 'true'
    if request.method == 'POST':
        control = Control(name, date_from=date_from, date_to=date_to,
                          date=date, debug_mode=debug_mode)
        control.launch()
    response = flask.jsonify(status=200)
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


@app.route('/api/revoke-control-run', methods=['DELETE'])
@auth.login_required
def revoke_control_run():
    """Revoke patricular control run."""
    request = flask.request
    process_id = int(request.args['id'])
    control = Control(process_id=process_id)
    control.revoke()
    return OK


@app.route('/api/delete-control-output-tables', methods=['DELETE'])
@auth.login_required
def delete_control_output_tables():
    """Run control and get its result in JSON."""
    request = flask.request
    name = request.args['name']
    control = Control(name)
    control.executor.delete_output_tables()
    response = flask.jsonify(status=200)
    return response


@app.route('/api/delete-control-temporary-tables', methods=['DELETE'])
@auth.login_required
def delete_control_temporary_tables():
    """Run control and get its result in JSON."""
    request = flask.request
    process_id = int(request.args['id'])
    control = Control(process_id=process_id)
    control.executor.delete_temporary_tables()
    response = flask.jsonify(status=200)
    return response


@app.route('/api/get-running-controls')
@auth.login_required
def get_running_controls():
    """Get list of currently running controls in JSON."""
    rows = reader.read_running_controls()
    response = flask.jsonify(rows)
    return response


@app.route('/api/get-all-controls')
@auth.login_required
def get_all_controls():
    """Get list of all controls in JSON."""
    rows = reader.read_control_config_all()
    response = flask.jsonify(rows)
    return response


@app.route('/api/get-control-versions')
@auth.login_required
def get_control_versions():
    """Get list of control versions by ID in JSON."""
    request = flask.request

    if 'control_id' in request.args:
        rows = reader.read_control_config_versions(request.args['control_id'])
    else:
        rows = []

    response = flask.jsonify(rows)
    return response


@app.route('/api/get-control-runs')
@auth.login_required
def get_control_runs():
    """Get list of all control runs in JSON."""
    rows = reader.read_control_results_for_day()
    response = flask.jsonify(rows)
    return response


@app.route('/api/read-control-logs')
@auth.login_required
def read_control_logs():
    """Get list of DB datasource columns in JSON."""
    request = flask.request

    if 'control_name' in request.args:
        rows = reader.read_control_logs(request.args['control_name'], int(request.args['days']) if 'days' in request.args else 31, ['W', 'C', 'E', 'D', 'I', 'S', 'P', 'F', 'X'])
    else:
        rows = []

    response = flask.jsonify(rows)
    return response


@app.route('/api/get-datasources')
@auth.login_required
def get_datasources():
    """Get list of all DB datasources in JSON."""
    rows = reader.read_datasources()
    response = flask.jsonify(rows)
    return response


@app.route('/api/get-datasource-columns')
@auth.login_required
def get_datasource_columns():
    """Get list of DB datasource columns in JSON."""
    request = flask.request

    if 'datasource_name' in request.args:
        rows = reader.read_datasource_columns(request.args['datasource_name'])
    else:
        rows = []

    response = flask.jsonify(rows)
    return response


@app.route('/api/save-control', methods=['POST', 'OPTIONS'])
@auth.login_required
def save_control():
    """Create or update control in configuration table."""
    request = flask.request

    if request.method == 'POST':
        try:
            data = request.get_json()
            reader.save_control(data)
            response = flask.jsonify(status=200)
        except Exception:
            response = flask.jsonify(status=400)
    else:
        response = flask.jsonify([])

    return response


@app.route('/api/delete-control', methods=['DELETE', 'OPTIONS'])
@auth.login_required
def delete_control():
    """Delete control from configuration table."""
    request = flask.request
    control_id = int(request.args['control_id'])

    if request.method == 'DELETE':
        reader.delete_control(control_id)

    response = flask.jsonify(status=200)
    return response


@app.route('/api/get-control-run')
@auth.login_required
def get_control_run():
    request = flask.request
    process_id = request.args['process_id']
    control = Control(process_id=process_id)
    response = flask.jsonify(name=control.name,
                             date_from=control.date_from,
                             date_to=control.date_to,
                             process_id=control.process_id,
                             start_date=control.start_date,
                             end_date=control.end_date,
                             status=control.status,
                             fetched_number=control.fetched_number,
                             success_number=control.success_number,
                             error_number=control.error_number,
                             error_level=control.error_level,
                             fetched_number_a=control.fetched_number_a,
                             fetched_number_b=control.fetched_number_b,
                             success_number_a=control.success_number_a,
                             success_number_b=control.success_number_b,
                             error_number_a=control.error_number_a,
                             error_number_b=control.error_number_b,
                             error_level_a=control.error_level_a,
                             error_level_b=control.error_level_b)
    return response
