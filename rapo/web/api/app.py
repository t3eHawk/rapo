"""Contains web API application and routes."""

import os

import flask

from .auth import auth
from .response import OK

from ...reader import reader

from ...main.control import Control

from datetime import datetime

app = flask.Flask(__name__)

app.static_folder = 'ui'


@app.route('/')
def serve_ui_index():
    return flask.send_from_directory(app.static_folder, 'index.html')


@app.route('/favicon.ico')
def serve_ui_favicon():
    return flask.send_from_directory(app.static_folder, 'favicon.ico')


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
    if request.method == 'POST':
        control = Control(name, date_from=date_from, date_to=date_to, date=date)
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


@app.route('/api/get-datasources')
@auth.login_required
def get_datasources():
    """Get list of all DB datasources in JSON."""
    rows = reader.read_datasources()
    response = flask.jsonify(rows)
    return response


@app.route('/api/get-datasource-columns')
@auth.login_required
def get_datasource_coluimns():
    """Get list of DB datasource columns in JSON."""
    request = flask.request
    
    if 'datasource_name' in request.args:
        rows = reader.read_datasource_columns(request.args['datasource_name'])
    else:
        rows = []
    
    response = flask.jsonify(rows)
    return response


@app.route('/api/get-datasource-date-columns')
@auth.login_required
def get_datasource_date_coluimns():
    """Get list of tables in JSON."""
    request = flask.request
    
    if 'datasource_name' in request.args:
        rows = reader.read_datasource_date_columns(request.args['datasource_name'])
    else:
        rows = []
    
    response = flask.jsonify(rows)
    return response


@app.route('/api/save-control', methods=['POST', 'OPTIONS'])
@auth.login_required
def save_control():
    """Create or update control to config table."""
    request = flask.request

    if request.method == 'POST':
        try:
            data = request.get_json() 
            reader.save_control(data)
            response = flask.jsonify(status=200)
        except:
            response = flask.jsonify(status=400)
    else:
        response = flask.jsonify([])

    return response


@app.route('/api/delete-control', methods=['DELETE', 'OPTIONS'])
@auth.login_required
def delete_control():
    """Save control to config table."""
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
