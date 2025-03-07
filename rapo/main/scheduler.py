"""Contains RAPO scheduler interface."""

import argparse
import datetime as dt
import getpass
import json
import os
import platform
import re
import signal
import subprocess as sp
import sys
import threading as th
import time
import queue

import psutil

from ..database import db
from ..config import config
from ..logger import logger
from ..reader import reader

from ..main.control import Control


class Scheduler():
    """Represents application scheduler.

    Application scheduler reads configuration from RAPO_CONFIG, schedule
    controls as a virtual jobs and run them when it is necessary in separate
    threads. Number of execution threads is limited by 5.
    Whole scheduling and execution events including errors is being logged
    into simple text files placed to logs folder near by main script with
    Scheduler instance.
    Scheduler timings (current timestamp, delay and waiting) can be seen in
    DEBUG mode.
    Schedule is being updated each 5 minutes from the beginning of the hour.

    Attributes
    ----------
    moment : float or None
        Current scheduler moment - internal timestamp.
    delay : floar or None
        Current scheduler delay - time that is needed to execute internal tasks
        including job scheduling and maintenance events.
    schedule : dict on None
        Dictionary with scheduled jobs where key is a control name and value
        is a control configuration presented as another dictionary.
    queue : queue.Queue
        Queue that consist of jobs that must be executed in FIFO method.
    executors : list
        List of threads that perform job execution.
    server : str or None
        Hostname on which this scheduler is running.
    username : str or None
        OS user that started the scheduler.
    pid : int or None
        OS PID under which this scheduler is running.
    start_date : datetime or None
        Date when scheduler was started.
    end_date : datetime or None
        Date when scheduler was stopped.
    status : str or None
        Current scheduler status.
    """

    def __init__(self):
        self.schedule = None
        self.moment = None
        self.delay = None
        self.queue = queue.Queue()
        self.executors = []
        self.maintenance = th.Event()
        self.maintainer = None

        self.table = db.tables.scheduler
        self.record = reader.read_scheduler_record()
        if self.record and self.record['status'] == 'Y':
            self.server = self.record['server']
            self.username = self.record['username']
            self.pid = int(self.record['pid'])
            self.start_date = self.record['start_date']
            self.stop_date = self.record['stop_date']
            self.status = True if self.record['status'] == 'Y' else False
        else:
            self.server = platform.node()
            self.username = getpass.getuser()
            self.pid = os.getpid()
            self.start_date = None
            self.stop_date = None
            self.status = False

        argv = self._parse_console_arguments()
        if argv:
            action = argv[0]
            if action == 'start':
                self.start()
            elif action == 'stop':
                self.stop()
        else:
            args = self._parse_arguments()
            if args.start is True:
                self._start()
            elif args.stop is True:
                self._stop()

    @property
    def running(self):
        """Check whether scheduler is running."""
        if self.status is True and self.pid and psutil.pid_exists(self.pid):
            return True
        else:
            return False

    def start(self):
        """Start scheduler.

        When scheduler is started then normally logs should start to generate
        (in console/file depending on setup).
        RAPO_SCHEDULER will be updated with information about current scheduler
        process including server, username, PID, start date and status.
        """
        return self._create()

    def stop(self):
        """Stop running scheduler.

        Process will be stopped.
        RAPO_SCHEDULER will be updated with stop date and status.
        """
        return self._destroy()

    def read(self):
        """Parse schedule from database table into appropriate structure."""
        return dict(self._sked())

    def _start(self):
        if self.running:
            message = f'scheduler already running at PID {self.pid}'
            raise Exception(message)
        logger.info('Starting scheduler...')
        self.start_date = dt.datetime.now()
        self.status = True
        self._start_signal_handlers()
        self._start_executors()
        self._start_maintainer()
        self._enable()
        logger.info(f'Scheduler started at PID {self.pid}')
        return self._run()

    def _run(self):
        self._synchronize()
        while True:
            self._process()

    def _stop(self):
        if self.status is True:
            logger.info('Stopping scheduler...')
            self.stop_date = dt.datetime.now()
            self.status = False
            self._disable()
            logger.info(f'Scheduler at PID {self.pid} stopped')
            return self._exit()

    def _create(self):
        exe = sys.executable
        file = os.path.abspath(sys.argv[0])
        args = '--start'
        settings = {}
        settings['stdout'] = sp.DEVNULL
        settings['stderr'] = sp.DEVNULL
        if sys.platform.startswith('win') is True:
            settings['creationflags'] = sp.CREATE_NO_WINDOW
        command = [exe, file, args]
        proc = sp.Popen(command, **settings)
        return proc

    def _destroy(self):
        if self.status is True:
            self.stop_date = dt.datetime.now()
            self.status = False
            self._disable()
            return self._terminate()

    def _enable(self):
        update = self.table.update().values(server=self.server,
                                            username=self.username,
                                            pid=self.pid,
                                            start_date=self.start_date,
                                            stop_date=self.stop_date,
                                            status='Y')
        db.execute(update)

    def _disable(self):
        update = self.table.update().values(stop_date=self.stop_date,
                                            status='N')
        db.execute(update)

    def _exit(self):
        return sys.exit()

    def _terminate(self):
        try:
            os.kill(self.pid, signal.SIGTERM)
        except OSError:
            message = f'scheduler at PID {self.pid} was not found'
            raise Warning(message)

    def _parse_console_arguments(self):
        return [arg for arg in sys.argv[1:] if arg.startswith('-') is False]

    def _parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', action='store_true', required=False)
        parser.add_argument('--stop', action='store_true', required=False)
        args, anons = parser.parse_known_args()
        return args

    def _start_signal_handlers(self):
        logger.debug('Starting signal handlers...')
        signal.signal(signal.SIGINT, lambda signum, frame: self._stop())
        signal.signal(signal.SIGTERM, lambda signum, frame: self._stop())
        logger.debug('Signal handlers started')

    def _start_executors(self):
        logger.debug('Starting executors...')
        thread_number = config['SCHEDULER']['control_parallelism']
        for i in range(thread_number):
            name = f'Control-Executor-{i}'
            target = self._execute
            thread = th.Thread(name=name, target=target, daemon=True)
            thread.start()
            self.executors.append(thread)
            logger.debug(f'Control Executor {i} started as {thread.name}')
        logger.debug('All executors started')

    def _start_maintainer(self):
        logger.debug('Starting maintainer...')
        name = 'Maintainer'
        target = self._maintain
        thread = th.Thread(name=name, target=target, daemon=True)
        thread.start()
        self.maintainer = thread
        logger.debug(f'Maintainer started as {thread.name}...')

    def _synchronize(self):
        logger.debug('Time will be synchronized')
        self.moment = time.time()
        logger.debug('Time was synchronized')

    def _increment(self):
        self.moment += 1

    def _process(self):
        self._read()
        self._walk()
        self._complete()
        self._next()

    def _read(self):
        try:
            interval = config['SCHEDULER']['refresh_interval']
            if not self.schedule or int(self.moment) % interval == 0:
                self.schedule = dict(self._sked())
                if self.schedule:
                    logger.debug(f'Schedule: {self.schedule}')
                else:
                    logger.debug('Schedule is empty')
        except Exception:
            logger.error()

    def _walk(self):
        now = time.localtime(self.moment)
        for name, record in self.schedule.items():
            try:
                if (
                    record['status'] is True
                    and self._check(record['mday'], now.tm_mday) is True
                    and self._check(record['wday'], now.tm_wday+1) is True
                    and self._check(record['hour'], now.tm_hour) is True
                    and self._check(record['min'], now.tm_min) is True
                    and self._check(record['sec'], now.tm_sec) is True
                ):
                    self._register(name, self.moment)
            except Exception:
                logger.error()

    def _complete(self):
        try:
            interval = config['SCHEDULER']['maintenance_interval']
            if interval and int(self.moment) % interval == 0:
                logger.debug('Maintenance triggered')
                self.maintenance.set()
            interval = config['SCHEDULER']['database_report_interval']
            if interval and int(self.moment) % interval == 0:
                report = db.engine.pool.status()
                logger.info(f'Database connection report: {report}')
        except Exception:
            logger.error()

    def _next(self):
        delay = time.time()-self.moment
        wait = 1-delay
        try:
            time.sleep(wait)
        except ValueError:
            logger.warning('TIME IS BROKEN')
            self._synchronize()
        else:
            logger.debug(f'moment={self.moment}, delay={delay}, wait={wait}')
            self._increment()

    def _sked(self):
        logger.debug('Getting schedule...')
        table = db.tables.config
        select = table.select()
        result = db.execute(select)
        schedule_keys = ['mday', 'wday', 'hour', 'min', 'sec']
        for record in result:
            try:
                control_name = record.control_name
                control_status = True if record.status == 'Y' else False
                schedule_config = dict.fromkeys(schedule_keys)
                if record.schedule_config:
                    input_config = json.loads(record.schedule_config)
                    output_config = {
                        key: value for key, value in input_config.items()
                        if key in schedule_keys
                    }
                    schedule_config.update(output_config)
                control_config = dict(**schedule_config, status=control_status)
            except Exception:
                logger.warning()
                continue
            else:
                yield control_name, control_config
        logger.debug('Schedule retrieved')

    def _check(self, unit, now):
        # Check if empty or *.
        if unit is None or re.match(r'^(\*)$', unit) is not None:
            return True
        # Check if unit is lonely digit and equals to now.
        elif re.match(r'^\d+$', unit) is not None:
            unit = int(unit)
            return True if now == unit else False
        # Check if unit is a cycle and integer division with now is true.
        elif re.match(r'^/\d+$', unit) is not None:
            unit = int(re.search(r'\d+', unit).group())
            if unit == 0:
                return False
            return True if now % unit == 0 else False
        # Check if unit is a range and now is in this range.
        elif re.match(r'^\d+-\d+$', unit) is not None:
            unit = [int(i) for i in re.findall(r'\d+', unit)]
            return True if now in range(unit[0], unit[1] + 1) else False
        # Check if unit is a list and now is in this list.
        elif re.match(r'^\d+,\s*\d+.*$', unit):
            unit = [int(i) for i in re.findall(r'\d+', unit)]
            return True if now in unit else False
        # All other cases is not for the now.
        else:
            return False

    def _register(self, name, moment):
        try:
            logger.info(f'Adding control {name}[{moment}] to queue...')
            self.queue.put((name, moment))
        except Exception:
            logger.error()
        else:
            logger.info(f'Control {name}[{moment}] was added to queue')

    def _execute(self):
        while True:
            if self.queue.empty() is False:
                name, moment = self.queue.get()
                logger.info(f'Initiating control {name}[{moment}]...')
                try:
                    control = Control(name, timestamp=moment)
                    control.run()
                    control.iterate()
                except Exception:
                    logger.error()
                else:
                    self.queue.task_done()
                    logger.info(f'Control {name}[{moment}] performed')
            time.sleep(1)

    def _maintain(self):
        while True:
            if self.maintenance.is_set():
                logger.info('Starting maintenance')
                self._clean()
                self.maintenance.clear()
                logger.info('Maintenance performed')
            time.sleep(1)

    def _clean(self):
        config = db.tables.config
        select = config.select().order_by(config.c.control_id)
        result = db.execute(select)
        for record in result:
            control = Control(name=record.control_name)
            control.clean()
