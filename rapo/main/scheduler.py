"""Contains RAPO scheduler interface."""

import argparse
import datetime as dt
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

from ..database import db
from ..logger import logger

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
        self.moment = None
        self.delay = None
        self.schedule = None
        self.queue = queue.Queue()
        self.executors = []
        for i in range(5):
            thread = th.Thread(name=f'Thread-Executor-{i}',
                               target=self._execute, daemon=True)
            thread.start()
            self.executors.append(thread)

        self.server = None
        self.username = None
        self.pid = None
        self.start_date = None
        self.stop_date = None
        self.status = None

        argv = [arg for arg in sys.argv if arg.startswith('-') is False]
        count = len(argv)
        if count > 1:
            act = argv[1]
            if act == 'start':
                exe = sys.executable
                file = os.path.abspath(argv[0])
                args = '--start'
                kwargs = {}
                kwargs['stdout'] = sp.DEVNULL
                kwargs['stderr'] = sp.DEVNULL
                if sys.platform.startswith('win') is True:
                    kwargs['creationflags'] = sp.CREATE_NO_WINDOW
                command = [exe, file, args]
                sp.Popen(command, **kwargs)
            elif act == 'stop':
                self._stop()
        else:
            parser = argparse.ArgumentParser()
            parser.add_argument('--start',
                                action='store_true',
                                required=False)
            parser.add_argument('--stop',
                                action='store_true',
                                required=False)
            args, anons = parser.parse_known_args()
            if args.start is True:
                self._start()
            elif args.stop is True:
                self._stop()
        pass

    def start(self):
        """Start scheduler.

        When scheduler is started then normally logs should start to generate
        (in console/file depending on setup).
        RAPO_SCHEDULER will be updated with information about current scheduler
        process including server, username, PID, start date and status.
        """
        self.__start()
        pass

    def stop(self):
        """Stop running scheduler.

        Process will be stopped.
        RAPO_SCHEDULER will be updated with stop date and status.
        """
        self._stop()
        pass

    def _start(self):
        try:
            self.__start()
        except Exception:
            logger.error()
        pass

    def __start(self):
        signal.signal(signal.SIGINT, self._exit)
        signal.signal(signal.SIGTERM, self._exit)
        logger.info('Starting scheduler...')
        self.schedule = dict(self._sked())
        if self.schedule:
            logger.debug(f'Schedule: {self.schedule}')
        else:
            logger.debug('Schedule is empty')
        self.server = platform.node()
        self.username = os.getlogin()
        self.pid = os.getpid()
        self.start_date = dt.datetime.now()
        self.status = 'W'
        conn = db.connect()
        table = db.table('rapo_scheduler')
        select = table.select()
        result = conn.execute(select).first()
        if result is not None and result.stop_date is None:
            pid = int(result.pid)
            raise AttributeError(f'Scheduler already running at PID {pid}')
        delete = table.delete()
        conn.execute(delete)
        insert = table.insert().values(server=self.server,
                                       username=self.username,
                                       pid=self.pid,
                                       start_date=self.start_date,
                                       status=self.status)
        result = conn.execute(insert)
        pk = int(result.inserted_primary_key[0])
        logger.debug(f'Scheduler owns id {pk}')
        logger.info(f'Scheduler started at PID {self.pid}')
        self._run()
        pass

    def _stop(self):
        self.__stop()
        self._kill()
        pass

    def __stop(self):
        conn = db.connect()
        table = db.table('rapo_scheduler')
        select = table.select()
        result = conn.execute(select).first()
        self.pid = int(result.pid)
        self.stop_date = dt.datetime.now()
        self.status = 'S'
        update = table.update().values(stop_date=self.stop_date,
                                       status=self.status)
        conn.execute(update)
        pass

    def _kill(self):
        try:
            os.kill(self.pid, signal.SIGTERM)
        except OSError:
            raise Warning(f'Scheduler at PID {self.pid} was not found')
        pass

    def _exit(self, signum, frame):
        logger.info('Stopping scheduler...')
        self.__stop()
        logger.info(f'Scheduler at PID {self.pid} stopped')
        self._kill()
        pass

    def _run(self):
        self._synchronize()
        while True:
            self._process()
        pass

    def _synchronize(self):
        logger.debug('Time will be synchronized')
        self.moment = time.time()
        logger.debug('Time was synchronized')
        pass

    def _increment(self):
        self.moment += 1
        pass

    def _process(self):
        try:
            if int(self.moment) % 300 == 0:
                self.schedule = dict(self._sked())
                if self.schedule:
                    logger.debug(f'Schedule: {self.schedule}')
                else:
                    logger.debug('Schedule is empty')
        except Exception:
            logger.error()
        now = time.localtime(self.moment)
        for name, params in self.schedule.items():
            try:
                if (params['status'] is True and
                    self._check(params['mday'], now.tm_mday) is True and
                    self._check(params['wday'], now.tm_wday+1) is True and
                    self._check(params['hour'], now.tm_hour) is True and
                    self._check(params['min'], now.tm_min) is True and
                    self._check(params['sec'], now.tm_sec) is True):
                    self._register(name, self.moment)
            except Exception:
                logger.error()
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
        pass

    def _sked(self):
        logger.debug('Getting schedule...')
        conn = db.connect()
        table = db.table('rapo_config')
        select = table.select()
        result = conn.execute(select)
        for row in result:
            try:
                name = row.control_name
                status = True if row.status == 'Y' else False
                schedule = row.schedule
                schedule = {} if schedule is None else json.loads(schedule)
                mday = schedule.get('mday')
                wday = schedule.get('wday')
                hour = schedule.get('hour')
                min = schedule.get('min')
                sec = schedule.get('sec')
            except Exception:
                logger.warning()
                continue
            else:
                yield name, {'status': status, 'mday': mday, 'wday': wday,
                             'hour': hour, 'min': min, 'sec': sec}
        logger.debug('Schedule retrieved')

    def _match(self):
        pass

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
        pass

    def _execute(self):
        while True:
            if self.queue.empty() is False:
                name, moment = self.queue.get()
                logger.info(f'Initiating control {name}[{moment}]...')
                try:
                    control = Control(name, timestamp=moment)
                    control.run()
                except Exception:
                    logger.error()
                else:
                    self.queue.task_done()
                    logger.info(f'Control {name}[{moment}] performed')
            time.sleep(1)
        pass
