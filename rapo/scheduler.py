import configparser
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import sqlalchemy as sql
import threading
import time
import queue

from .config import config
from .logger import logger
from .database import db

from .control import Control


class Scheduler():
    def __init__(self):
        self.moment = None
        self.delay = None
        self.schedule = None
        self.queue = queue.Queue()
        self.executors = []
        for i in range(5):
            thread = threading.Thread(name=f'Thread-Executor-{i}',
                                      target=self.execute, daemon=True)
            thread.start()
            self.executors.append(thread)

        self.server = None
        self.username = None
        self.pid = None
        self.start_date = None
        self.stop_date = None
        self.status = None

        if len(logger.sysinfo.anons) > 0:
            arg = logger.sysinfo.anons[0]
            if arg == 'start':
                exe = 'pythonw'
                file = os.path.abspath(sys.argv[0])
                args = '--start'
                command = [exe, file, args]
                logger.debug(f'command={command}')
                subprocess.Popen(command)
            elif arg == 'stop':
                self.stop()
            else:
                logger.sysinfo.add('--start', required=False,
                                   action='store_true')
                logger.sysinfo.add('--stop', required=False,
                                   action='store_true')
                if logger.sysinfo.args.start is True:
                    self.start()
                elif logger.sysinfo.args.stop is True:
                    self.stop()
            sys.exit()
        pass

    def start(self):
        try:
            signal.signal(signal.SIGINT, self.exit)
            logger.info('Starting scheduler...')
            self.schedule = dict(self.sked())
            logger.debug(f'Schedule: {self.schedule}')
            self.server = logger.sysinfo.desc.hostname
            self.username = logger.sysinfo.desc.user
            self.pid = logger.sysinfo.desc.pid
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
            logger.debug(f'PRIMARY KEY RAPO_SCHEDULER.ID={pk}')
            logger.info(f'Scheduler started at PID {self.pid}')
            self.run()
        except:
            logger.error()
        pass

    def stop(self):
        try:
            logger.info('Stopping scheduler...')
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
            logger.info(f'Scheduler at PID {self.pid} stopped')
        except:
            logger.error()
        else:
            self.kill()
        pass

    def run(self):
        self.synchronize()
        while True:
            self.process()
        pass

    def synchronize(self):
        logger.debug('Time will be synchronized')
        self.moment = time.time()
        logger.debug('Time was synchronized')
        pass

    def increment(self):
        self.moment += 1
        pass

    def process(self):
        try:
            if int(self.moment) % 300 == 0:
                self.schedule = dict(self.sked())
                logger.debug(f'Schedule: {self.schedule}')
        except:
            logger.error()
        now = time.localtime(self.moment)
        for name, params in self.schedule.items():
            try:
                if (params['status'] is True and
                    self.check(params['mday'], now.tm_mday) is True and
                    self.check(params['wday'], now.tm_wday+1) is True and
                    self.check(params['hour'], now.tm_hour) is True and
                    self.check(params['min'], now.tm_min) is True and
                    self.check(params['sec'], now.tm_sec) is True):
                        self.register(name, self.moment)
            except:
                logger.error()
        delay = time.time()-self.moment
        wait = 1-delay
        try:
            time.sleep(wait)
        except ValueError:
            logger.warning('TIME IS BROKEN')
            self.synchronize()
        else:
            logger.debug(f'moment={self.moment}, delay={delay}, wait={wait}')
            self.increment()
        pass

    def sked(self):
        logger.debug('Getting schedule...')
        conn = db.connect()
        table = db.table('rapo_config')
        select = table.select()
        result = conn.execute(select)
        for row in result:
            name = row.control_name
            status = True if row.status == 'Y' else False
            schedule = {} if row.schedule is None else json.loads(row.schedule)
            mday = schedule.get('mday')
            wday = schedule.get('wday')
            hour = schedule.get('hour')
            min = schedule.get('min')
            sec = schedule.get('sec')
            yield name, {'status': status, 'mday': mday, 'wday': wday,
                         'hour': hour, 'min': min, 'sec': sec}
        logger.debug('Schedule retrieved')

    def match(self):
        pass

    def check(self, unit, now):
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
            if unit == 0: return False
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

    def register(self, name, moment):
        try:
            logger.info(f'Adding control {name}[{moment}] to queue...')
            self.queue.put((name, moment))
        except:
            logger.error()
        else:
            logger.info(f'Control {name}[{moment}] was added to queue')
        pass

    def execute(self):
        while True:
            if self.queue.empty() is False:
                name, moment = self.queue.get()
                logger.info(f'Initiating control {name}[{moment}]...')
                try:
                    control = Control(name, trigger=moment)
                    control.run()
                except:
                    logger.error()
                else:
                    self.queue.task_done()
                    logger.info(f'Control {name}[{moment}] performed')
            time.sleep(1)
        pass

    def kill(self):
        try:
            os.kill(self.pid, signal.SIGINT)
        except OSError:
            logger.warning(f'Scheduler at PID {self.pid} was not found')
        pass

    def exit(self, signum, frame):
        self.stop()
        pass
