"""Python Revenue Assurance Process Optimizer."""

from .core.scheduler import Scheduler
from .core.control import Control
from .web import Server


__author__ = 'Timur Faradzhov'
__copyright__ = 'Copyright 2025, The Rapo project'
__credits__ = ['Timur Faradzhov', 'Kostadin Taneski']

__license__ = 'MIT'
__version__ = '0.6.8'
__maintainer__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__status__ = 'Development'

__all__ = [Scheduler, Control, Server]
