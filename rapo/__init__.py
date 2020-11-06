"""Python Revenue Assurance Process Optimizer."""

from .main.scheduler import Scheduler
from .main.control import Control
from .web import WEBAPI


__author__ = 'Timur Faradzhov'
__copyright__ = 'Copyright 2020, The RAPO project'
__credits__ = ['Timur Faradzhov']

__license__ = 'MIT'
__version__ = '0.3.1'
__maintainer__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__status__ = 'Development'

__all__ = [Scheduler, Control, WEBAPI]
