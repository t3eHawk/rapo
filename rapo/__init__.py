"""Python Revenue Assurance Process Optimizer."""

from .main.scheduler import Scheduler
from .main.control import Control
from .web import WEBAPI


__author__ = 'Timur Faradzhov'
__copyright__ = 'Copyright 2023, The RAPO project'
__credits__ = ['Timur Faradzhov', 'Kostadin Taneski']

__license__ = 'MIT'
__version__ = '0.4.4'
__maintainer__ = 'Timur Faradzhov'
__email__ = 'timurfaradzhov@gmail.com'
__status__ = 'Development'

__all__ = [Scheduler, Control, WEBAPI]
