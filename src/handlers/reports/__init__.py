"""Report handlers; importing the package registers them on dp."""

from . import fb, filters, kpi, period  # noqa: F401
from .kpi import _send_kpi_menu  # noqa: F401
from .period import _send_reports_menu  # noqa: F401
