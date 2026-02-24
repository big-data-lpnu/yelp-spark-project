import logging
from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    """Route log messages through tqdm.write() to avoid corrupting progress bars."""
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
        except Exception:
            self.handleError(record)
