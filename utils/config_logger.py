import os
from dataclasses import dataclass
import logging
import sys


@dataclass
class BaseLogger:
    """Базовый класс для создания логгеров"""
    path: str = None
    current_logger: object = None
    client_formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(message)s')
    logging_level = logging.DEBUG

    def create_stream(self, log_path=None):
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(self.client_formatter)
        stream.setLevel(logging.INFO)
        log_file = logging.FileHandler(log_path, encoding='utf8')
        log_file.setFormatter(self.client_formatter)

        # создаём регистратор и настраиваем его
        logger = logging.getLogger('etl_client')
        logger.addHandler(stream)
        logger.addHandler(log_file)
        logger.setLevel(self.logging_level)

        # if platform.system() == 'Linux':
        #     logger = LoggingMixin().log

        self.current_logger = logger

        return self.current_logger

