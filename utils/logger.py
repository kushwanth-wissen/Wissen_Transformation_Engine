import logging
import os
import sys
import io
from datetime import datetime

class Logger:
    def __init__(self, name="DynamicTransformationEngine"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)

        # Create formatters
        file_formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)-5s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_formatter = logging.Formatter(
            '%(message)s'
        )

        # Only add handlers once to avoid duplicate logs on repeated instantiation
        if not self.logger.handlers:
            # File handler (explicitly use UTF-8 encoding)
            file_handler = logging.FileHandler('logs/app.log', encoding='utf-8')
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

            # Console handler: wrap stdout in a UTF-8 text wrapper with replace errors
            # to avoid UnicodeEncodeError on Windows consoles that use legacy encodings.
            try:
                console_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            except Exception:
                # Fallback to sys.stdout if wrapping isn't supported
                console_stream = sys.stdout

            console_handler = logging.StreamHandler(stream=console_stream)
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
    
    def print_header(self, project_name, stage, source_type, input_dir):
        separator = "═" * 63
        self.logger.info(f"\n{separator}")
        self.logger.info(f"🏗️  PROJECT          : {project_name}")
        self.logger.info(f"🧩  PIPELINE STAGE   : {stage}")
        self.logger.info(f"🕒  START TIME       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📄  SOURCE TYPE      : {source_type}")
        self.logger.info(f"📁  INPUT DIRECTORY  : {input_dir}")
        self.logger.info(f"{separator}\n")
    
    def print_section(self, title, data):
        separator = "─" * 63
        self.logger.info(f"🔹 [{title}]")
        for key, value in data.items():
            prefix = "✅ " if str(value).lower() == "success" else ""
            self.logger.info(f"     → {key:<18}: {prefix}{value}")
        self.logger.info("\n" + separator + "\n")
    
    def print_summary(self, data):
        separator = "═" * 63
        self.logger.info(separator)
        for key, value in data.items():
            prefix = ""
            if key == "PIPELINE STATUS":
                prefix = "✅ " if str(value).lower() == "success" else "❌ "
            elif key == "FINAL OUTPUT":
                prefix = "📦 "
            elif key == "TOTAL DURATION":
                prefix = "⏱️  "
            self.logger.info(f"{prefix}{key}: {value}")
        self.logger.info("\n" + separator)

    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)

    def exception(self, message):
        """Log an error message with the current exception traceback."""
        # Use logger.exception to include stack trace
        self.logger.exception(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def debug(self, message):
        self.logger.debug(message)