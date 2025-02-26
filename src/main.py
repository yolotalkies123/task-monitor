import threading
import uvicorn
import logging
from config.settings import AppConfig
from core.scheduler import CloudTaskScheduler
from core.consumer import TaskConsumer
from core.monitoring import create_app  # Change this import
import nest_asyncio
import asyncio
from datetime import datetime
import pytz

# Apply nest_asyncio to allow nested event loops in Jupyter-like environments
nest_asyncio.apply()


class ApplicationManager:
    def __init__(self):
        self.logger = None
        self.config = None
        self.scheduler = None
        self.consumer = None
        self.is_running = True
        self.startup_time = datetime.now(pytz.UTC)

        # Create the app in the constructor
        self.app = create_app()  # Use the factory function to create the app

    def initialize(self):
        """Initialize application components with robust error handling"""
        try:
            # Set up basic logging first as fallback
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y.%m.%d T %H:%M'
            )
            self.logger = logging.getLogger('cloud-monitor')

            # Initialize configuration
            self.config = AppConfig()

            # Set up enhanced logging with our custom formatter
            try:
                self.logger = setup_logging(
                    'cloud-monitor',
                    self.config.log_level,
                    time_format="%Y.%m.%d T %H:%M"
                )
            except Exception as e:
                self.logger.warning(
                    f"Failed to set up enhanced logging: {str(e)}. Using basic logging."
                )

            self.logger.info(
                "Initializing Cloud Task Monitoring System",
                extra={
                    'startup_time': self.startup_time.strftime("%Y.%m.%d T %H:%M")
                }
            )

            # Make configuration available to FastAPI app
            self.app.state.config = self.config
            self.app.state.startup_time = self.startup_time

        except Exception as e:
            if not self.logger:
                logging.basicConfig(level=logging.ERROR)
                self.logger = logging.getLogger('cloud-monitor')
            self.logger.error(f"Initialization error: {str(e)}", exc_info=True)
            raise

    async def setup_scheduler(self):
        """Initialize and start the task scheduler"""
        try:
            self.scheduler = CloudTaskScheduler(self.config)
            # Create event loop for scheduler thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # Start the scheduler
            self.scheduler.start()
            self.logger.info(
                "Task scheduler started successfully",
                extra={'component': 'scheduler'}
            )
        except Exception as e:
            self.logger.error(
                f"Failed to start scheduler: {str(e)}",
                extra={'component': 'scheduler'},
                exc_info=True
            )
            raise

    def start_scheduler(self):
        """Start scheduler in a dedicated thread"""
        asyncio.run(self.setup_scheduler())

    def start_api_server(self):
        """Start the FastAPI server with proper logging"""
        config = uvicorn.Config(
            self.app,  # Use self.app instead of the imported app
            host="0.0.0.0",
            port=8080,
            log_level=self.config.log_level.lower(),
            access_log=True,
            log_config={
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "()": "uvicorn.logging.DefaultFormatter",
                        "fmt": "%(asctime)s [%(levelname)s] %(message)s",
                        "datefmt": "%Y.%m.%d T %H:%M",
                        "use_colors": None
                    },
                    "access": {
                        "()": "uvicorn.logging.AccessFormatter",
                        "fmt": '%(asctime)s [%(levelname)s] %(client_addr)s - "%(request_line)s" %(status_code)s',
                        "datefmt": "%Y.%m.%d T %H:%M"
                    },
                },
                "handlers": {
                    "default": {
                        "formatter": "default",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stdout"
                    },
                    "access": {
                        "formatter": "access",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stdout"
                    },
                },
                "loggers": {
                    "": {"handlers": ["default"], "level": "INFO"},
                    "uvicorn.error": {"level": "INFO"},
                    "uvicorn.access": {
                        "handlers": ["access"],
                        "level": "INFO",
                        "propagate": False
                    },
                },
            }
        )
        server = uvicorn.Server(config)
        server.run()

    async def cleanup(self):
        """Cleanup resources with proper error handling"""
        try:
            self.is_running = False

            if self.scheduler:
                self.logger.info("Stopping scheduler...")
                self.scheduler.shutdown()

            if self.consumer:
                self.logger.info("Cleaning up consumer...")
                await self.consumer.cleanup()

            self.logger.info(
                "Cleanup completed successfully",
                extra={
                    'shutdown_time': datetime.now(pytz.UTC).strftime("%Y.%m.%d T %H:%M")
                }
            )

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}", exc_info=True)


async def run_application():
    """Main application runner with comprehensive error handling"""
    app_manager = ApplicationManager()

    try:
        # Initialize all components
        app_manager.initialize()

        # Start API server in a separate thread
        api_thread = threading.Thread(
            target=app_manager.start_api_server,
            daemon=True
        )
        api_thread.start()

        # Start scheduler in a separate thread
        scheduler_thread = threading.Thread(
            target=app_manager.start_scheduler,
            daemon=True
        )
        scheduler_thread.start()

        # Initialize and run consumer
        app_manager.consumer = TaskConsumer(app_manager.config)
        await app_manager.consumer.run()

    except Exception as e:
        if hasattr(app_manager, 'logger') and app_manager.logger:
            app_manager.logger.error(
                f"Application error: {str(e)}",
                exc_info=True,
                extra={
                    'error_time': datetime.now(pytz.UTC).strftime("%Y.%m.%d T %H:%M")
                }
            )
        else:
            logging.error(f"Fatal error before logger initialization: {str(e)}")
    finally:
        await app_manager.cleanup()


def main():
    """Application entry point with signal handling"""
    try:
        asyncio.run(run_application())
    except KeyboardInterrupt:
        logging.info("Received shutdown signal")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
    finally:
        logging.info("Application shutdown complete")


if __name__ == "__main__":
    main()