from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from kafka import KafkaProducer
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import json
import logging
from datetime import datetime, timedelta
import pytz
from utils.error_handler import error_handler, TaskError


class CloudTaskScheduler:
    """Manages scheduling of cloud scanning tasks"""

    def __init__(self, config: 'AppConfig'):
        """Initialize scheduler components"""
        self.config = config
        self.logger = logging.getLogger('scheduler')

        # Set up MongoDB connection
        self.mongo_client = MongoClient(config.mongo.uri)

        # Configure Kafka producer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Add retry configuration for better reliability
            retries=5,
            retry_backoff_ms=1000
        )

        # Initialize the AsyncIOScheduler
        self.scheduler = AsyncIOScheduler(
            timezone=pytz.UTC,
            job_defaults={
                'coalesce': True,  # Combine multiple pending executions
                'max_instances': 1  # Prevent concurrent executions
            }
        )
        self.is_running = False

    def get_tenant_databases(self) -> list:
        """Retrieve all tenant database names, excluding system DBs"""
        try:
            return [db for db in self.mongo_client.list_database_names()
                    if not db.startswith('system')]
        except PyMongoError as e:
            self.logger.error(f"Failed to get tenant databases: {str(e)}")
            raise

    @error_handler(logging.getLogger('scheduler'))
    async def schedule_tenant_tasks(self):
        """Schedule tasks for all tenants with ready clouds"""
        try:
            for tenant_db in self.get_tenant_databases():
                db = self.mongo_client[tenant_db]

                # Find clouds ready for scanning
                ready_clouds = db.clouds.find({"status": "ready"})

                for cloud in ready_clouds:
                    current_time = datetime.now(pytz.UTC)
                    # Schedule for the next hour boundary
                    next_run = current_time.replace(
                        minute=0, second=0, microsecond=0
                    ) + timedelta(hours=1)

                    # Create task record with improved metadata
                    task = {
                        "cloud_id": str(cloud["_id"]),
                        "type": "cloud_scan",
                        "status": "scheduled",
                        "priority": "normal",
                        "next_scheduled": next_run,
                        "created_at": current_time,
                        "metadata": {
                            "cloud_provider": cloud.get("provider"),
                            "cloud_region": cloud.get("region"),
                            "schedule_source": "automatic"
                        }
                    }

                    # Insert task and get ID
                    task_id = str(db.tasks.insert_one(task).inserted_id)

                    # Prepare Kafka message
                    kafka_message = {
                        "tenant_db": tenant_db,
                        "task_id": task_id,
                        "cloud_id": str(cloud["_id"]),
                        "scheduled_time": next_run.isoformat(),
                        "priority": task["priority"]
                    }

                    # Send to Kafka with delivery confirmation
                    future = self.kafka_producer.send(
                        self.config.kafka.topic,
                        kafka_message
                    )
                    future.get(timeout=10)  # Wait for confirmation

                    self.logger.info(
                        f"Scheduled task for tenant {tenant_db}",
                        extra={
                            'tenant_id': tenant_db,
                            'task_id': task_id,
                            'cloud_id': str(cloud["_id"]),
                            'scheduled_time': next_run.isoformat()
                        }
                    )

        except Exception as e:
            self.logger.error(f"Error in scheduler: {str(e)}", exc_info=True)
            raise

    def start(self):
        """Start the scheduler with proper error handling"""
        try:
            if not self.is_running:
                # Add the scheduling job with improved configuration
                self.scheduler.add_job(
                    self.schedule_tenant_tasks,
                    IntervalTrigger(
                        seconds=self.config.scheduler_interval,
                        timezone=pytz.UTC
                    ),
                    name='tenant_task_scheduler',
                    id='tenant_task_scheduler',
                    replace_existing=True,
                    next_run_time=datetime.now(pytz.UTC)
                )

                # Start the scheduler
                self.scheduler.start()
                self.is_running = True
                self.logger.info(
                    "Scheduler started successfully",
                    extra={'component': 'scheduler'}
                )
        except Exception as e:
            self.logger.error(f"Failed to start scheduler: {str(e)}")
            raise

    def shutdown(self):
        """Shutdown the scheduler gracefully"""
        try:
            if self.is_running:
                # Flush any pending Kafka messages
                self.kafka_producer.flush()

                # Close Kafka producer
                self.kafka_producer.close()

                # Shutdown scheduler
                self.scheduler.shutdown()
                self.is_running = False

                self.logger.info(
                    "Scheduler shutdown completed",
                    extra={'component': 'scheduler'}
                )
        except Exception as e:
            self.logger.error(f"Error during scheduler shutdown: {str(e)}")
            raise