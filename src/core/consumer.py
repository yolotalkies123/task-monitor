from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime
import pytz
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from utils.error_handler import error_handler, TaskError


class TaskConsumer:
    """Processes cloud scanning tasks from Kafka"""

    def __init__(self, config: 'AppConfig'):
        """Initialize consumer with configuration"""
        self.config = config
        self.logger = logging.getLogger('consumer')
        self.mongo_client = MongoClient(config.mongo.uri)
        self.executor = ThreadPoolExecutor(max_workers=config.consumer_threads)
        self.setup_consumer()

    def setup_consumer(self):
        """Initialize Kafka consumer with improved configuration"""
        self.consumer = KafkaConsumer(
            self.config.kafka.topic,
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            group_id=self.config.kafka.group_id,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: (
                json.loads(m.decode('utf-8')) if isinstance(m, bytes)
                else json.loads(m) if isinstance(m, str)
                else m
            )
        )

    @error_handler(logging.getLogger('consumer'))
    async def process_task(self, task_data: dict):
        """Process a single cloud scanning task"""
        # Ensure task_data is properly deserialized
        if isinstance(task_data, (bytes, str)):
            task_data = (json.loads(task_data.decode('utf-8'))
                         if isinstance(task_data, bytes)
                         else json.loads(task_data))

        tenant_db = task_data['tenant_db']
        task_id = task_data['task_id']

        try:
            db = self.mongo_client[tenant_db]
            current_time = datetime.now(pytz.UTC)

            # Update task status to processing
            await asyncio.to_thread(
                db.tasks.update_one,
                {"_id": task_id},
                {
                    "$set": {
                        "status": "processing",
                        "started_at": current_time,
                        "last_updated": current_time
                    }
                }
            )

            # Simulated task execution
            await asyncio.sleep(2)

            # Update task as completed
            await asyncio.to_thread(
                db.tasks.update_one,
                {"_id": task_id},
                {
                    "$set": {
                        "status": "DONE",
                        "completed_at": current_time,
                        "last_updated": current_time
                    }
                }
            )

            # Add to task history
            await asyncio.to_thread(
                db.task_history.insert_one,
                {
                    "task_id": task_id,
                    "status": "DONE",
                    "timestamp": current_time,
                    "formatted_time": current_time.strftime("%Y.%m.%d T %H:%M")
                }
            )

            self.logger.info(
                f"Task completed successfully",
                extra={
                    'tenant_id': tenant_db,
                    'task_id': task_id,
                    'status': 'DONE'
                }
            )

        except Exception as e:
            error_time = datetime.now(pytz.UTC)
            error_status = "ERROR"
            error_message = str(e)

            try:
                # Update task status to error
                await asyncio.to_thread(
                    db.tasks.update_one,
                    {"_id": task_id},
                    {
                        "$set": {
                            "status": error_status,
                            "error": error_message,
                            "error_time": error_time,
                            "last_updated": error_time
                        }
                    }
                )

                # Add error to task history
                await asyncio.to_thread(
                    db.task_history.insert_one,
                    {
                        "task_id": task_id,
                        "status": error_status,
                        "error": error_message,
                        "timestamp": error_time,
                        "formatted_time": error_time.strftime("%Y.%m.%d T %H:%M")
                    }
                )
            except Exception as inner_e:
                self.logger.error(f"Failed to record error state: {str(inner_e)}")

            raise TaskError(
                message=f"Task processing failed: {error_message}",
                task_id=task_id,
                tenant_id=tenant_db
            )

    async def run(self):
        """Main consumer loop with enhanced error handling"""
        try:
            self.logger.info("Starting task consumer...")

            while True:
                try:
                    messages = self.consumer.poll(timeout_ms=1000)
                    if not messages:
                        await asyncio.sleep(1)  # Avoid busy waiting
                        continue

                    for topic_partition, records in messages.items():
                        for record in records:
                            try:
                                await self.process_task(record.value)
                                self.logger.info(
                                    f"Successfully processed task from {topic_partition}"
                                )
                            except Exception as e:
                                self.logger.error(f"Failed to process task: {str(e)}")

                        # Commit offset after processing batch
                        self.consumer.commit()

                except Exception as e:
                    self.logger.error(f"Error processing message batch: {str(e)}")
                    await asyncio.sleep(1)  # Brief pause before retry

        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}", exc_info=True)
            raise
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=True)
            if hasattr(self, 'consumer'):
                self.consumer.close()
            self.logger.info("Consumer cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during consumer cleanup: {str(e)}")