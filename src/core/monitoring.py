from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
import logging
from bson import ObjectId
import pytz
from utils.error_handler import error_handler
from config.settings import AppConfig
from collections import defaultdict

logger = logging.getLogger(__name__)

class TaskMonitoring:
    def __init__(self, config: AppConfig):
        self.config = config
        self.mongo_client = MongoClient(
            config.mongo.uri,
            maxPoolSize=config.mongo.max_pool_size,
            minPoolSize=config.mongo.min_pool_size
        )

    @error_handler(logger)
    async def get_tenant_tasks(self, tenant_id: str, limit: int = 10) -> dict:
        try:
            db = self.mongo_client[tenant_id]

            history = list(
                db.task_history.find({"status": {"$in": ["ERROR", "DONE"]}})
                .sort('timestamp', -1)
                .limit(limit)
            )

            logger.info(f"Found {len(history)} ERROR/DONE task history entries for tenant {tenant_id}")

            if not history:
                logger.info(f"No ERROR/DONE task history found for tenant {tenant_id}")
                return {
                    "output": [f"{tenant_id} > No ERROR/DONE task history found"],
                    "tenant_id": tenant_id
                }

            task_groups = defaultdict(list)
            for entry in history:
                task_id = entry.get('task_id', 'UNKNOWN')
                task_groups[task_id].append(entry)

            output_lines = [f"{tenant_id} > Recent ERROR/DONE task history"]

            for task_id, entries in task_groups.items():
                output_lines.append(f"Task ID: {task_id}")

                for entry in entries:
                    timestamp = entry['timestamp'].strftime("%Y.%m.%d T %H:%M")
                    status = entry['status']
                    cloud_id = entry.get('cloud_id', 'UNKNOWN')

                    output_lines.append(f"* Cloud ID: {cloud_id}")
                    output_lines.append(f"* Task status: {status}")
                    output_lines.append(f"* Timestamp: {timestamp}")

                    if 'details' in entry:
                        for key, value in entry['details'].items():
                            output_lines.append(f"* {key.capitalize()}: {value}")

                    output_lines.append("---")

                output_lines.append("")

            return {
                "output": output_lines,
                "tenant_id": tenant_id
            }

        except PyMongoError as e:
            logger.error(f"MongoDB error for tenant {tenant_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

def create_app() -> FastAPI:
    app = FastAPI(title="Cloud Task Monitor", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "timestamp": datetime.now(pytz.UTC)}

    @app.get("/tasks/{tenant_id}")
    async def get_tasks(tenant_id: str):
        try:
            config = app.state.config if hasattr(app.state, 'config') else AppConfig()
            monitoring = TaskMonitoring(config)
            result = await monitoring.get_tenant_tasks(tenant_id)
            return result
        except Exception as e:
            logger.error(f"Failed to get tasks for tenant {tenant_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    return app

# Create FastAPI application
app = create_app()