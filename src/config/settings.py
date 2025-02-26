from dataclasses import dataclass, field
from typing import List
import os
from datetime import timedelta


@dataclass
class MongoConfig:
    """MongoDB configuration settings"""
    uri: str = field(
        default_factory=lambda: os.getenv(
            "MONGO_URI",
            "mongodb://localhost:27017"
        )
    )
    max_pool_size: int = 100
    min_pool_size: int = 10
    # Add connection timeout settings
    connect_timeout_ms: int = 5000
    server_selection_timeout_ms: int = 5000


@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    bootstrap_servers: List[str] = field(
        default_factory=lambda: os.getenv(
            "KAFKA_SERVERS",
            "localhost:9092"
        ).split(",")
    )
    topic: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_TOPIC",
            "cloud-tasks"
        )
    )
    group_id: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_GROUP_ID",
            "cloud-task-processor"
        )
    )
    # Add Kafka-specific settings
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000


@dataclass
class RetryConfig:
    """Retry configuration for operations"""
    max_retries: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 10.0  # seconds
    exponential_base: float = 2.0


@dataclass
class AppConfig:
    """Main application configuration"""
    # Database and messaging configurations
    mongo: MongoConfig = field(default_factory=MongoConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Application settings
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )
    consumer_threads: int = field(
        default_factory=lambda: int(os.getenv("CONSUMER_THREADS", "4"))
    )
    scheduler_interval: int = field(
        default_factory=lambda: int(os.getenv("SCHEDULER_INTERVAL", "3600"))
    )

    # Task-specific settings
    task_timeout: timedelta = timedelta(hours=1)
    max_task_retries: int = 3

    def __post_init__(self):
        """Validate configuration after initialization"""
        self.validate_config()

    def validate_config(self):
        """Validate configuration values"""
        if self.consumer_threads < 1:
            raise ValueError("consumer_threads must be at least 1")

        if self.scheduler_interval < 60:
            raise ValueError("scheduler_interval must be at least 60 seconds")

        if not self.mongo.uri:
            raise ValueError("MongoDB URI must be provided")

        if not self.kafka.bootstrap_servers:
            raise ValueError("Kafka bootstrap servers must be configured correctly")