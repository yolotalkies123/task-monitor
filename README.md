**System Architecture:**

**Main Process**
├── API Server Thread (FastAPI)
├── Scheduler Thread (Creates tasks)
└── Main Thread (Kafka Consumer)


**Data flow:**

-Scheduler creates tasks and sends to Kafka
-Consumer processes tasks from Kafka
-API server provides monitoring endpoints
-All components share MongoDB for state

**Stages**
When you start the system using docker-compose up, several things will happen in sequence:
1.First Stage - Infrastructure Setup:

MongoDB will start first, and the mongo-init.js script will execute. We will see logs showing:

"Starting MongoDB initialization process..."
Messages for each tenant database being created
Collection creation confirmations
Index creation notifications
Sample data insertion progress
"MongoDB initialization completed successfully"



2.Second Stage - Supporting Services:

Mongo Express will start after MongoDB is healthy, providing a web interface at http://localhost:8081 where you can browse the created databases and collections. You'll need to use the credentials:

Username: dev
Password: dev


ZooKeeper will initialize, followed by Kafka. The Kafka UI will become available at http://localhost:8082, allowing you to monitor:

Topic creation
Message flow
Consumer group status



3.Third Stage - Main Application:
The cloud-monitor service will start last, and you'll see several components initializing:

The Scheduler will begin running and you'll see logs like:

"Task scheduler started successfully"
Periodic scheduling messages for each tenant
Task creation notifications


The Consumer will start processing tasks, generating logs such as:

Task processing status updates
Completion notifications
Any error messages


The API Server will become available at http://localhost:8080, where you can:

Access /tasks/{tenant_id} endpoints
View task monitoring data


**Expected Output Examples:**
When you call the API for a specific tenant (e.g., http://localhost:8080/tasks/039228b4-1080-4e8e-9aa5-b16c21184b0c), you'll see output like:
Copy039228b4-1080-4e8e-9aa5-b16c21184b0c > cloud scan
* next scheduled at : 2025.02.15 T 13:00
* previous task done : 2025.02.01 T 13:00 > status: DONE
* previous task done : 2025.02.01 T 12:00 > status: ERROR
.....


Task Status Progression:

Tasks will move from "scheduled" to "processing" to either "DONE" or "ERROR"
You'll see different colored status indicators (green for DONE, red for ERROR)


Timing Patterns:

Tasks are scheduled hourly
Each tenant may have multiple tasks in different states
Timestamps will follow the format YYYY.MM.DD T HH:MM


Database Growth:

The task_history collection will continuously grow as tasks complete
New tasks will be created by the scheduler
You can monitor this through Mongo Express



**Challenges expected here:**

Initial Startup Timing:

Services need to start in the correct order
You might see some connection retry messages while services are starting
The system should stabilize within 1-2 minutes


Resource Usage:

With 10 tenants and multiple tasks per tenant, expect moderate database activity
Monitor container logs for any performance issues
The system is designed to handle this load, but watch for any warning messages


Data Consistency:

Check that timestamps are in UTC
Verify that task status colors are displaying correctly
Confirm that task history is being recorded properly# cloud-monitor
# cloud-monitor
