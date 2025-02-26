// Enhanced MongoDB Initialization Script for Cloud Monitoring
// Focuses exclusively on cloud_scan tasks with comprehensive history
// Each task has exactly 5 history entries showing its complete lifecycle

print('Starting MongoDB initialization process...');

// Use admin database for initial setup
db = db.getSiblingDB('admin');

try {
    // Create admin user with comprehensive permissions
    if (db.getUser("admin") == null) {
        db.createUser({
            user: "admin",
            pwd: "strongAdminPassword!23",
            roles: [
                { role: "root", db: "admin" },
                { role: "userAdminAnyDatabase", db: "admin" },
                { role: "readWriteAnyDatabase", db: "admin" }
            ]
        });
        print('Successfully created admin user');
    }

    // New tenant IDs with a more memorable pattern
    const tenantIds = [
        "tenant-alpha-prod-01234567890",
        "tenant-beta-prod-12345678901",
        "tenant-gamma-dev-23456789012",
        "tenant-delta-staging-3456789",
        "tenant-epsilon-test-456789012"
    ];

    // Comprehensive cloud provider and region mapping
    const providers = {
        'AWS': ['us-west-1', 'us-east-1', 'eu-west-1', 'ap-southeast-1'],
        'Azure': ['eastus', 'westeurope', 'southeastasia', 'ukwest'],
        'GCP': ['us-central1', 'europe-west1', 'asia-east1']
    };

    // Utility functions for data generation
    const utils = {
        generateObjectId: () => new ObjectId(),
        randomElement: (array) => array[Math.floor(Math.random() * array.length)],
        addHours: (date, hours) => new Date(date.getTime() + hours * 60 * 60 * 1000),
        generateAccountId: () => Math.floor(Math.random() * 1000000000000).toString(),
        generateResourceId: (prefix) => `${prefix}-${Math.random().toString(36).substring(7)}`
    };

    // Base timestamp for consistent data generation
    const baseDate = new Date("2025-02-14T13:00:00Z");

    // Only using cloud_scan type
    const taskType = "cloud_scan";
    const priorities = ["low", "medium", "high", "critical"];

    // Define the standard 5-stage lifecycle for tasks
    const taskLifecycles = {
        // Complete task lifecycle with successful completion
        "DONE": [
            {status: "scheduled", details: {reason: "Scheduled by system", priority_level: ""}},
            {status: "pending", details: {queue_position: "In processing queue", estimated_start: ""}},
            {status: "processing", details: {worker_id: "", progress_percent: 25}},
            {status: "processing", details: {worker_id: "", progress_percent: 75}},
            {status: "DONE", details: {results: "", resources_processed: 0}}
        ],
        // Complete task lifecycle with error
        "ERROR": [
            {status: "scheduled", details: {reason: "Scheduled by system", priority_level: ""}},
            {status: "pending", details: {queue_position: "In processing queue", estimated_start: ""}},
            {status: "processing", details: {worker_id: "", progress_percent: 25}},
            {status: "processing", details: {worker_id: "", progress_percent: 60}},
            {status: "ERROR", details: {error: "", error_code: ""}}
        ],
        // Incomplete task lifecycle (still processing)
        "processing": [
            {status: "scheduled", details: {reason: "Scheduled by system", priority_level: ""}},
            {status: "pending", details: {queue_position: "In processing queue", estimated_start: ""}},
            {status: "processing", details: {worker_id: "", progress_percent: 25}},
            {status: "processing", details: {worker_id: "", progress_percent: 50}},
            {status: "processing", details: {worker_id: "", progress_percent: 75}}
        ]
    };

    // Success result templates for cloud_scan tasks
    const successResults = [
        { results: "Cloud scan completed successfully", resources_processed: 15 },
        { results: "All cloud resources validated", resources_processed: 23 },
        { results: "Infrastructure scan completed", resources_processed: 42 },
        { results: "Cloud resources inventory completed", resources_processed: 31 },
        { results: "Environment scan finished", resources_processed: 19 }
    ];

    // Error result templates for cloud_scan tasks
    const errorResults = [
        { error: "Resource access denied", error_code: "AUTH_403" },
        { error: "API rate limit exceeded", error_code: "RATE_429" },
        { error: "Cloud provider service unavailable", error_code: "SVC_503" },
        { error: "Invalid cloud configuration", error_code: "CONFIG_400" },
        { error: "Cloud provider timeout", error_code: "TIMEOUT_408" }
    ];

    // Tenant and data initialization
    tenantIds.forEach((tenantId) => {
        print(`Initializing database for tenant: ${tenantId}`);
        const tenantDb = db.getSiblingDB(tenantId);

        // Safely drop and recreate collections
        ['clouds', 'tasks', 'task_history'].forEach(collection => {
            try {
                tenantDb[collection].drop();
            } catch(e) {
                print(`Collection ${collection} might not exist. Continuing...`);
            }
        });

        // Generate clouds for this tenant
        const cloudCount = Math.floor(Math.random() * 3) + 2; // 2-4 clouds per tenant
        const generatedClouds = [];

        for (let i = 0; i < cloudCount; i++) {
            const provider = utils.randomElement(Object.keys(providers));
            const region = utils.randomElement(providers[provider]);
            const cloudId = utils.generateObjectId();

            const cloudDocument = {
                _id: cloudId,
                name: `${provider} ${region} Environment ${i + 1}`,
                provider: provider,
                region: region,
                status: "ready",
                created_at: utils.addHours(baseDate, -24 * (i + 1)),
                last_scan: utils.addHours(baseDate, -12),
                next_scan: utils.addHours(baseDate, 1)
            };

            tenantDb.clouds.insertOne(cloudDocument);
            generatedClouds.push(cloudId);
        }

        // Task generation for each cloud
        generatedClouds.forEach(cloudId => {
            // Generate 4-6 tasks per cloud for variety
            const taskCount = Math.floor(Math.random() * 3) + 4;

            for (let j = 0; j < taskCount; j++) {
                const priority = utils.randomElement(priorities);
                const taskId = utils.generateObjectId();

                // Determine task status - mix of done, error, processing
                let taskStatus;
                if (j % 3 === 0) {
                    taskStatus = "DONE";
                } else if (j % 3 === 1) {
                    taskStatus = "ERROR";
                } else {
                    taskStatus = "processing";
                }

                // Calculate the base creation time for this task
                // Stagger task creation times to create a realistic timeline
                const createdAt = utils.addHours(baseDate, -24 + (j * 3));

                // Calculate additional timestamps based on the task's lifecycle stage
                const scheduledFor = utils.addHours(createdAt, 1);
                const startedAt = utils.addHours(createdAt, 2);

                // Completed time only for DONE or ERROR tasks
                const completedAt = taskStatus !== "processing"
                    ? utils.addHours(createdAt, 6)  // 6 hours after creation
                    : null;

                // Create the task document
                const taskDocument = {
                    _id: taskId,
                    cloud_id: cloudId,
                    type: taskType,  // Always "cloud_scan"
                    status: taskStatus,
                    priority: priority,
                    created_at: createdAt,
                    scheduled_for: scheduledFor,
                    started_at: startedAt,
                    completed_at: completedAt,
                    metadata: {
                        scan_type: "full_inventory",
                        resources_scanned: ["ec2", "s3", "rds", "lambda", "vpc"]
                    }
                };

                tenantDb.tasks.insertOne(taskDocument);

                // Get the lifecycle stages for this task type
                const lifecycle = taskLifecycles[taskStatus];

                // Generate exactly 5 history entries for this task
                const historyEntries = [];

                for (let k = 0; k < 5; k++) {
                    // Get the status and base details template for this stage
                    const stageInfo = lifecycle[k];
                    const statusType = stageInfo.status;

                    // Clone the details template to avoid modifying the original
                    let details = JSON.parse(JSON.stringify(stageInfo.details));

                    // Calculate timestamp based on the creation time and stage
                    // First entry at creation time, subsequent entries spaced 1 hour apart
                    const historyTime = utils.addHours(createdAt, k);

                    // Enrich details with stage-specific information
                    if (statusType === "scheduled") {
                        details.priority_level = priority;
                        details.scheduled_by = "system_scheduler";
                    } else if (statusType === "pending") {
                        details.estimated_start = utils.addHours(historyTime, 1).toISOString();
                        details.queued_behind = Math.floor(Math.random() * 5);
                    } else if (statusType === "processing") {
                        details.worker_id = `worker-${Math.random().toString(36).substring(7)}`;
                        // Progress increases with each processing stage
                        details.estimated_completion = utils.addHours(historyTime, 2).toISOString();
                    } else if (statusType === "DONE") {
                        // Add success details from templates
                        const success = utils.randomElement(successResults);
                        details.results = success.results;
                        details.resources_processed = success.resources_processed;
                        details.execution_time_ms = Math.floor(Math.random() * 5000) + 1000;
                    } else if (statusType === "ERROR") {
                        // Add error details from templates
                        const error = utils.randomElement(errorResults);
                        details.error = error.error;
                        details.error_code = error.error_code;
                        details.attempted_retries = Math.floor(Math.random() * 3);
                    }

                    // Create the history entry document
                    const historyEntry = {
                        _id: utils.generateObjectId(),
                        task_id: taskId,
                        cloud_id: cloudId,
                        status: statusType,
                        timestamp: historyTime,
                        duration_ms: k > 0 ? Math.floor(Math.random() * 3600000) + 600000 : 0, // Random duration between 10min-1hr
                        details: details
                    };

                    historyEntries.push(historyEntry);
                }

                // Insert all 5 history entries for this task
                tenantDb.task_history.insertMany(historyEntries);
                print(`Created cloud_scan task ${taskId} with exactly 5 history entries for cloud ${cloudId}`);
            }
        });

        // Create indexes for better query performance
        tenantDb.tasks.createIndex({ "cloud_id": 1 });
        tenantDb.tasks.createIndex({ "status": 1 });
        tenantDb.tasks.createIndex({ "created_at": -1 });
        tenantDb.tasks.createIndex({ "type": 1, "status": 1 });
        tenantDb.tasks.createIndex({ "type": 1, "scheduled_for": 1 });

        tenantDb.task_history.createIndex({ "task_id": 1, "timestamp": -1 });
        tenantDb.task_history.createIndex({ "cloud_id": 1 });
        tenantDb.task_history.createIndex({ "status": 1, "timestamp": -1 });

        tenantDb.clouds.createIndex({ "status": 1 });
        tenantDb.clouds.createIndex({ "provider": 1 });

        print(`Successfully initialized tenant: ${tenantId} with indexes`);
    });

    print('MongoDB initialization completed successfully');

} catch (error) {
    print('Critical Error during MongoDB initialization:');
    printjson(error);
    throw error;
}