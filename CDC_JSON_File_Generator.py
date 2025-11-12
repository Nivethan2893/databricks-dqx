# Databricks notebook source
# MAGIC %md
# MAGIC # CDC JSON File Generator - Write to Unity Catalog Volume
# MAGIC
# MAGIC ## Purpose
# MAGIC Generate CDC events as **individual JSON files** in a Unity Catalog volume, simulating how Kafka/Event Hub would deliver data.
# MAGIC
# MAGIC ## Workflow
# MAGIC 1. **Phase 1**: Generate initial data (creates, updates) → Write to volume → Stop
# MAGIC 2. **Phase 2**: Generate delete events for some records → Write to volume → Test DLT pipeline
# MAGIC
# MAGIC ## Quick Start
# MAGIC 1. Run **Cmd 2** - Configure volume path
# MAGIC 2. Run **Cmd 3** - Create volume if needed
# MAGIC 3. Run **Cmd 4** - Generate initial data (100 files with creates/updates)
# MAGIC 4. **Stop and configure your DLT pipeline to read from volume**
# MAGIC 5. Run **Cmd 8** - Generate delete events (20 files with deletes)
# MAGIC 6. **Verify deleted records are removed from your final table**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Volume Configuration
CATALOG = "cdigitpoc"
SCHEMA = "0_bronze"
VOLUME = "cdc_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Data Configuration
INITIAL_BATCH_SIZE = 100  # Number of files to generate initially
DELETE_BATCH_SIZE = 20    # Number of delete files to generate later

# Track which customer IDs we created (so we can delete them later)
CUSTOMER_IDS_FILE = f"{VOLUME_PATH}/_metadata/customer_ids.json"

print(f"✅ Configuration:")
print(f"   Volume Path: {VOLUME_PATH}")
print(f"   Initial Batch: {INITIAL_BATCH_SIZE} files")
print(f"   Delete Batch: {DELETE_BATCH_SIZE} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume

# COMMAND ----------

# Create catalog, schema, and volume if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}
    COMMENT 'CDC JSON files for testing DLT pipeline'
""")

print(f"✅ Volume ready: {VOLUME_PATH}")

# List current contents
try:
    files = dbutils.fs.ls(VOLUME_PATH)
    print(f"\n📁 Current files in volume: {len(files)}")
    if len(files) < 20:  # Only show if not too many
        for f in files[:10]:
            print(f"   - {f.name}")
        if len(files) > 10:
            print(f"   ... and {len(files) - 10} more")
except Exception as e:
    print(f"📁 Volume is empty (this is expected on first run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Generate Initial Data (Creates & Updates)
# MAGIC
# MAGIC This generates JSON files with CREATE and UPDATE operations. Each file represents one CDC event, similar to how Kafka delivers messages.

# COMMAND ----------

import json
import random
from datetime import datetime
import uuid

def generate_create_event(customer_id, timestamp_ms):
    """Generate a CREATE event"""
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
    last_names = ["Smith", "Johnson", "Brown", "Davis", "Wilson", "Moore", "Taylor", "Anderson"]
    domains = ["email.com", "test.com", "example.com", "demo.com"]
    
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    email = f"{first_name.lower()}.{last_name.lower()}{customer_id}@{random.choice(domains)}"
    phone = f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    age = random.randint(18, 80)
    
    return {
        "payload": {
            "before": None,
            "after": {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "age": age,
                "created_at": timestamp_ms - random.randint(0, 86400000)
            },
            "source": {
                "table": "customer",
                "schema": "abs",
                "db": "testdb",
                "connector": "mysql"
            },
            "op": "c",
            "ts_ms": timestamp_ms,
            "transaction": None
        }
    }

def generate_update_event(customer_id, timestamp_ms):
    """Generate an UPDATE event"""
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
    last_names = ["Smith", "Johnson", "Brown", "Davis", "Wilson", "Moore", "Taylor", "Anderson"]
    domains = ["email.com", "test.com", "example.com", "demo.com"]
    
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    old_email = f"{first_name.lower()}.old{customer_id}@{random.choice(domains)}"
    new_email = f"{first_name.lower()}.{last_name.lower()}{customer_id}@{random.choice(domains)}"
    phone = f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    age = random.randint(18, 80)
    
    return {
        "payload": {
            "before": {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": old_email,
                "phone": phone,
                "age": age,
                "created_at": timestamp_ms - 86400000
            },
            "after": {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": new_email,
                "phone": phone,
                "age": age + 1,
                "created_at": timestamp_ms - 86400000
            },
            "source": {
                "table": "customer",
                "schema": "abs",
                "db": "testdb",
                "connector": "mysql"
            },
            "op": "u",
            "ts_ms": timestamp_ms,
            "transaction": None
        }
    }

# Generate initial batch
print(f"🔄 Generating {INITIAL_BATCH_SIZE} initial CDC events (creates and updates)...")
current_time_ms = int(datetime.now().timestamp() * 1000)
customer_ids = []

# Create subdirectory for initial data
initial_dir = f"{VOLUME_PATH}/initial_data"
dbutils.fs.mkdirs(initial_dir)

files_created = 0
for i in range(INITIAL_BATCH_SIZE):
    customer_id = 1000 + i  # Sequential IDs for easier tracking
    customer_ids.append(customer_id)
    
    # 70% creates, 30% updates
    if random.random() < 0.7:
        event = generate_create_event(customer_id, current_time_ms + i * 1000)
        op_type = "create"
    else:
        event = generate_update_event(customer_id, current_time_ms + i * 1000)
        op_type = "update"
    
    # Write as JSON file (simulating Kafka message)
    filename = f"{initial_dir}/event_{i:05d}_{op_type}_{customer_id}.json"
    dbutils.fs.put(filename, json.dumps(event), overwrite=True)
    files_created += 1
    
    if (i + 1) % 20 == 0:
        print(f"   Generated {i + 1}/{INITIAL_BATCH_SIZE} files...")

# Save customer IDs for later deletion
metadata_dir = f"{VOLUME_PATH}/_metadata"
dbutils.fs.mkdirs(metadata_dir)
dbutils.fs.put(
    CUSTOMER_IDS_FILE, 
    json.dumps({"customer_ids": customer_ids, "timestamp": datetime.now().isoformat()}),
    overwrite=True
)

print(f"\n✅ Successfully generated {files_created} JSON files!")
print(f"   Location: {initial_dir}")
print(f"   Customer IDs: {min(customer_ids)} to {max(customer_ids)}")
print(f"   Metadata saved to: {CUSTOMER_IDS_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Initial Files

# COMMAND ----------

# Count files
initial_files = dbutils.fs.ls(f"{VOLUME_PATH}/initial_data")
print(f"📊 Total files generated: {len(initial_files)}")

# Show sample files
print(f"\n📁 Sample files:")
for f in initial_files[:5]:
    print(f"   {f.name} ({f.size} bytes)")

# Read and display a sample event
sample_file = initial_files[0].path
sample_content = dbutils.fs.head(sample_file)
sample_json = json.loads(sample_content)

print(f"\n📄 Sample CREATE event:")
print(json.dumps(sample_json, indent=2))

# Count by operation type
creates = sum(1 for f in initial_files if 'create' in f.name)
updates = sum(1 for f in initial_files if 'update' in f.name)

print(f"\n📈 Operation breakdown:")
print(f"   CREATE: {creates} ({creates*100//len(initial_files)}%)")
print(f"   UPDATE: {updates} ({updates*100//len(initial_files)}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛑 STOP HERE - Configure Your DLT Pipeline
# MAGIC
# MAGIC ### Now do this:
# MAGIC
# MAGIC 1. **Modify your DLT bronze layer** to read from the volume:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="dgli_bronze.abs_bronze.abs_bronze",
# MAGIC     comment="Raw JSON from volume (simulating Kafka)",
# MAGIC     table_properties={"quality": "bronze"},
# MAGIC )
# MAGIC def bronze():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("json")
# MAGIC         .option("multiLine", "true")
# MAGIC         .load("/Volumes/dgli_bronze/abs_bronze/cdc_data/initial_data/*.json")
# MAGIC         .selectExpr("to_json(struct(*)) AS payload_json")
# MAGIC         .withColumn("ingest_time", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC 2. **Start your DLT pipeline** and let it process the initial data
# MAGIC
# MAGIC 3. **Verify the data** in your final table:
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM dgli_silver.abs_final.customer;
# MAGIC -- Should show approximately 100 records
# MAGIC ```
# MAGIC
# MAGIC 4. **Once verified, come back and run Phase 2 below** to generate delete events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Generate Delete Events
# MAGIC
# MAGIC ⚠️ **Run this AFTER your DLT pipeline has processed the initial data**
# MAGIC
# MAGIC This will generate DELETE events for some of the customers created in Phase 1.

# COMMAND ----------

def generate_delete_event(customer_id, customer_data, timestamp_ms):
    """Generate a DELETE event for an existing customer"""
    return {
        "payload": {
            "before": customer_data,
            "after": None,  # NULL for deletes
            "source": {
                "table": "customer",
                "schema": "abs",
                "db": "testdb",
                "connector": "mysql"
            },
            "op": "d",
            "ts_ms": timestamp_ms,
            "transaction": None
        }
    }

# Load customer IDs from metadata
try:
    metadata_content = dbutils.fs.head(CUSTOMER_IDS_FILE)
    metadata = json.loads(metadata_content)
    all_customer_ids = metadata["customer_ids"]
    
    # Select subset to delete (last 20% of created customers)
    num_to_delete = min(DELETE_BATCH_SIZE, len(all_customer_ids) // 5)
    customer_ids_to_delete = random.sample(all_customer_ids, num_to_delete)
    
    print(f"🔄 Generating {num_to_delete} DELETE events...")
    print(f"   Deleting customer IDs: {sorted(customer_ids_to_delete)[:10]}...")
    
    # Create subdirectory for delete events
    delete_dir = f"{VOLUME_PATH}/delete_data"
    dbutils.fs.mkdirs(delete_dir)
    
    current_time_ms = int(datetime.now().timestamp() * 1000)
    
    for i, customer_id in enumerate(customer_ids_to_delete):
        # Create minimal customer data for the "before" state
        customer_data = {
            "customer_id": customer_id,
            "first_name": "DeletedUser",
            "last_name": f"ID{customer_id}",
            "email": f"deleted{customer_id}@example.com",
            "phone": "+1-555-000-0000",
            "age": 30,
            "created_at": current_time_ms - 86400000
        }
        
        event = generate_delete_event(customer_id, customer_data, current_time_ms + i * 1000)
        
        # Write delete event as JSON file
        filename = f"{delete_dir}/event_delete_{i:05d}_{customer_id}.json"
        dbutils.fs.put(filename, json.dumps(event), overwrite=True)
        
        if (i + 1) % 5 == 0:
            print(f"   Generated {i + 1}/{num_to_delete} delete events...")
    
    print(f"\n✅ Successfully generated {num_to_delete} DELETE events!")
    print(f"   Location: {delete_dir}")
    print(f"   Customer IDs deleted: {sorted(customer_ids_to_delete)}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("Make sure you ran Phase 1 first to generate initial data!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Delete Files

# COMMAND ----------

# Count delete files
try:
    delete_files = dbutils.fs.ls(f"{VOLUME_PATH}/delete_data")
    print(f"📊 Total delete files: {len(delete_files)}")
    
    # Show sample files
    print(f"\n📁 Sample delete files:")
    for f in delete_files[:5]:
        print(f"   {f.name} ({f.size} bytes)")
    
    # Read and display a sample delete event
    sample_file = delete_files[0].path
    sample_content = dbutils.fs.head(sample_file)
    sample_json = json.loads(sample_content)
    
    print(f"\n📄 Sample DELETE event:")
    print(json.dumps(sample_json, indent=2))
    
    # Extract customer IDs being deleted
    deleted_ids = []
    for f in delete_files:
        content = dbutils.fs.head(f.path)
        event = json.loads(content)
        cid = event["payload"]["before"]["customer_id"]
        deleted_ids.append(cid)
    
    print(f"\n🗑️ Customer IDs being deleted:")
    print(f"   {sorted(deleted_ids)}")
    
except Exception as e:
    print(f"No delete files found yet. Run Phase 2 first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Update DLT Pipeline to Process Deletes
# MAGIC
# MAGIC ### Option 1: Process deletes from separate directory
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="dgli_bronze.abs_bronze.abs_bronze")
# MAGIC def bronze():
# MAGIC     # Read from both initial_data and delete_data directories
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("json")
# MAGIC         .option("multiLine", "true")
# MAGIC         .load("/Volumes/dgli_bronze/abs_bronze/cdc_data/*_data/*.json")  # Wildcard to read both
# MAGIC         .selectExpr("to_json(struct(*)) AS payload_json")
# MAGIC         .withColumn("ingest_time", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### Option 2: Process entire volume
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="dgli_bronze.abs_bronze.abs_bronze")
# MAGIC def bronze():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("json")
# MAGIC         .option("multiLine", "true")
# MAGIC         .load("/Volumes/dgli_bronze/abs_bronze/cdc_data/**/*.json")  # Recursive
# MAGIC         .selectExpr("to_json(struct(*)) AS payload_json")
# MAGIC         .withColumn("ingest_time", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### After updating your DLT pipeline:
# MAGIC
# MAGIC 1. **Restart your DLT pipeline** (or let it pick up new files if running)
# MAGIC 2. **Wait for processing** (usually 30-60 seconds)
# MAGIC 3. **Verify deletes worked**:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check delete events reached bronze
# MAGIC SELECT COUNT(*) as delete_events
# MAGIC FROM dgli_bronze.abs_bronze.abs_bronze
# MAGIC WHERE get_json_object(payload_json, '$.payload.op') = 'd';
# MAGIC
# MAGIC -- Check specific deleted customer IDs are gone
# MAGIC SELECT customer_id 
# MAGIC FROM dgli_silver.abs_final.customer
# MAGIC WHERE customer_id IN (1000, 1001, 1002)  -- Replace with your deleted IDs
# MAGIC ORDER BY customer_id;
# MAGIC -- Should return 0 rows for deleted IDs
# MAGIC
# MAGIC -- Check final count decreased
# MAGIC SELECT COUNT(*) as total_records
# MAGIC FROM dgli_silver.abs_final.customer;
# MAGIC -- Should be approximately: initial_count - delete_count
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Generated Files

# COMMAND ----------

# Summary of all files
print("📊 Summary of Generated Data:\n")

# Initial data
try:
    initial_files = dbutils.fs.ls(f"{VOLUME_PATH}/initial_data")
    print(f"✅ Initial Data: {len(initial_files)} files")
    print(f"   Location: {VOLUME_PATH}/initial_data/")
except:
    print(f"❌ Initial Data: Not generated yet")

# Delete data
try:
    delete_files = dbutils.fs.ls(f"{VOLUME_PATH}/delete_data")
    print(f"✅ Delete Data: {len(delete_files)} files")
    print(f"   Location: {VOLUME_PATH}/delete_data/")
except:
    print(f"❌ Delete Data: Not generated yet")

# Metadata
try:
    metadata_files = dbutils.fs.ls(f"{VOLUME_PATH}/_metadata")
    print(f"✅ Metadata: {len(metadata_files)} files")
    print(f"   Location: {VOLUME_PATH}/_metadata/")
except:
    print(f"❌ Metadata: Not available")

print(f"\n📁 Full Volume Path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Query - Preview Records by Operation

# COMMAND ----------

# If your DLT pipeline has run, check the results
try:
    # Count by operation in bronze
    print("📊 Bronze Layer - Events by Operation:")
    bronze_ops = spark.sql("""
        SELECT 
            get_json_object(payload_json, '$.payload.op') AS operation,
            CASE 
                WHEN get_json_object(payload_json, '$.payload.op') = 'c' THEN 'CREATE'
                WHEN get_json_object(payload_json, '$.payload.op') = 'u' THEN 'UPDATE'
                WHEN get_json_object(payload_json, '$.payload.op') = 'd' THEN 'DELETE'
            END AS operation_name,
            COUNT(*) AS count
        FROM dgli_bronze.abs_bronze.abs_bronze
        GROUP BY 1, 2
        ORDER BY 1
    """)
    display(bronze_ops)
    
    # Check final table
    print("\n📊 Final Table - Record Count:")
    final_count = spark.sql("SELECT COUNT(*) as total FROM dgli_silver.abs_final.customer").collect()[0].total
    print(f"   Total records: {final_count}")
    
except Exception as e:
    print(f"⚠️ Tables not available yet. Run your DLT pipeline first!")
    print(f"   Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up Volume (Optional)

# COMMAND ----------

# Uncomment to clean up all generated files
# dbutils.fs.rm(f"{VOLUME_PATH}/initial_data", recurse=True)
# dbutils.fs.rm(f"{VOLUME_PATH}/delete_data", recurse=True)
# dbutils.fs.rm(f"{VOLUME_PATH}/_metadata", recurse=True)
# print(f"✅ Cleaned up all files from {VOLUME_PATH}")

# Or drop the entire volume
# spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
# print(f"✅ Dropped volume {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary & Workflow
# MAGIC
# MAGIC ### ✅ Phase 1: Initial Data (Completed if you see files above)
# MAGIC 1. Generated ~100 JSON files (CREATE and UPDATE events)
# MAGIC 2. Files stored in: `/Volumes/dgli_bronze/abs_bronze/cdc_data/initial_data/`
# MAGIC 3. Customer IDs tracked in metadata
# MAGIC
# MAGIC ### ✅ Phase 2: Delete Events (Run when ready to test deletes)
# MAGIC 1. Generate ~20 DELETE events for existing customers
# MAGIC 2. Files stored in: `/Volumes/dgli_bronze/abs_bronze/cdc_data/delete_data/`
# MAGIC 3. DELETE events contain customer IDs from Phase 1
# MAGIC
# MAGIC ### 🔄 Testing Workflow
# MAGIC
# MAGIC **Step 1**: Generate initial data (Phase 1)
# MAGIC ```
# MAGIC Run Cells 1-5 → Creates initial_data folder with CREATE/UPDATE events
# MAGIC ```
# MAGIC
# MAGIC **Step 2**: Configure DLT to read from volume
# MAGIC ```python
# MAGIC spark.readStream.format("json").load("/Volumes/.../initial_data/*.json")
# MAGIC ```
# MAGIC
# MAGIC **Step 3**: Run DLT pipeline, verify initial data loaded
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM dgli_silver.abs_final.customer;  -- Should be ~100
# MAGIC ```
# MAGIC
# MAGIC **Step 4**: Generate delete events (Phase 2)
# MAGIC ```
# MAGIC Run Cell 8 → Creates delete_data folder with DELETE events
# MAGIC ```
# MAGIC
# MAGIC **Step 5**: Update DLT to read both folders
# MAGIC ```python
# MAGIC spark.readStream.format("json").load("/Volumes/.../*_data/*.json")
# MAGIC ```
# MAGIC
# MAGIC **Step 6**: Restart DLT, verify deletes applied
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM dgli_silver.abs_final.customer;  -- Should be ~80 (100-20)
# MAGIC ```
# MAGIC
# MAGIC ### ✅ Expected Results
# MAGIC - Initial count: ~100 records
# MAGIC - After deletes: ~80 records (100 - 20 deletes)
# MAGIC - Deleted customer IDs should not exist in final table
# MAGIC - DELETE events visible in bronze layer (op='d')
# MAGIC
# MAGIC **Your delete handling is working correctly if these conditions are met!** 🎉
# MAGIC