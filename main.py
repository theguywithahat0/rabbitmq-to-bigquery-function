import json
import os
import time
import traceback
import io
from typing import Dict, Any, List
import pika
import ssl
from google.cloud import bigquery
import functions_framework

# Environment variables for configuration
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', '5671'))
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', '/')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE')
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
BQ_DATASET = os.environ.get('BQ_DATASET')

# Initialize BigQuery client
try:
    bq_client = bigquery.Client()
    print("Successfully initialized BigQuery client")
except Exception as e:
    print(f"Warning: Failed to initialize BigQuery client: {str(e)}")
    bq_client = None

def get_table_from_message(message: Dict[str, Any]) -> str:
    """Extract the table name from a message."""
    # Look for table information in common fields
    for field in ['EntityType', 'Table', 'TableName']:
        if field in message and message[field]:
            # Clean the table name for BigQuery
            table_name = ''.join(c if c.isalnum() else '_' for c in str(message[field])).lower()
            return table_name
    
    # Default table if no table info found
    return 'default_table'

def transform_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Transform the message for BigQuery insertion."""
    # Initialize output dictionary
    output = {}
    
    # Process all fields from the message
    for key, value in message.items():
        if key in ['Table', 'EntityType', 'TableName']:
            continue  # Skip the table field
            
        if isinstance(value, (str, int, float, bool, type(None))):
            # Simple type, add directly
            output[key] = value
        elif isinstance(value, dict):
            # Handle nested objects like Data
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, (str, int, float, bool, type(None))):
                    output[f"{key}_{nested_key}"] = nested_value
                else:
                    output[f"{key}_{nested_key}"] = json.dumps(nested_value)
        else:
            # Complex type, convert to JSON string
            output[key] = json.dumps(value)
    
    # Add metadata
    output['processing_timestamp'] = time.time()
    
    return output

def write_to_bigquery(table_name: str, rows: List[Dict[str, Any]]) -> List[str]:
    """Write a batch of rows to BigQuery."""
    if not rows:
        print(f"No rows to write to table {table_name}")
        return []
        
    if not bq_client:
        return ["BigQuery client not initialized"]
        
    try:
        # Full table reference
        table_ref = f"{BQ_DATASET}.{table_name}"
        
        # Check if table exists
        try:
            bq_client.get_table(table_ref)
            table_exists = True
        except Exception:
            table_exists = False
            print(f"Table {table_ref} not found. Will create it.")
        
        # If table doesn't exist, create it
        if not table_exists:
            # Convert rows to newline-delimited JSON for schema detection
            json_data = '\n'.join([json.dumps(row) for row in rows])
            
            # Auto-detect schema based on the data
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            # Load the data to create the table
            job = bq_client.load_table_from_file(
                file_obj=io.StringIO(json_data),
                destination=table_ref,
                job_config=job_config
            )
            job.result()  # Wait for completion
            print(f"Created table {table_ref}")
            return []  # No errors
        
        # Table exists, use insert_rows_json
        errors = bq_client.insert_rows_json(table_ref, rows)
        if errors:
            error_messages = [f"Error: {str(error)}" for error in errors]
            print(f"Errors inserting rows into {table_ref}: {errors}")
            return error_messages
        else:
            print(f"Successfully inserted {len(rows)} rows into {table_ref}")
            return []
    except Exception as e:
        error_message = f"Error writing to BigQuery: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        return [error_message]

def process_rabbitmq_messages(max_messages: int = 10000) -> Dict[str, Any]:
    """Connect to RabbitMQ, process messages, and write to BigQuery."""
    # Message processing results
    results = {
        'messages_processed': 0,
        'tables_updated': set(),
        'errors': []
    }
    
    # RabbitMQ connection
    try:
        print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
        
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                ssl_options=pika.SSLOptions(context=ssl_context)
            )
        )
        
        channel = connection.channel()
        print(f"Connected to RabbitMQ and created channel")
        
        # Message buffer organized by table
        message_buffer = {}
        
        # Process messages until max_messages or no more messages
        for i in range(max_messages):
            # Get a message with no auto-ack
            method_frame, header_frame, body = channel.basic_get(
                queue=RABBITMQ_QUEUE,
                auto_ack=False
            )
            
            # No more messages
            if method_frame is None:
                print("No more messages in queue")
                break
                
            try:
                # Parse message
                message = json.loads(body.decode('utf-8'))
                
                # Determine destination table
                table_name = get_table_from_message(message)
                
                # Transform message for BigQuery
                transformed = transform_message(message)
                
                # Add to buffer for batch processing
                if table_name not in message_buffer:
                    message_buffer[table_name] = []
                    
                message_buffer[table_name].append(transformed)
                
                # If buffer for this table is large enough, write to BigQuery
                if len(message_buffer[table_name]) >= 100:  # Batch size of 100
                    bq_errors = write_to_bigquery(table_name, message_buffer[table_name])
                    if bq_errors:
                        results['errors'].extend(bq_errors)
                    
                    results['tables_updated'].add(table_name)
                    results['messages_processed'] += len(message_buffer[table_name])
                    message_buffer[table_name] = []
                
                # Acknowledge message
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                
            except Exception as e:
                # Log error and nack message
                error_message = f"Error processing message: {str(e)}\n{traceback.format_exc()}"
                print(error_message)
                results['errors'].append(error_message)
                
                # Reject message and requeue
                channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag,
                    requeue=True
                )
        
        # Process any remaining messages in buffer
        for table_name, rows in message_buffer.items():
            if rows:
                bq_errors = write_to_bigquery(table_name, rows)
                if bq_errors:
                    results['errors'].extend(bq_errors)
                
                results['tables_updated'].add(table_name)
                results['messages_processed'] += len(rows)
        
        # Close connection
        connection.close()
        print("Closed RabbitMQ connection")
        
    except Exception as e:
        error_message = f"Error connecting to RabbitMQ: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        results['errors'].append(error_message)
    
    # Convert set to list for JSON serialization
    results['tables_updated'] = list(results['tables_updated'])
    return results

@functions_framework.http
def rabbitmq_to_bigquery(request):
    """
    HTTP-triggered function that processes RabbitMQ messages and loads to BigQuery.
    """
    try:
        # Parse request for configuration
        request_json = request.get_json(silent=True) or {}
        max_messages = request_json.get('max_messages', 10000)
        
        print(f"Starting to process up to {max_messages} messages")
        
        # Process messages
        start_time = time.time()
        results = process_rabbitmq_messages(max_messages)
        duration = time.time() - start_time
        
        # Add timing information
        results['duration_seconds'] = duration
        results['messages_per_second'] = (
            results['messages_processed'] / duration if duration > 0 and results['messages_processed'] > 0 else 0
        )
        
        print(f"Processed {results['messages_processed']} messages in {duration:.2f} seconds")
        
        # Return results
        return results
        
    except Exception as e:
        error_message = f"Function error: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        return {'error': error_message}, 500

# This is crucial for Cloud Run
if __name__ == "__main__":
    # Start the server on port 8080
    from functions_framework import create_app
    app = create_app(target="rabbitmq_to_bigquery", source=None)
    
    # Start the server
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
