# RabbitMQ to BigQuery Cloud Function

This Cloud Function connects to a RabbitMQ queue, processes messages, and loads them into BigQuery tables. It dynamically creates tables based on message content if they don't already exist.

## Features

- Processes messages from a RabbitMQ queue
- Creates BigQuery tables dynamically if they don't exist
- Transforms message data for BigQuery insertion
- Batches inserts for better performance
- Handles errors gracefully with message requeuing
- Provides detailed processing statistics

## Requirements

- Python 3.10+
- Google Cloud Functions
- RabbitMQ server with SSL enabled
- BigQuery dataset

## Dependencies

- `functions-framework==3.*` - Google Cloud Function framework
- `pika==1.3.1` - RabbitMQ client
- `google-cloud-bigquery>=3.0.0` - BigQuery client

## Environment Variables

The function requires the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `RABBITMQ_HOST` | RabbitMQ host address | (required) |
| `RABBITMQ_PORT` | RabbitMQ port | 5671 |
| `RABBITMQ_VHOST` | RabbitMQ virtual host | / |
| `RABBITMQ_QUEUE` | RabbitMQ queue name | (required) |
| `RABBITMQ_USERNAME` | RabbitMQ username | (required) |
| `RABBITMQ_PASSWORD` | RabbitMQ password | (required) |
| `BQ_DATASET` | BigQuery dataset name | (required) |

## Deployment

### Deploy from GitHub

1. Connect your GitHub repository to Google Cloud
2. Deploy using gcloud CLI:

```bash
gcloud functions deploy rabbitmq-to-bigquery \
  --gen2 \
  --runtime=python310 \
  --region=us-central1 \
  --source=https://source.developers.google.com/projects/YOUR_PROJECT/repos/github_YOUR_USERNAME_YOUR_REPO_NAME/moveable-aliases/main/paths/ \
  --trigger-http \
  --entry-point=rabbitmq_to_bigquery \
  --set-env-vars=RABBITMQ_HOST=your-host,RABBITMQ_PORT=5671,RABBITMQ_VHOST=/,RABBITMQ_QUEUE=your-queue,RABBITMQ_USERNAME=your-username,RABBITMQ_PASSWORD=your-password,BQ_DATASET=your-dataset
```

### Deploy from Google Cloud Console

1. Go to Cloud Functions in the Google Cloud Console
2. Click "Create Function"
3. Set your trigger type to HTTP
4. In the "Source code" section, select "Repository"
5. Connect and select your GitHub repository
6. Set the entry point to "rabbitmq_to_bigquery"
7. Configure environment variables
8. Deploy

## Usage

The function can be triggered via HTTP request. You can optionally specify the maximum number of messages to process:

```json
{
  "max_messages": 5000
}
```

## Function Response

The function returns a JSON object with processing statistics:

```json
{
  "messages_processed": 250,
  "tables_updated": ["table1", "table2"],
  "errors": [],
  "duration_seconds": 5.67,
  "messages_per_second": 44.09
}
```

## Table Creation Logic

Tables are named based on message content using these fields (in order of priority):
1. EntityType
2. Table
3. TableName

If none of these fields are present, messages are written to "default_table".

## Contributing

Please feel free to submit issues or pull requests for improvements or bug fixes.

## License
This project is open source and available under the MIT License.
