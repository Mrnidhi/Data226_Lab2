# Week 8: Airflow + Pinecone Vector Search Pipeline

## Overview
This project implements an end-to-end data pipeline using Apache Airflow to orchestrate text processing, embedding generation, and semantic search with Pinecone vector database.

## Architecture
- **Airflow**: Workflow orchestration with CeleryExecutor
- **Pinecone**: Serverless vector database for embeddings
- **Sentence Transformers**: Text embedding using MiniLM (384 dimensions)
- **Docker Compose**: Multi-container deployment

## Components

### 1. DAG: `hw8_pinecone_pipeline`
Located in `airflow/dags/hw8_pinecone_pipeline.py`

**Tasks:**
1. **build_input_file**: Downloads text from Project Gutenberg, cleans and chunks it, writes to JSONL
2. **create_pinecone_index**: Idempotently creates a serverless Pinecone index
3. **embed_and_upsert**: Generates embeddings using MiniLM and upserts vectors to Pinecone
4. **semantic_search**: Performs similarity search to verify the pipeline

### 2. Docker Services
- **airflow-webserver** (port 8080)
- **airflow-scheduler**
- **airflow-worker** (Celery)
- **airflow-triggerer**
- **postgres** (metadata DB)
- **redis** (message broker)
- **dbt** (data transformation, optional)

## Setup Instructions

### Prerequisites
- Docker Desktop installed and running
- Pinecone account with API key
- Minimum 4GB RAM allocated to Docker

### Steps

1. **Configure environment**:
   ```bash
   echo "AIRFLOW_UID=$(id -u)" > .env
   echo "AIRFLOW_GID=$(id -g)" >> .env
   ```

2. **Build and initialize**:
   ```bash
   docker compose -f docker-compose-celery.yaml build
   docker compose -f docker-compose-celery.yaml up airflow-init
   docker compose -f docker-compose-celery.yaml up -d
   ```

3. **Set Airflow Variable** (via UI at http://localhost:8080):
   - Key: `pinecone_cfg`
   - Value (JSON):
     ```json
     {
       "api_key": "your-pinecone-api-key",
       "index_name": "hw8-embeddings",
       "dimension": 384,
       "metric": "cosine",
       "cloud": "aws",
       "region": "us-east-1",
       "top_k": 5
     }
     ```

4. **Trigger the DAG**:
   - Navigate to http://localhost:8080
   - Login: `airflow` / `airflow`
   - Enable and trigger `hw8_pinecone_pipeline`

## Key Features

### Text Processing
- Downloads public domain text (Shakespeare's Romeo and Juliet)
- Normalizes whitespace and splits into overlapping chunks
- Chunks: 1200 characters with 150-character overlap

### Embeddings
- Model: `sentence-transformers/all-MiniLM-L6-v2`
- Dimensions: 384
- Batch processing: 64 chunks per batch

### Vector Database
- Platform: Pinecone Serverless
- Index spec: AWS us-east-1
- Similarity metric: Cosine

## File Structure
```
Week 8/
├── airflow/
│   ├── Dockerfile              # Extends apache/airflow:2.10.4
│   ├── requirements.txt        # Python dependencies
│   └── dags/
│       └── hw8_pinecone_pipeline.py
├── dbt/
│   └── Dockerfile              # dbt-core with Snowflake adapter
├── docker-compose-celery.yaml  # Multi-container orchestration
├── docker-compose.yaml         # Simplified single-node setup
├── .gitignore
└── README.md
```

## Troubleshooting

### DAGs not visible
- Ensure `airflow/dags` is correctly mounted in docker-compose
- Restart scheduler: `docker compose -f docker-compose-celery.yaml restart airflow-scheduler`

### Permission errors
- HuggingFace cache redirected to `/opt/airflow/data/.cache/huggingface`
- Ensure `data/` directory exists and has correct permissions

### Pinecone connection issues
- Verify API key in Airflow Variables
- Check network connectivity in container
- Confirm index region matches your Pinecone project

## Technologies Used
- Apache Airflow 2.10.4
- Pinecone (latest)
- Sentence Transformers
- PostgreSQL (Airflow metadata)
- Redis (Celery broker)
- Docker & Docker Compose

## Assignment Context
This project fulfills the requirements for DATA 226 Week 8 homework, demonstrating:
- Airflow DAG design with TaskFlow API
- Vector database integration
- ML model deployment in containerized environments
- Idempotent data pipeline design
- Semantic search implementation

## Author
Nidhi Gowda
SJSU DATA 226 - Data Warehousing
Fall 2025
