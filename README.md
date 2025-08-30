# HMDB Chatbot

## Project Overview
Developed an intelligent, LLM-driven chatbot that answers complex user questions about metabolites by leveraging a Neo4j Knowledge Graph (KG), HMDB API, and advanced LLM reasoning.

## Directory Structure
```
.
├── backend/                    # FastAPI backend server
│   ├── api/                   # REST API controllers
│   ├── cache/                 # Cache storage (queries, API responses, memory, schema)
│   ├── ingestion/             # HMDB data ingestion scripts
│   │   └── xml files/         # Store HMDB XML files here
│   ├── pipeline/              # LLM processing pipeline
│   ├── services/              # Core business logic services
│   ├── tests/                 # Test suite
│   ├── utils/                 # Utilities (Neo4j, caching, schema)
│   ├── main.py               # FastAPI application entry point
│   └── requirements.txt      # Python dependencies
├── frontend/                  # React frontend application
│   ├── src/                  # React source code
│   │   ├── components/       # React components
│   │   └── hooks/           # Custom React hooks
│   └── package.json         # Node.js dependencies
└── README.md               # This file
```

## System Requirements
- **Python**: 3.9 or higher
- **Node.js**: 18+ with npm
- **Docker**: For Neo4j container
- **RAM**: Minimum 8GB (16GB recommended for large datasets)
- **Storage**: At least 10GB free space for HMDB data and caches

## Installation Guide

### Step 1: Clone and Setup
```bash
git clone <repository-url>
cd lc
```

### Step 2: Backend Setup
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Step 3: Environment Configuration
Create a `.env` file in the `backend/` directory with the following variables:

```env
# Neo4j Configuration
NEO4J_URI="bolt://localhost:7687"
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_neo4j_password

# Primary LLM Provider - Groq (Required)
GROQ_API_KEY=your_groq_api_key
GROQ_API_KEY_GENERATION=your_groq_generation_api_key2

# HMDB API Configuration (Required)
HMDB_API_KEY=your_hmdb_api_key
HMDB_BASE_URL=your_hmdb_base_url

# Qwen API Keys
QWEN=your_qwen_api_key
QWEN_API=your_qwen_api_key
```

**Note:** You'll need to obtain these API keys from their respective services:
- Groq API keys from [Groq's website](https://groq.com).Make sure to get two different api keys for groq.make sure to activate paid version for better context window and fast llm responses .
- HMDB API key from [HMDB's website](https://hmdb.ca)
- Qwen API keys from [Qwen's platform](https://qwen.ai)

### Setting Up Neo4j via Docker
We use Neo4j in a Docker container for local graph database hosting.

1. Pull the official Neo4j image:
   ```bash
   docker pull neo4j:latest
   ```
   This project has been tested with neo4j:latest (version 2025.02.0).

2. Run the Neo4j container:
   ```bash
   docker run -d \
     --name neo4j-container \
     -p 7474:7474 -p 7687:7687 \
     -v $HOME/neo4j/data:/data \
     -v $HOME/neo4j/import:/import \
     -v $HOME/neo4j/plugins:/plugins \
     -e NEO4J_AUTH=neo4j/test1234 \
     -e NEO4J_dbms_security_procedures_unrestricted=apoc.* \
     -e NEO4J_PLUGINS='["apoc"]' \
     neo4j:latest
   ```
   Replace `test1234` with your own password if desired.

**Access Neo4j Browser:**
- URL: http://localhost:7474
- Username: neo4j
- Password: your_password

### Step 5: HMDB Data Preparation
Download all 8 HMDB XML files and place them in `backend/ingestion/xml files/`:

1. Store the XML files in the existing `xml files/` folder within the `ingestion/` directory:
   ```bash
   # The xml files should be placed in: backend/ingestion/xml files/
   ```

2. Place the following XML files inside `backend/ingestion/xml files/`:
   - hmdb_metabolites.xml
   - hmdb_proteins.xml
   - csf_metabolites.xml
   - serum_metabolites.xml
   - urine_metabolites.xml
   - sweat_metabolites.xml
   - saliva_metabolites.xml
   - feces_metabolites.xml

   **Note:** These files are large. Ensure they are unzipped XML files downloaded from the official HMDB FTP or website.

### Populating the Neo4j Knowledge Graph
Once the `xml files/` folder is ready:

1. In your Python virtual environment, run:
   ```bash
   cd backend
   python ingest_hmdb.py
   ```

2. What to expect:
   - The script will connect to your running Neo4j container
   - It will create constraints and indexes automatically
   - Each XML file is parsed and streamed into the graph
   - The ingestion may take a while depending on file size and system performance
   - Wait for a success message and check Neo4j getting populated from Neo4j browser

### Running the Backend 
Once ingestion is done and `.env` is ready:

1. Activate your virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Run the backend server:
   ```bash
   cd backend
   python main.py
   ```

3. Notes:
   - The first launch may take ~5–7 minutes as the Neo4j schema is parsed and cached
   - You'll see:
     ```
     running fine
     app startup time: ...
     Server runs on: http://localhost:8001
     ```

### Running the Frontend (React/Vite)

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install frontend dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

4. Access the frontend:
   - Visit http://localhost:3000 in your browser
   - The frontend connects to the backend automatically 