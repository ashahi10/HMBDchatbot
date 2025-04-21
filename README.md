# HMDB Chatbot

## Project Overview
Developed an intelligent, LLM-driven chatbot that answers complex user questions about metabolites by leveraging a Neo4j Knowledge Graph (KG), HMDB API, and advanced LLM reasoning.

## Directory Structure
```
.
├── backend/           # Backend server and API implementation
├── frontend/          # Frontend React application
├── .gitignore        # Git ignore rules
└── .pytest_cache/    # Python test cache directory
```

## System Requirements
- Python 3.9+
- Node.js 18+ with npm
- Docker (for Neo4j container)

## Installation

### Backend Setup
1. Clone the repository
2. Navigate to the backend directory:
   ```bash
   cd backend
   ```
3. Create and activate a Python virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Create `.env` file in the backend directory
5. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Required API Keys
Create a `.env` file in the backend directory with the following environment variables:

```env
# Neo4j Configuration
NEO4J_URI="bolt://localhost:7687"
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password

# Groq API Keys
GROQ_API_KEY=your_groq_api_key
GROQ_API_KEY_GENERATION=your_groq_generation_api_key2

# HMDB API Configuration
HMDB_API_KEY=your_hmdb_api_key
HMDB_BASE_URL=your_hmdb_base_url

# Qwen API Keys
QWEN=your_qwen_api_key
QWEN_API=your_qwen_api_key
```

**Note:** You'll need to obtain these API keys from their respective services:
- Groq API keys from [Groq's website](https://groq.com).Make sure to get two different api keys for groq.
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

3. Access Neo4j Browser UI:
   - Go to http://localhost:7474 in your browser
   - Login with:
     - Username: neo4j
     - Password: test1234

### Preparing HMDB Data
Before populating the graph database, download all 8 HMDB XML files.

1. Create a directory named `HMDB_DATA` in the `backend/` folder:
   ```bash
   mkdir backend/HMDB_DATA
   ```

2. Place the following XML files inside `HMDB_DATA/`:
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
Once the `HMDB_DATA/` folder is ready:

1. In your Python virtual environment, run:
   ```bash
   cd backend
   python ingest_hmdb.py
   ```

2. What to expect:
   - The script will connect to your running Neo4j container
   - It will create constraints and indexes automatically
   - Each XML file is parsed and streamed into the graph
   - The ingestion may take FEW HOURS depending on file size and system performance
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
   - The first launch may take ~3–4 minutes as the Neo4j schema is parsed and cached
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