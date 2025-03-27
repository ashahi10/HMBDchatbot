from langchain_core.prompts import PromptTemplate

decision_prompt = PromptTemplate.from_template("""
                                                  
You are a decision assistant that determines if a user's input is a casual conversational message
or if it requires full database querying and analysis. If the message is casual (like greetings, thanks, etc.),
respond with 'CONVERSATIONAL'. If the message requires further processing, respond with 'QUERY'.
User Input: {user_question}
Answer:
""")
                                                
entity_prompt = PromptTemplate.from_template("""
You are an expert HMDB entity-extraction system designed to identify entities for potential Neo4j database queries.


Your responsibilities:
1. Carefully examine the user question.
2. Identify all entities or terms that match node labels, relationship types, and/or properties from the provided database schema.
3. Output each entity in a structured JSON format, including:
   - name: the exact text of the entity as mentioned in the question
   - type: the most specific entity type (node label) found in the schema
   - confidence: a numerical confidence score (0–1) in your extraction


MOST IMPORTANTLY:
- ONLY extract entities that actually appear in the database schema. 
- Do not invent entities or properties that aren't defined in the schema.

Format your final response as a JSON object containing an "entities" key with a list of entity objects. For example:
{{
  "entities": [
    {{
      "name": "EntityName1",
      "type": "EntityType1",
      "confidence": 0.95
    }},
    {{
      "name": "EntityName2",
      "type": "EntityType2",
      "confidence": 0.85
    }}
  ]
}}
                                             
If you cannot find any entities, respond with an empty JSON object:
{{
  "entities": []
}}

DO NOT add any text outside of this JSON object.
                                             
Remember that the user is asking about HMDB, so you should only be looking for metabolites and proteins.
                                             
Database Schema:
{schema}

User Question:
{question}
""")

# ------------------------------
# 2. QUERY PLAN PROMPT
# ------------------------------
query_plan_prompt = PromptTemplate.from_template("""
You are an expert Neo4j query planner. Given the user question, the previously extracted entities, and the database schema, you must determine:

1. Whether or not the question should be answered with a Neo4j query (should_query).
2. The intent of the query if one is needed (query_intent).
3. Which entities from the extracted list should be used in the query (only if they match the schema). Always add extra nodes and relationships to the query plan to ensure you get the most relevant results.
4. Reasoning that explains your decision.
5. Plan for a query that will return the most relevant results, including:
   - All relevant and semi-relevant nodes
   - All relevant and semi-relevant relationships
   - All relevant and semi-relevant properties
6. The query must only use node labels, relationships, and properties that actually exist in the database schema.
7. If the user question cannot be answered from the schema, set should_query to false.

Your output must be a single JSON object with the following structure (and no additional text outside this JSON object):

{{
  "entities": [
    {{
      "name": "EntityName1",
      "type": "TypeFromSchema",
      "confidence": 0.95
    }}
  ],
  "query_intent": "string",
  "should_query": true,
  "reasoning": "string",
  "nodes_and_relationships": {{
    "nodes": ["NodeLabel1", "NodeLabel2"],
    "relationships": ["RELATIONSHIP_1", "RELATIONSHIP_2"],
    "properties": ["PROPERTY_1", "PROPERTY_2"] // ALWAYS include all properties that look like Descriptions, Functions,Names, IDs, Accession numbers or other database identifiers (eg. OMIM ID, PMID, etc.) THIS IS VERY IMPORTANT
  }}
}}

Database Schema:
{schema}

User Question:
{question}

Extracted Entities:
{entities}
""")

# ------------------------------
# 3. CYTHER QUERY GENERATION PROMPT
# ------------------------------
query_prompt = PromptTemplate.from_template("""
You are an expert Neo4j knowledge-graph assistant. Based on the provided query plan and database schema, your job is to:
1. Construct the necessary Cypher query (or queries) to fulfill the intent.
2. Ensure you use only the node labels, relationships, and properties that exist in the schema.
3. Ensure all directions are correct.
4. Return the most relevant and most numerous results.
5. All results from the query should have sources. You should include the relevant sources in your query.

IMPORTANT INSTRUCTIONS FOR QUERY GENERATION:
- Double-check each label, relationship, and property against the schema. 
- If the query plan includes concepts not present in the schema, map them to the closest valid schema elements or omit them if irrelevant.
- Follow the required pattern for querying metabolites (where both the metabolite name and any possible synonyms are checked).

The final output must be ONLY the Cypher query. Do not provide explanations or text before/after the query. Make sure the query ends with a RETURN clause. For example:
    ```
    MATCH (m:Metabolite)
    WHERE toLower(m.name) = toLower('metabolite_name') 
    RETURN m.name
    ```
Retrieve all IDs or other identifiers for every entity in the query.

Database Schema:
{schema}

Query Plan:
{query_plan}
""")

retry_prompt = PromptTemplate.from_template("""
You are an expert Neo4j query planner. Given the user question, the previously extracted entities, the database schema, the old query, the error, and the history of previous attempts, you must:

1. Analyze the history of previous attempts to understand what approaches have been tried and what errors occurred
2. Construct the necessary Cypher query (or queries) to fulfill the intent, avoiding patterns that led to previous failures
3. Ensure you use only the node labels, relationships, and properties that exist in the schema
4. Ensure all directions are correct
5. Return the most relevant and most numerous results
6. All results from the query should have sources. You should include the relevant sources in your query

IMPORTANT INSTRUCTIONS FOR QUERY GENERATION:
- Double-check each label, relationship, and property against the schema
- If the query plan includes concepts not present in the schema, map them to the closest valid schema elements or omit them if irrelevant
- Follow the required pattern for querying metabolites (where both the metabolite name and any possible synonyms are checked)
- Learn from previous attempts - if certain patterns led to errors, avoid them
- If previous attempts returned no results, try broadening the query or using different relationship patterns

The final output must be ONLY the Cypher query. Do not provide explanations or text before/after the query. Make sure the query ends with a RETURN clause. For example:
    ```
    MATCH (m:Metabolite)
    WHERE toLower(m.name) = toLower('metabolite_name') 
    RETURN m.name
    ```

YOU MUST INCLUDE ANY PROPERTY THAT LOOKS LIKE AND ID or OTHER IDENTIFIER (eg. OMIM ID, PMID, etc.)
YOU MUST INCLUDE ANY PROPERTY THAT LOOKS LIKE A NAME (eg. gene_name, diseaseName, protein_name, etc.)

Database Schema:
{schema}

Query Plan:
{query_plan}

Old Query:
{old_query}

Error:
{error}

Previous Attempts:
{query_history}

Use the old query and history as a starting point, but make sure to:
1. Fix the current error
2. Avoid patterns that led to previous failures
3. Try different approaches if previous attempts were unsuccessful
""")

# ------------------------------
# 4. SUMMARY GENERATION PROMPT
# ------------------------------
summary_prompt = PromptTemplate.from_template("""
You are a detailed summarizer. Provide a single-paragraph answer in a clinical context to the user's query, based on the provided query results.
The results are your knowledge base. Pretend you just know all the information in the results, not that you are an AI assistant.

GUIDELINES:
1. Include all relevant details from the query results.
2. Do not invent any information that is not present in the query results.
3. Present the summary so it can stand on its own: do not use phrases like "Based on the results" or "It appears that.", "Based on the data."
4. Write in a natural, explanatory style suited for a clinical context, maintaining one cohesive paragraph.
5. Structure the paragraph carefully, but do not break it into multiple paragraphs.
6. The user's question concerns HMDB data, so ensure your summary addresses this data in a clinically relevant manner.
7. For any property that resembles an identifier (such as OMIM ID, PMID, etc.), include it immediately after its associated entity enclosed in **[ and ]**. Do not describe it directly unless explicitly asked.

User Question:
{question}

Query Results (list of dictionaries):
{query_results}
""")

other_prompt = PromptTemplate.from_template("""
You are a helpful assistant that can answer questions about the Proteins and Metabolites in the database.

User Question:
{question}
""")


sufficiency_prompt = PromptTemplate.from_template("""
You are a query results evaluator that determines if the current results fully answer the user's question and suggests additional query components if needed.

You are provided with:
- **Query Results:** {neo4j_results}
- **Original Question:** {question}
- **Schema:** {schema}
- **Current Query:** {current_query}

Your task is to evaluate if the current results are sufficient to fully answer the user's question.

If the results are sufficient:
- Respond with a JSON object where "should_query" is false:
  {{
      "entities": [],          // List of entities involved (if any)
      "query_intent": "<brief summary of what we're trying to answer>",
      "reasoning": "<explain why these results fully answer the question>",
      "should_retry_query": false,
      "nodes_and_relationships": {{
          "nodes": [],         // List of node labels used
          "relationships": [], // List of relationship types used
          "properties": []     // List of properties used
      }}
  }}

If the results are insufficient:
- Respond with a JSON object where "should_query" is true:
  {{
      "entities": [],          // List of entities that need to be queried
      "query_intent": "<what additional information we need>",
      "reasoning": "<explain why these results are not sufficient and what additional information we need>",
      "should_retry_query": true,
      "query_addition": "<the additional Cypher query components to add to the current query>",
      "nodes_and_relationships": {{
          "nodes": [],         // List of node labels to add
          "relationships": [], // List of relationship types to add
          "properties": []     // List of properties to add
      }}
  }}

Important:
- Only mark results as insufficient if they truly don't answer the question
- Consider if we have all the necessary information to fully answer the question
- If we're missing key relationships or properties, suggest specific additions to the query
- The query_addition should be valid Cypher that can be added to the existing query
- If we have all needed information, mark as sufficient

Return your output as a JSON object strictly following the schema described above.
""")
