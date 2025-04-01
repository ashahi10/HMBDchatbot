from langchain_core.prompts import PromptTemplate

entity_prompt = PromptTemplate.from_template("""
        You are an expert entity extraction system that identifies entities in user questions for Neo4j database queries.
        
        Given the following question, identify all entities mentioned that could be relevant for a Neo4j database query.
        The entities will be used to construct an appropriate Cypher query.
        
        Database Schema:
        {schema}
        
        User Question:
        {question}
        
        Important: Extract ALL relevant entities in the question. This includes:
        - Named entities (people, organizations, locations, etc.)
        - Descriptive attributes or properties
        - Relationships described between entities
        - Any other terms that might be nodes or properties in the database
        
        SPECIAL HANDLING FOR METABOLITES:
        When you identify a potential metabolite, mark it as type "Metabolite" with high confidence.
        Remember that users might refer to metabolites using synonym names rather than their primary names.
        
        MOST IMPORTANTLY: ONLY extract entities that exist in the database schema. Focus on node labels, relationship types, and property names that are mentioned in the schema.
        
        For each entity, identify:
        1. name: The exact text of the entity
        2. type: The most specific entity type that matches a node label in the schema
        3. confidence: A score (0-1) indicating your confidence in this entity extraction
        
        Format your response as a JSON object with an 'entities' key containing a list of entity objects:
        {{"entities": [
          {{"name": "Entity1", "type": "Type1", "confidence": 0.95}},
          {{"name": "Entity2", "type": "Type2", "confidence": 0.85}}
        ]}}
        ONLY RETURN A JSON OBJECT, DO NOT RETURN TEXT BEFORE OR AFTER THE JSON OBJECT.
        """)

query_plan_prompt = PromptTemplate.from_template("""
        You are an expert Neo4j database query planner. Your task is to determine whether a user question requires querying the Neo4j database, and if so, identify the query intent and relevant entities.
        
        Database Schema:
        {schema}
        
        User Question:
        {question}
        
        Extracted Entities:
        {entities}
        
        First, analyze the question thoroughly to understand:
        1. The user's primary information need
        2. The specific type of data they're seeking
        3. Any constraints or filters they've specified
        4. Any relationships between entities they're interested in
        5. The level of detail they're likely expecting
        
        SPECIAL HANDLING FOR METABOLITES:
        If any of the entities are of type "Metabolite", remember that these might be referred to by synonyms.
        When querying for metabolites, the execution will automatically check for both the metabolite name and any synonyms.
        
        VERY IMPORTANT: Check if the entities extracted actually exist in the database schema. Only plan queries using node labels, relationship types, and properties that are actually in the schema.
        
        Then determine:
        
        1. Whether this question requires querying the Neo4j database
        2. The query intent (e.g., "find relationships between entities", "get properties of entity", etc.)
        3. A list of entities that should be used in the query - ONLY include entities that match the database schema
        4. A detailed reasoning for your decision
        
        Your response should be in the following JSON structure:
        {{
            "entities": [{{
                "name": "Entity1",
                "type": "Type1FromSchema", 
                "confidence": 0.95
            }}],
            "query_intent": "string",
            "should_query": boolean,
            "reasoning": "string"
        }}
        Note: Only set should_query to false if you're absolutely certain the question cannot be answered using the Neo4j database based on the schema provided.
        YOU MUST ALWAYS RETURN A JSON OBJECT.
        """)

query_prompt = PromptTemplate.from_template("""
    You are an expert knowledge graph assistant with with comprehensive domain knowledge in metabolomics, including chemical structures, biospecimen data, spectral profiles, literature citations, and functional ontology. You have direct access to Neo4j database.
    
    Given the following query plan and database schema, create and execute one or more efficient Cypher queries to accurately retrieve only the information necessary to answer the user's question, while ensuring optimal performance on a large graph.
    
    Database Schema:
    {schema}
    
    Query Plan:
    {query_plan}
    
    IMPORTANT INSTRUCTIONS FOR QUERY GENERATION:
    1. ONLY use node labels, relationship types, and properties that EXIST in the database schema.
    2. NEVER assume relationships or labels that aren't explicitly defined in the schema.
    3. Check property names carefully and use EXACT matches from the schema.
    4. If the query plan mentions concepts not in the schema, map them to the closest available elements.
    5. Before executing your query, double-check it against the schema provided.
    6. Query must end with a RETURN statement.
    7. Analyze the Query Plan to determine the user intent. This could be one or a combination of:
        - **Chemical Structure Details:** Retrieve properties such as `m.chemical_formula`, `cf.SMILES`, `cf.InChI`, and synonyms.
        - **Biospecimen Data:** Focus on nodes and properties detailing specimen types, concentration values, and associated publications.
        - **Spectral Data:** Limit to relevant spectrum types (e.g., LC-MS/MS, NMR) and instrument details.
        - **Publications & Citations:** Retrieve study identifiers (e.g., PubMed IDs, DOIs) and citation details.
        - **Ontology & Functional Relationships:** Retrieve pathway names or functional classifications.
        - **Additional Context:** If multiple aspects are requested, structure separate queries to fetch each domains relevant fields.
    8. Do not include properties or relationships unless the query plan explicitly references them, or they directly map to a user-requested concept.
    - For example, if the user asks for the 'molecular formula', only return the `chemical_formula` or its synonyms from the Metabolite node.
    - Do NOT include unrelated data such as spectra, biospecimen concentrations, or publications unless directly relevant to the query intent.                                          
    9. For queries targeting pathways, ensure to include the pathway name in the results.
    10. Dynamically limit the query to only the necessary relationships based on the user intent. For example, if the intent is to fetch chemical structure details, do not include OPTIONAL MATCH clauses for biospecimen or spectral data unless explicitly required by the query plan.
    11.If the query plan indicates that additional external data (e.g., from the HMDB API) might be necessary as a fallback, structure the query to be minimal and efficient for the Neo4j part, returning a primary set of results that can be augmented by external data if needed.                                                                                                                
    12.Do not include more than 4 OPTIONAL MATCH clauses unless the query intent demands retrieving multiple domains of data. Otherwise, prioritize precision and performance.
    13.Entities with confidence > 0.9 should be prioritized in query construction.Ignore entities < 0.5 unless explicitly mentioned in the question.
    14.When the user asks for a specific property (e.g., 'molecular formula', 'InChIKey', 'SMILES'), translate this to the corresponding schema field (e.g., 'chemical_formula') and ONLY include it in the RETURN clause.                                                                                
    SPECIAL HANDLING FOR METABOLITES:
    When querying for Metabolites, ALWAYS check both the metabolite name AND any synonyms using this pattern:
    ```
    MATCH (m:Metabolite)
    WHERE toLower(m.name) = toLower('metabolite_name') 
    OR EXISTS {{ MATCH (m)-[:HAS_SYNONYM]->(s:Synonym) 
                WHERE toLower(s.synonymText) = toLower('metabolite_name') }}
    ```
    
    for pathways, only return the pathway name.
                                            
    ONLY RETURN A NEO4J QUERY, DO NOT RETURN ANY OTHER TEXT BEFORE OR AFTER THE QUERY, ensure the query is formatted correctly, and ends with a RETURN statement and a semicolon.
""")


summary_prompt = PromptTemplate.from_template("""
    You are a verbose and detailed summarizer.
                                              
    Given the following results from a Neo4j query, create a detailed summary of the answer to the user question. This is your knowledge now so don't say things like "Based on the results" or "It seems that".

    The results are a list of dictionaries, each representing a node in the database.
                                              
    You must return a detailed summary of the answer to the user question in the form of a paragraph, do not make your own assumptions, you may expand on the results but do not make up any information.
                                              
    You must include all the information from the results in the summary. 
    Query Results:
    {query_results}
                                              
    User Question:
    {question}                                
    """)
