# knowledge-graph
A Knowledge Graph is developed using Spark Framework within Databricks and represented in Neo4j to establish relationships between data products. We utilize a pretrained BERT Sentence Transformer model (all-MiniLM-L6-v2) from Hugging Face for embedding column definitions. The cosine similarity provided by sklearn serves as the semantic match score, aiding users in identifying and exploring related data products. We Use Databricks notebook to perform the following:
1. Import and load packages including Hugginface’s all-MiniLM-L6-v2 sentence transformer and sklearn’s Cosine Similarity.
2. Create tables to store the scores
3. Create function to calculate and store similarity scores in a pair
4. Establish connection to Neo4j
5. Export similarity scores to Neo4j


