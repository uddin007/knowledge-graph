# knowledge-graph
A Knowledge Graph is developed using Spark Framework within Databricks and represented in Neo4j to establish relationships between data products. We utilize a pretrained BERT Sentence Transformer model (all-MiniLM-L6-v2) from Hugging Face for embedding column definitions. The cosine similarity provided by sklearn serves as the semantic match score, aiding users in identifying and exploring related data products. We Use Databricks notebook to perform the following:
1. Import and load packages including Hugginface’s all-MiniLM-L6-v2 sentence transformer and sklearn’s Cosine Similarity.
2. Create tables to store the scores
3. Create function to calculate and store similarity scores in a pair
4. Establish connection to Neo4j
5. Export similarity scores to Neo4j
This is an example dataset in Unity Catalog. Defination of each product is stored in the Comment section. This fact table provides daily sales of 3 products in a store.

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/372109c4-24b3-494e-aff5-cdb3e8170569)

First we establish the pairwise relation between each field of two datasets. Then we take the average to represent overall relationship between two tables. 

![kg-process](https://github.com/uddin007/knowledge-graph/assets/37245809/6c0aa0cf-c9c4-4dee-b618-3feab0dfaec0)

As shown in the above Figure, dataset 1 and 2 has higher similarity score than 1 and 2 or 2 and 3. The reason being dataset 1 and 2 are lists of products from a toy store unlike dataset 3, where products are coffee brewers. 

To store and represent the results in Neo4j graph database, following steps are applied:
1. Provision Neo4j SaaS application from Azure Marketplace
2. Once provisioned click & open the SaaS account (Figure)

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/9bab86bf-5c72-42fc-bf1c-dabffec71fbb)

3. Launch a Neo4j instance by choosing VM type and Pricing

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/6ae0c84f-5ead-46b4-b218-e625675677c3)

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/c962ee27-8bcd-47d3-96ec-863db51bf859)

5. Download username, password and URL

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/08103812-41ad-4df8-ba08-970346ad47e8)

7. Upload appropriate JAR file in Databricks cluster 'Libraries'

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/546f0a6d-375a-4541-a841-3b917481352d)

8. Use the credentials to connect to the Neo4j instance

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/7b8ef379-37d2-4d66-81d2-dfebb042e8ae)

9. Use the credentials to connect from Databricks 
10. Store results to Neo4j database
11. Run queries and visualize

![image](https://github.com/uddin007/knowledge-graph/assets/37245809/af4b593d-711c-442d-98fc-9cbf48e315d6)






