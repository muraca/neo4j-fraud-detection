from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "ciao"))

def drop_content(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def create_customer(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "MERGE (c:Customer {customer_id : row.customer_id}) " +
            "SET c = { customer_id : row.customer_id, " +
            "x_customer_id : row.x_customer_id, y_customer_id : row.y_customer_id, " +
            "mean_amount : row.mean_amount, std_amount : row.std_amount, " + 
            "mean_nb_tx_per_day : row.mean_nb_tx_per_day }")

def create_terminal(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "MERGE (t:Terminal { terminal_id : row.terminal_id }) " + 
            "SET t = { terminal_id = row.terminal_id," +
            "x_terminal_id : row.x_terminal_id, y_terminal_id : row.y_terminal_id }")

def create_transaction(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\"  AS row " +  
            "MERGE (c:Customer {customer_id : row.customer_id}) " +
            "MERGE (t:Terminal {terminal_id : row.terminal_id}) " +
            "MERGE (c)-[tx:TRANSACTION{ transaction_id : row.transaction_id }]->(t) " +
            "SET tx = { transaction_id = row.transaction_id, tx_amount : row.tx_amount, " +    
            "tx_datetime : datetime({epochSeconds: toInteger(row.tx_datetime)}), " +
            "tx_fraud : row.tx_fraud, period_of_the_day : row.period_of_the_day, " +
            "kind_of_products : row.kind_of_products } ")
            
s = "1"
# path dalla root, altrimenti Neo4J non riesce a leggere
from_root = "/Users/rosario/Documents/Uni/Corsi/New%20Generation%20DBMS/neo4j-fraud-detection/"
file_path = "file:///" + from_root + "datasets/" + s + "/"
with driver.session() as session:
    session.write_transaction(drop_content)
    session.write_transaction(create_customer, file_path + "customers-" + s + ".csv")
    session.write_transaction(create_terminal, file_path + "terminals-" + s + ".csv")
    session.write_transaction(create_transaction, file_path + "transactions-" + s + ".csv")

driver.close()
