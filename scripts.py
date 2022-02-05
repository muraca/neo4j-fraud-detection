from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "ciao"))

def drop_content(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def create_customer(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "CREATE (c:Customer {customer_id : row.customer_id, " +
            "x_customer_id : row.x_customer_id, y_customer_id : row.y_customer_id, " +
            "mean_amount : row.mean_amount, std_amount : row.std_amount, " + 
            "mean_nb_tx_per_day : row.mean_nb_tx_per_day })")

def create_terminal(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "CREATE (t:Terminal { terminal_id : row.terminal_id, " +
            "x_terminal_id : row.x_terminal_id, y_terminal_id : row.y_terminal_id })")

def create_transaction(tx, PATH):
    tx.run (
            # "USING PERIODIC COMMIT 1000" +
            "LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " +  
            "MATCH (c:Customer {customer_id : row.customer_id}) " +
            ", (t:Terminal {terminal_id : row.terminal_id}) " +
            "CREATE (c)-[tx:TRANSACTION { transaction_id : row.transaction_id, " +
            "amount : row.amount, fraud : row.fraud, " +    
            "tx_datetime : datetime({epochSeconds: toInteger(row.tx_datetime)}) }]->(t) ")
    
s = "1"
# path dalla root, altrimenti Neo4J non riesce a leggere
from_root = "Users/rosario/Documents/Uni/Corsi/New%20Generation%20DBMS/neo4j-fraud-detection/"
file_path = "file:///" + from_root + "datasets/" + s + "/"
with driver.session() as session:
    session.write_transaction(drop_content)
    print("drop content failed " + str(session._state_failed))
    session.write_transaction(create_customer, file_path + "customers-" + s + ".csv")
    print("write customer failed " + str(session._state_failed))
    session.write_transaction(create_terminal, file_path + "terminals-" + s + ".csv")
    print("write terminal failed " + str(session._state_failed))
    session.write_transaction(create_transaction, file_path + "transactions-" + s + ".csv")
    print("write transaction failed " + str(session._state_failed))
driver.close()
