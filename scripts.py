from neo4j import GraphDatabase
import pathlib

def drop_content(tx):
    tx.run("MATCH (n) DETACH DELETE n")
#TODO load integers with toInteger, doubles with toDouble etc
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
            # "WITH row LIMIT 1000 " +
            "MATCH (c:Customer {customer_id : row.customer_id}) " +
            ", (t:Terminal {terminal_id : row.terminal_id}) " +
            "CREATE (c)-[tx:TRANSACTION { transaction_id : row.transaction_id, " +
            "tx_amount : row.tx_amount, tx_fraud : row.tx_fraud, " +    
            "tx_datetime : datetime({epochSeconds: toInteger(row.tx_datetime)}) }]->(t) ")
    
dataset = "1"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "ciao"))

from_root = str(pathlib.Path(__file__).parent.resolve())
file_path = "file://" + from_root + "/datasets/" + dataset + "/"

with driver.session() as session:
    session.write_transaction(drop_content)
    session.write_transaction(create_customer, file_path + "customers-" + dataset + ".csv")
    session.write_transaction(create_terminal, file_path + "terminals-" + dataset + ".csv")
    session.write_transaction(create_transaction, file_path + "transactions-" + dataset + ".csv")
    print("write transaction " + "failed" if session._state_failed else "completed")

driver.close()
