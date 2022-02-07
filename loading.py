from neo4j import GraphDatabase
import pathlib

IS_CONTENT = "MATCH (n) RETURN true LIMIT 1"
DROP_CONTENT = "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"

CUSTOMER_INDEX = "CREATE INDEX cust_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id) "
TERMINAL_INDEX = "CREATE INDEX term_id IF NOT EXISTS FOR (t:Terminal) ON (t.terminal_id) "

def load_customers(PATH):
    return (
            "USING PERIODIC COMMIT 1000 " +
            "LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "CREATE (c:Customer {customer_id : toInteger(row.customer_id), " +
            "x_customer_id : toFloat(row.x_customer_id)," + 
            "y_customer_id : toFloat(row.y_customer_id), " +
            "mean_amount : toFloat(row.mean_amount), " + 
            "std_amount : toFloat(row.std_amount), " + 
            "mean_nb_tx_per_day : toFloat(row.mean_nb_tx_per_day) })")

def load_terminals(PATH):
    return (
            "USING PERIODIC COMMIT 1000 " +
            "LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "CREATE (t:Terminal { terminal_id : toInteger(row.terminal_id), " +
            "x_terminal_id : toFloat(row.x_terminal_id), " +
            "y_terminal_id : toFloat(row.y_terminal_id) })")

def load_transactions(PATH):
    return (
            "USING PERIODIC COMMIT 1000 " +
            "LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " +  
            "MATCH (c:Customer {customer_id : toInteger(row.customer_id)}), " +
            "(t:Terminal {terminal_id : toInteger(row.terminal_id)}) " +
            "CREATE (c)-[tx:TRANSACTION { " + 
            "transaction_id : toInteger(row.transaction_id), " +
            "tx_datetime : datetime({epochSeconds: toInteger(row.tx_datetime)}), " +
            "tx_amount : toFloat(row.tx_amount), " + 
            "tx_fraud : toFloat(row.tx_fraud) }]->(t) ")
    
dataset = "1"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "ciao"))

from_root = str(pathlib.Path(__file__).parent.resolve())
file_path = "file://" + from_root + "/datasets/" + dataset + "/"

with driver.session() as session:
    while session.run(IS_CONTENT).value(default=False):
        session.run(DROP_CONTENT)

    session.run(CUSTOMER_INDEX)
    session.run(TERMINAL_INDEX)

    session.run(load_customers(file_path + "customers-" + dataset + ".csv"))
    session.run(load_terminals(file_path + "terminals-" + dataset + ".csv"))
    session.run(load_transactions(file_path + "transactions-" + dataset + ".csv"))

    print("write transaction " + "failed" if session._state_failed else "completed")

driver.close()
