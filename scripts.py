from neo4j import GraphDatabase

uri = "http://localhost:7474"
driver = GraphDatabase.driver(uri, auth=("neo4j", ""))

def drop_content(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def create_customer(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "CREATE (:Customer {customer_id : row.customer_id, x_customer_id : row.x_customer_id, " + 
            "y_customer_id : row.y_customer_id, mean_amount : row.mean_amount, " +
            "std_amount : row.std_amount, mean_nb_tx_per_day : row.mean_nb_tx_per_day})")

def create_terminal(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "CREATE (:Terminal {terminal_id : row.terminal_id, x_terminal_id : row.x_terminal_id, " + 
            "y_terminal_id : row.y_terminal_id})")

def create_transaction(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " + 
            "MERGE (c:Customer {customer_id : row.customer_id}) " +
            "MERGE (t:Terminal {terminal_id : row.terminal_id}) " +
            "MERGE (c)-[tx:TRANSACTION {transaction_id : row.transaction_id}]->(t) " +
            "SET tx += {tx_datetime : datetime(row.tx_datetime), tx_amount : row.tx_amount, " +
            "tx_fraud : row.tx_fraud, period_of_the_day : row.period_of_the_day " +
            "kind_of_products : tx.kind_of_products}")
            
s = "1"
file_path = "file:///datasets/" + s + "/"
with driver.session() as session:
    session.write_transaction(drop_content)
    session.write_transaction(create_customer, file_path + "customers-" + s + ".csv")
    session.write_transaction(create_terminal, file_path + "terminals-" + s + ".csv")
    session.write_transaction(create_transaction, file_path + "transactions-" + s + ".csv")

driver.close()
