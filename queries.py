from neo4j import GraphDatabase
import random

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "ciao"))


PERIODS = ['morning', 'afternoon', 'evening', 'night']
KINDS = ['high-tech', 'food', 'clothing', 'consumable', 'other']

def q1(limit = 10):
    query_1 = ( "MATCH (c:Customer)-[t:TRANSACTION]->() " +
                "WHERE t.tx_datetime.year = datetime().year " +
                "AND t.tx_datetime.month = datetime().month " +
                "RETURN c.customer_id, t.tx_datetime.day, SUM (t.tx_amount) " +
                "ORDER BY c.customer_id, t.tx_datetime.day" )
    with driver.session() as session:
        res = session.run(query_1)
        keys = [0,1,2]
        values = res.values(*keys)
        print("customer_id, day, sum(amount):")

        for v in range(limit):
            print(values[v])

    
def q2(limit=10000):
    query_2a = ("MATCH ()-[lt:TRANSACTION]->(tr:Terminal)" +
                "WHERE (datetime().month = 1 AND lt.tx_datetime.month = 12 " + 
                "AND lt.tx_datetime.year = datetime().year - 1) OR " +
                "(datetime().month > 1 AND lt.tx_datetime.month = datetime().month - 1 " +
                "AND lt.tx_datetime.year = datetime().year) " +
                "RETURN tr.terminal_id, AVG(lt.tx_amount) as average LIMIT $limit")
    
    query_2b = ("MATCH ()-[t:TRANSACTION]->(tr:Terminal {terminal_id: $terminal_id}) " +
                "WHERE t.tx_datetime.year = datetime().year AND " + 
                "t.tx_datetime.month = datetime().month AND " + 
                "t.tx_amount > $average / 2 RETURN t.transaction_id")

    with driver.session() as session:
        res_a = session.run(query_2a, limit=limit)
        keys = [0,1]
        values = res_a.values(*keys)

        res_b = {}
        for r in values:
            val = session.run(query_2b, terminal_id=r[0], average=r[1]).value()
            if val != None and len(val) > 0:
                res_b[r[0]] = val

        print("terminal_id, fraudulent_transaction_id")

        for r in res_b.keys():
            print(r, res_b[r])


def q3(customer_id=35, degree=2):
    query_3a = ("MATCH (u1:Customer)-[]->(t:Terminal)<-[]-(u2:Customer) " +
                "WHERE u1.customer_id > u2.customer_id AND NOT ((u1)-[:CO_CUSTOMER]-(u2)) " +
                "MERGE (u1)-[:CO_CUSTOMER]->(u2)")

    query_3b = ("MATCH (u1:Customer {customer_id : $customer_id})-[:CO_CUSTOMER*" + str(degree) + "]-(u2) " +
                "WHERE u1.customer_id <> u2.customer_id " +
                "RETURN DISTINCT(u2.customer_id)")

    with driver.session() as session:
        session.run(query_3a)
        res_b = session.run(query_3b, customer_id=customer_id)

        print("co-customers of " + str(customer_id) + " with degree " + str(degree) + " are: ")
        print(res_b.values(0))

def extend_transactions():
    with driver.session() as session:
        ids = session.run("MATCH ()-[t:TRANSACTION]->() RETURN t.transaction_id").value()
        for id in ids:
            period_of_the_day = PERIODS[random.randint(0,len(PERIODS)-1)]
            kind_of_products = KINDS[random.randint(0,len(KINDS)-1)]
            session.run("MATCH ()-[t:TRANSACTION {transaction_id : $id}]-() " + 
                        "SET t.period_of_the_day = $period_of_the_day, " + 
                        "t.kind_of_products = $kind_of_products",
                        id=id, period_of_the_day=period_of_the_day, kind_of_products=kind_of_products)
        
def extend_customers():
    with driver.session() as session:
        session.run("MATCH (c1:Customer)-[tx:TRANSACTION]->(t)<-[]-(c2:Customer), " +
                    "(c1)-[t1:TRANSACTION{kind_of_products : tx.kind_of_products}]->(t), " +
                    "(c2)-[t2:TRANSACTION{kind_of_products : tx.kind_of_products}]->(t) " +
                    "WITH c1, c2, COUNT(DISTINCT t1) AS count_1, COUNT(DISTINCT t2) AS count_2 " +
                    "WHERE c1.customer_id > c2.customer_id AND count_1 > 3 AND count_2 > 3 " +
                    "MERGE (c1)-[:BUYING_FRIEND]->(c2)")

def q4(customer_id=1):
    query_4 = ( "MATCH (c:Customer {customer_id : $customer_id})-[:BUYING_FRIEND]-(u) " +
                "RETURN u.customer_id")
    
    with driver.session() as session:
        res = session.run(query_4, customer_id=customer_id)
        values = res.value()
        print("buying friends of " + str(customer_id) + ": ")
        print(values)

def q5():
    query_5 = ( "MATCH ()-[t:TRANSACTION]->() " +
                "RETURN t.period_of_the_day, count(t) as transactions_count, " + 
                "count(CASE WHEN t.tx_fraud = true THEN 1 END) as fraud_count")

    with driver.session() as session:
        res = session.run(query_5)
        keys = [0,1,2]
        values = res.values(*keys)
        print(values)
    

if __name__ == "__main__":
    #call the queries
    driver.close()

