from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "ciao"))


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

    driver.close()
    
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

    driver.close()

if __name__ == "__main__":
    q2()

