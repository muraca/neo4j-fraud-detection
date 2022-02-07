# 1. Generation of the datasets
We have modified the generation scripts in order to fulfill our needs.  
The file *generator.py* contains the methods for the random generation of the datasets.  
The generated data is stored in csv files, and located in subfolders of the *datasets* folder.  
Users can choose the size of the datasets in multiple of 50 MB, by changing the values inside the *sizes* array.  
For example, the folder *2* will contain a dataset of size ≈ 100 MB.

# 2. Conceptual model
Considering the requirements, we have defined the *Customer* and *Terminal* classes.  

Each *Customer* will be defined by the following properties:
-   **customer_id**: The customer unique ID
-   x_customer_id: latitude of the coordinates of the customer's location
-   y_customer_id: longitude of the coordinates of the customer's location
-   mean_amount : The mena of the transaction amounts for the customer, drawn from a uniform distribution (5,100).
-   std_amount: The standard deviation of the transaction amounts for the customer, the mean_amount divided by two.
-   mean_nb_tx_per_day: The average number of transactions per day for the customer, drawn from a uniform distribution (0,4).

And the *Terminal* will have the following properties:
-   **terminal_id**: The terminal ID
-   x_terminal_id: latitude of the coordinates of the terminal's location
-   y_terminal_id: longitude of the coordinates of the terminal's location 

*(locations are defined in a 100 x 100 grid)*


A *Transaction* is the product of an operation made from a *Customer* on a *Terminal*, 
so we decided to represent it as an association entity, with the following properties:
-   **transaction_id**: A unique identifier for the transaction.
-   tx_datetime: Date and time at which the transaction occurs.
-   *customer_id*: The unique identifier for the customer. 
-   *terminal_id*: The unique identifier for the terminal.
-   tx_amount: The amount of the transaction.
-   tx_fraud: A binary variable, false for a legitimate transaction, or true for a fraudulent transaction.


Since the requirements didn't specify any constraint, the association is a many-to-many 
because in the real world a customer can make any number of transactions, 
and in a terminal can be made any number of transactions.  
The attributes are the same described in the original dataset.
We omitted the customer_id and terminal_id in *Transaction*,
because they are implicit in the many-to-many association.
![UML](./pictures/UML.png)


For the sake of completeness, we decided to make a conceptual model of the domain with the required extensions 
of period_of_the_day and kind_of_products for *Transaction*, and buying_friend relationship for *Customer*.
extension for *Customer*:
-   buying_friend: customers who made more than three transactions related to the same types of products 
    on the same terminal
extension for *Transaction*:
-   PERIOD_OF_THE_DAY: A string containing the period of the day {morning, afternoon, evening, night}
    in which the transaction has been executed.
-   KIND_OF_PRODUCTS: A string containing the kind of products that have been bought through the transaction
    {high-tech, food, clothing, consumable, other}.
![UML-Extended](./pictures/UML-Extended.png)

# 3. Logical model
We chose Neo4j as NoSQL system because of its nature: 
the existing model could be easily represented as a property graph, 
where the needed queries could be efficiently executed on graph links.  
The *Customer* and *Terminal* classes are represented by nodes, because they are independent,
while the *Transaction* association is a link between a Customer and a Terminal because it depends on them.
![Logical-Model-Graph](./pictures/logical-model-graph.png)

Here is the graph representation of some mock data:
![Logical-Model-Graph-Mock](./pictures/graph.svg)
![Logical-Model-Graph-Mock-Legend](./pictures/graph-legend.png)


The logical model was extended to represent the additional requirements:
- buying_friend becomes an edge between two Customer nodes
- period_of_the_day and kind_of_products become new attributes of the Transaction relationship
![Logical-Model-Extended-Graph](./pictures/logical-model-extended-graph.png)

Here is the graph representation of some mock data:
![Logical-Model-Extended-Graph-Mock](./pictures/graph-extended.svg)
![Logical-Model-Extended-Graph-Mock-Legend](./pictures/graph-extended-legend.png)

# 4. Loading of the datasets
Python can communicate with Neo4j using the *neo4j* module.  
The loading of the datasets on the Neo4j system is done by a Python script contained in the *loading.py* file.  
Users can choose the dataset to load by setting the *dataset* variable accordingly, 
and change the database URI based on their host, and the authentication credentials.  
``` python
dataset = "1"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(username, password))
```

The paths of the datasets are computed based on the chosen dataset,
and a connection session is created.
``` python
from_root = str(pathlib.Path(__file__).parent.resolve())
file_path = "file://" + from_root + "/datasets/" + dataset + "/"

with driver.session() as session:
    session.write_transaction(drop_content)
    session.write_transaction(create_customer, file_path + "customers-" + dataset + ".csv")
    session.write_transaction(create_terminal, file_path + "terminals-" + dataset + ".csv")
    session.write_transaction(create_transaction, file_path + "transactions-" + dataset + ".csv")
    print("write transaction " + "failed" if session._state_failed else "completed")

driver.close()
```

The first step is the deletion of the data contained in the Neo4j database, using the following method:
``` python
def drop_content(tx):
    tx.run("MATCH (n) DETACH DELETE n")
```
*tx* is the transaction object passed by the Neo4j session.


Then the *Customer* and *Terminal* nodes are created by loading the CSV files on Neo4j using Cypher's constructs.
``` python
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
```

Finally, the *Transaction* edges are created by matching the respective *Customer* and *Terminal* nodes involved in the operation.
``` python
def create_transaction(tx, PATH):
    tx.run ("LOAD CSV WITH HEADERS FROM \"" + PATH + "\" AS row " +  
            "MATCH (c:Customer {customer_id : row.customer_id}) " +
            ", (t:Terminal {terminal_id : row.terminal_id}) " +
            "CREATE (c)-[tx:TRANSACTION { transaction_id : row.transaction_id, " +
            "tx_amount : row.tx_amount, tx_fraud : row.tx_fraud, " +    
            "tx_datetime : datetime({epochSeconds: toInteger(row.tx_datetime)}) }]->(t) ")
```


# 5. Implementation of the required queries
The required queries are reported here in the Cypher language, and implemented as methods in the file *queries.py*

1.  For each customer, identify the amount that they have spent for every day of the current month.
```
MATCH (c:Customer)-[t:TRANSACTION]->()  
WHERE t.tx_datetime.year = datetime().year AND t.tx_datetime.month = datetime().month
RETURN c.customer_id, t.tx_datetime.day, SUM (t.tx_amount)
ORDER BY c.customer_id, t.tx_datetime.day
```

2.  For each terminal identify the possible fraudulent transactions.  
    The fraudulent transactions are those whose import is higher
    than 50% of the import of the transactions 
    executed on the same terminal in the last month.

    #TODO ask teacher

    For this request, two different Cypher queries are needed.
    The results of the first query are used by the Python script to call the second one.

    a.  For each terminal, identify the average import of the transactions executed in the last month.
    ```
    MATCH ()-[lt:TRANSACTION]->(tr:Terminal)
    WHERE
        (datetime().month = 1 AND lt.tx_datetime.month = 12 AND lt.tx_datetime.year = datetime().year - 1)
    OR 
        (datetime().month > 1 AND lt.tx_datetime.month = datetime().month - 1 AND lt.tx_datetime.year = datetime().year)
    RETURN tr.terminal_id, AVG(lt.tx_amount) as average 
    ```
    If the current month is January, then the query checks the transactions from December of the last year.
    Otherwise, it simply checks the transactions of the last month and same year.  

    The parameters of the first query, *$terminal_id* and *$average*, are passed as arguments to the second query.

    b.  Given a terminal_id and the average import of the transactions executed in the last month on that terminal,
        return the possible fraudulent transactions executed in the current month. 
        
    ```
    MATCH ()-[t:TRANSACTION]->(tr:Terminal {terminal_id: $terminal_id})
    WHERE t.tx_datetime.year = datetime().year AND t.tx_datetime.month = datetime().month AND t.tx_amount > $average / 2
    RETURN t
    ```

3.  Given a user u, determine the “co-customer-relationships CC of degree k”.
    A user u’ is a co-customer of u if you can determine a chain “u1-t1-u2-t2-...tk-1-uk“ such that u1=u, uk=u’,
    and for each 1<=I,j<=k, ui <> uj, and t1,..tk-1 are the terminals on a transaction has been executed.
    Therefore, CCk(u)={u’| a chain exists between u and u’ of degree k}

    For this operation, an additional link is needed.

    a.  Create the co-customer link for degree k=1 (if not exists)
    ```
    MATCH (u1:Customer)-[]->(t:Terminal)<-[]-(u2:Customer)
    WHERE u1.customer_id > u2.customer_id AND NOT ((u1)-[:CO_CUSTOMER]-(u2))
    MERGE (u1)-[:CO_CUSTOMER]->(u2)
    ```
    The customer_ids need to be different, because a *Customer* can't be co-customer of itself.  
    However, if the <> operator is used, Neo4j would create two different edges for the relationship.  
    We want to avoid this because it would make possible to visit a node multiple times,
    so we use the > operator instead to achieve uniqueness.
    

    b.  Match the co-customer link of degree $k
    ```
    MATCH (u1)-[:CO_CUSTOMER*$k]-(u2)
    WHERE u1.customer_id > u2.customer_id
    RETURN u1.customer_id, u2.customer_id
    ```
    The operator * in the association indicates how deep the recursion should go on the CO_CUSTOMER link.
    The match need to have no direction because of situations like:
    ```
    {id : 5} -> {id : 4} <- {id : 6}
    ```
    And the operator > is used instead of the <> to avoid duplicate results.

4.  Extend the logical model. See *Logical Model* for more details.
    a.  Extend *Transaction* with random values for period_of_the_day, kind_of_products.
    ``` python
    import random

    PERIODS = ['morning', 'afternoon', 'evening', 'night']
    KINDS = ['high-tech', 'food', 'clothing', 'consumable', 'other']

    def extend_transactions(session):
        ids = session.run("MATCH ()-[t:TRANSACTION]->() RETURN t.transaction_id")
        for id in ids:
            period_of_the_day = PERIODS[random.randint(0,len(PERIODS)-1)]
            kind_of_products = KINDS[random.randint(0,len(KINDS)-1)]
            real_id = id.get("t.transaction_id")
            session.run("MATCH ()-[t:TRANSACTION {transaction_id : $id}]-() " + 
                        "SET t += { period_of_the_day : $period_of_the_day, " + 
                        " kind_of_products : $kind_of_products }",
                        id=real_id, period_of_the_day=period_of_the_day, kind_of_products=kind_of_products)
    ```

    b. Add the *buying_friends* link.
    ```
    MATCH (c1:Customer)-[tx:TRANSACTION]->(t)<-[]-(c2:Customer),
    (c1)-[t1:TRANSACTION{kind_of_products : tx.kind_of_products}]->(t), 
    (c2)-[t2:TRANSACTION{kind_of_products : tx.kind_of_products}]->(t)
    WITH c1, c2, COUNT(DISTINCT t1) AS count_1, COUNT(DISTINCT t2) AS count_2
    WHERE c1.customer_id > c2.customer_id AND count_1 > 3 AND count_2 > 3
    MERGE (c1)-[:BUYING_FRIEND]->(c2)
    ```


    Given a *Customer* with id customer_id, determine its buying friends
    and return a list containing their customer_ids
    ```
    MATCH (c:Customer {customer_id : $customer_id})-[:BUYING_FRIEND]-(u)
    RETURN c.customer_id, COLLECT(u.customer_id)
    ```

5.  For each period of the day identifies the number of transactions that occurred in that period,
    and the *AVERAGE* number of fraudulent transactions __for that month__.

    ```
    MATCH ()-[t:TRANSACTION]->()
    RETURN t.period_of_the_day, count(t) as transactions_count, count(CASE WHEN t.tx_fraud = true THEN 1 END) as fraud_count
    ```
# 6. Performances discussion



### TODO change file names and create queries.py