Each customer will be defined by the following properties:

-   CUSTOMER_ID: The customer unique ID
-   (x_customer_id,y_customer_id): A pair of real coordinates (x_customer_id,y_customer_id)
    in a 100 * 100 grid, that defines the geographical location of the customer
-   (mean_amount, std_amount): The mean and standard deviation of the transaction amounts for the customer,
    assuming that the transaction amounts follow a normal distribution. The mean_amount will be drawn from a
    uniform distribution (5,100) and the std_amount will be set as the mean_amount divided by two.
-   mean_nb_tx_per_day: The average number of transactions per day for the customer, assuming that the number of
    transactions per day follows a Poisson distribution. This number will be drawn from a uniform distribution (0,4).
extension:
-   buying_friends: customers who made more than three transactions related to the same types of products 
    from the same 


Transaction features:

-   TRANSACTION_ID: A unique identifier for the transaction
-   TX_DATETIME: Date and time at which the transaction occurs
-   CUSTOMER_ID: The unique identifier for the customer. 
-   TERMINAL_ID: The unique identifier for the merchant (or more precisely the terminal).
-   TX_AMOUNT: The amount of the transaction.
-   TX_FRAUD: A binary variable, false for a legitimate transaction, or true for a fraudulent transaction.
extension:
-   PERIOD_OF_THE_DAY: A string containing the period of the day {morning, afternoon, evening, night}
    in which the transaction has been executed.
-   KIND_OF_PRODUCTS: A string containing the kind of products that have been bought through the transaction
    {high-tech, food, clothing, consumable, other} 


Terminal properties:

-   TERMINAL_ID: The terminal ID
-   (x_terminal_id,y_terminal_id): A pair of real coordinates (x_terminal_id,y_terminal_id) in a 100 * 100 grid,
    that defines the geographical location of the terminal
----
```
CREATE  (c1:Customer {customer_id: 0, x_customer_id: 54.881350, y_customer_id: 71.518937, mean_amount: 62.262521, std_amount: 31.131260, mean_nb_tx_per_day: 2.179533}),
        (c2:Customer {customer_id: 1, x_customer_id: 42.365480, y_customer_id: 64.589411, mean_amount: 46.570785, std_amount: 23.285393, mean_nb_tx_per_day: 3.567092}),
        (tr:Terminal {terminal_id: 0, x_terminal_id: 54.881350, y_terminal_id: 64.589411}),
        (c1)-[:TRANSACTION {transaction_id: 0, tx_datetime: datetime('2018-04-01T07:19:05'), tx_amount: 123.59, tx_fraud: false, period_of_the_day: "morning" , kind_of_products: "other"}]->(tr),
        (c1)-[:TRANSACTION {transaction_id: 1, tx_datetime: datetime('2018-04-01T19:05:20'), tx_amount: 46.51, tx_fraud: false, period_of_the_day: "evening" , kind_of_products: "other"}]->(tr),
        (c1)-[:TRANSACTION {transaction_id: 2, tx_datetime: datetime('2018-04-01T20:11:01'), tx_amount: 77.9, tx_fraud: true, period_of_the_day: "evening" , kind_of_products: "other"}]->(tr),
        (c1)-[:TRANSACTION {transaction_id: 3, tx_datetime: datetime('2018-04-01T20:15:05'), tx_amount: 45.55, tx_fraud: true, period_of_the_day: "evening" , kind_of_products: "other"}]->(tr),
        (c2)-[:TRANSACTION {transaction_id: 4, tx_datetime: datetime('2018-04-02T06:15:04'), tx_amount: 2.59, tx_fraud: false, period_of_the_day: "morning" , kind_of_products: "other"}]->(tr),
        (c2)-[:TRANSACTION {transaction_id: 5, tx_datetime: datetime('2018-04-05T07:30:05'), tx_amount: 55.11, tx_fraud: true, period_of_the_day: "morning" , kind_of_products: "other"}]->(tr),
        (c2)-[:TRANSACTION {transaction_id: 6, tx_datetime: datetime('2018-04-05T07:45:24'), tx_amount: 56.00, tx_fraud: true, period_of_the_day: "morning" , kind_of_products: "other"}]->(tr),
        (c2)-[:TRANSACTION {transaction_id: 7, tx_datetime: datetime('2018-04-06T17:21:45'), tx_amount: 11.75, tx_fraud: false, period_of_the_day: "afternoon" , kind_of_products: "other"}]->(tr)
RETURN tr
```

----

Q1  For each customer identifies the amount that he/she has spent for every day of the current month. DONE
```
MATCH (c:Customer)-[t:TRANSACTION]->()  
WHERE t.tx_datetime.year = datetime().year AND t.tx_datetime.month = datetime().month
RETURN c.customer_id, t.tx_datetime.day, SUM (t.tx_amount)
ORDER BY c.customer_id, t.tx_datetime.day
```


----

Q2  For each terminal identify the possible fraudulent transactions. //MADE IN THIS MONTH
    The fraudulent transactions are those whose import is higher     //ASK
    than 50% of the average import of the transactions 
    executed on the same terminal in the last month.

a.  For each terminal, identify the average import of the transactions executed in the last month.
```
MATCH ()-[lt:TRANSACTION]->(tr:Terminal)
WHERE (datetime().month = 1 AND lt.tx_datetime.month = 12 AND lt.tx_datetime.year = datetime().year - 1)
    OR (datetime().month > 1 AND lt.tx_datetime.month = datetime().month - 1 AND lt.tx_datetime.year = datetime().year)
RETURN tr.terminal_id, AVG(lt.tx_amount) as average
```


b.  Given a terminal_id and the average import of the transactions executed in the last month on that terminal,
    return the possible fraudulent transactions executed in the current month. 
    
```
MATCH ()-[t:TRANSACTION]->(tr:Terminal {terminal_id: $terminal_id})
WHERE t.tx_datetime.year = datetime().year AND t.tx_datetime.month = datetime().month AND t.tx_amount > $average / 2
RETURN t
```

----

Q3  Given a user u, determine the “co-customer-relationships CC of degree k”.
    A user u’ is a co-customer of u if you can determine a chain “u1-t1-u2-t2-...tk-1-uk“ such that u1=u, uk=u’,
    and for each 1<=I,j<=k, ui <> uj, and t1,..tk-1 are the terminals on a transaction has been executed.
    Therefore, CCk(u)={u’| a chain exists between u and u’ of degree k}

a.  Create the co-customer link for level 1 (if not exists)
```
MATCH (u1:Customer)-[]->(t:Terminal)<-[]-(u2:Customer)
WHERE u1.customer_id <> u2.customer_id AND NOT ((u1)-[:CO_CUSTOMER]-(u2))
MERGE (u1)-[:CO_CUSTOMER]->(u2)
```

b.  Match the co-customer link with level $level
```
MATCH (u1)-[:CO_CUSTOMER*$level]->(u2)
WHERE u1.customer_id <> u2.customer_id
RETURN u1.customer_id, u2.customer_id
```

----
Q4  Given a user u, determine its buying friends
```
MATCH (:Customer {customer_id : $u})-[:BUYING_FRIEND]-(u)
RETURN $u, COLLECT(u)
```

----
Q5  For every period of the day, identify the number of transactions that occurred in that period, 
    and the average number of fraudulent transactions. //ASK
```
MATCH ()-[t:TRANSACTION]->()
RETURN t.period_of_the_day, count(t) as transactions_count, count(CASE WHEN t.tx_fraud = true THEN 1 END) as fraud_count
```

