# Project: Creation of a NoSQL Database for Credit Card Fraud Detection
Payment card fraud is a major challenge for business owners, payment card issuers, and
transactional services companies, causing every year substantial and growing financial losses.
Many Machine Learning (ML) approaches have been proposed in the literature that tries to 
automate the process of identifying fraudulent patterns from large volumes of data. The book 
“<a href="https://fraud-detection-handbook.github.io/fraud-detection-handbook">Machine Learning
for Credit Card Fraud detection – Practical handbook</a>” reports different approaches for 
facing the issue and for evaluating the quality of the proposed prediction results.  
For the purpose of this project, we are not interested in the application and verification of ML 
approaches but we wish to exploit the transaction data simulator code (reported in
<a href="https://fraud-detection-handbook.github.io/fraud-detection-handbook/
Chapter_3_GettingStarted/SimulatedDataset.html">Section 2 of Chapter 3 of the cited book</a>) 
for the generation of data to be exploited for feeding a NoSQL database.


The simulator has the purpose to generate 3 tables:
1.  **Customer profile**. Each customer has a unique identifier and is associated with the 
    following properties: geographical location, spending frequency, and spending amounts.
    Moreover,  the list of terminals on which the customer makes transactions is associated
    with his profile.
2.  **Terminal profile**. Terminal properties consist of a geographical location.
3.  **Transactions**. This table reports for each transaction, the customer identifier, the 
    terminal identifier, the amount that has been paid, and the date on which the transaction
    occurred. Some transactions can be marked as fraudulent.


## The following activities should be carried out:
1.  You have to use the scripts for the generation of at least 3 datasets (each one containing
    the three tables described above) at an increased size (at least 50 Mbyte, 100 Mbyte,
    200 Mbyte).
2.  Define a conceptual model for the considered domain.
3.  Choose one of the NOSQL systems considered in the course (Oracle + XML, MongoDB, Cassandra,
    Neo4J) and provide a data modeling to optimize the execution of the following operations:

    *   For each customer identifies the amount that he/she has spent for every day of the 
        current month.  
    *   For each terminal identify the possible fraudulent transactions. The fraudulent 
        transactions are those whose import is higher than 50% of the average import of the
        transactions executed on the same terminal in the last month.  
    *   Given a user u, determine the “co-customer-relationships CC of degree k”.  
        A user u’ is a co-customer of u if you can determine a chain “u1-t1-u2-t2-...tk-1-uk“
        such that u1=u, uk=u’, and for each 1<=i,j<=k, ui <> uj, and t1,..tk-1 are the terminals
        on which a transaction has been executed. Therefore, CCk(u)={u’| a chain exists between
        u and u’ of degree k}.  
        Please, note that depending on the adopted model, the computation of CCk(u) could be
        quite complicated. Consider therefore at least the computation of CC3(u) (i.e. the
        co-costumer relationships of degree 3).
    * Extend the logical model that you have stored in the NOSQL database by introducing the 
    following information (pay attention that this operation should be done once the NOSQL 
    database has been already loaded with the data extracted from the datasets):
        *   Each transaction should be extended with:
            1.  The period of the day {morning, afternoon, evening, night} in which the
                transaction has been executed.
            2.  The kind of products that have been bought through the transaction {high-
                tech, food, clothing, consumable, other}
            The values can be chosen randomly.
        *   Customers that make more than three transactions related to the same types of
            products from the same terminal should be connected as “buying_friends”.
            Therefore also this kind of relationship should be explicitly stored in the NOSQL 
            database and can be queried.
    *   For each period of the day identifies the number of transactions that occurred in that 
        period, and the average number of fraudulent transactions.
4.  Create a script for loading the considered datasets in the chosen NOSQL and develop the 
    scripts for implementing the previous operations. Take in mind that depending on the 
    identified system, primitives of the query language should be embedded in a Python script.
5.  Evaluate the execution times for all the previous operations (also the one for updating the
    NOSQL datastore) in the generated datasets.