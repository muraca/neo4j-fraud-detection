import numpy as np
import random
from datetime import datetime  
import os
import csv

PERIODS = ['morning', 'afternoon', 'evening', 'night']
KINDS = ['high-tech', 'food', 'clothing', 'consumable', 'other']


def generate_customer_profiles_table(n_customers, random_state=0):
    
    np.random.seed(random_state)
        
    customer_id_properties=[]
    
    # Generate customer properties from random distributions 
    for customer_id in range(n_customers):
        
        x_customer_id = np.random.uniform(0,100)
        y_customer_id = np.random.uniform(0,100)
        
        mean_amount = np.random.uniform(5,100) # Arbitrary (but sensible) value 
        std_amount = mean_amount/2 # Arbitrary (but sensible) value
        
        mean_nb_tx_per_day = np.random.uniform(0,4) # Arbitrary (but sensible) value 
        
        customer_id_properties.append([customer_id, x_customer_id, y_customer_id, mean_amount, std_amount, mean_nb_tx_per_day])

    return customer_id_properties

def generate_terminal_profiles_table(n_terminals, random_state=0):
    
    np.random.seed(random_state)
        
    terminal_id_properties=[]
    
    # Generate terminal properties from random distributions 
    for terminal_id in range(n_terminals):
        
        x_terminal_id = np.random.uniform(0,100)
        y_terminal_id = np.random.uniform(0,100)
        
        terminal_id_properties.append([terminal_id, x_terminal_id, y_terminal_id])

    return terminal_id_properties

def generate_transactions_table(n_transactions, customers, terminals, start_date = datetime(2022, 1, 1), nb_days = 40):
    customer_transactions = []
    
    for n in range(n_transactions):
        day = random.randint(0, nb_days - 1)
        customer_id = random.randint(0, len(customers) - 1)
        customer_profile = customers[customer_id]
        
        terminal_id = random.randint(0, len(terminals) - 1)
        
        # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that 
        # most transactions occur during the day.
        time_tx = 0

        # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
        while time_tx < 0 and time_tx > 86400:
            time_tx = int(np.random.normal(86400/2, 20000))
                    
        # Amount is drawn from a normal distribution  
        amount = np.random.normal(customer_profile[3], customer_profile[4])
        
        # If amount negative, draw from a uniform distribution
        if amount<0:
            amount = np.random.uniform(0,customer_profile[3]*2)
        
        amount=np.round(amount,decimals=2)

        fraud = random.randint(0,1) == 1

        period_of_the_day = PERIODS[random.randint(0,len(PERIODS)-1)]
        kind_of_products = KINDS[random.randint(0,len(KINDS)-1)]
        
        
        customer_transactions.append([n, time_tx + day * 86400 + int(start_date.timestamp()), customer_id, terminal_id, amount, fraud, period_of_the_day, kind_of_products])

    return customer_transactions


print("generating...")
os.mkdir("datasets")
sizes = [1,2,4]
for size in sizes:
    customers = generate_customer_profiles_table(200000 * size, 987654)
    terminals = generate_terminal_profiles_table(200000 * size, 123125)
    transactions = generate_transactions_table(500000 * size, customers, terminals)
    
    s = str(size)
    dir = 'datasets/' + s
    os.mkdir(dir)

    cs = dir + '/customers-' + s + '.csv'
    ts = dir + '/terminals-' + s + '.csv'
    tr = dir + '/transactions-' + s + '.csv'

    with open(cs, 'w', newline='') as csvfile:
        w = csv.writer(csvfile, delimiter=',')
        w.writerow(['customer_id', 'x_customer_id', 'y_customer_id', 'mean_amount', 'std_amount', 'mean_nb_tx_per_day'])
        w.writerows(customers)

    with open(ts, 'w', newline='') as csvfile:
        w = csv.writer(csvfile, delimiter=',')
        w.writerow(['terminal_id', 'x_terminal_id', 'y_terminal_id'])
        w.writerows(terminals)

    with open(tr, 'w', newline='') as csvfile:
        w = csv.writer(csvfile, delimiter=',')
        w.writerow(['transaction_id', 'tx_datetime', 'customer_id', 'terminal_id', 'amount', 'fraud', 'period_of_the_day', 'kind_of_products'])
        w.writerows(transactions)

    size = os.path.getsize(cs) + os.path.getsize(ts) + os.path.getsize(tr)
    print(s + " tot size: " + str(np.round(size/1000000, 2)) + " MB")
