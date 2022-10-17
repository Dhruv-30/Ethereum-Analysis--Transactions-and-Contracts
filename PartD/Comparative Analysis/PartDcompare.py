import pyspark
import time

start = time.time()

def transact(line):
        try:
                fields = line.split(',')
                if len(fields)!=7:
                        return False
                int(fields[3])
                return True

        except:
                return False

def contract(line):
        try:
                fields = line.split(',')
                if len(fields)!=5:
                        return False
                return True
        except:
                return False

sc = pyspark.SparkContext()
#Transactions Processing

transactions = sc.textFile("/data/ethereum/transactions")

true_transaction = transactions.filter(transact) #getting required transactions

map_transaction = true_transaction.map(lambda tr : (tr.split(',')[2], int(tr.split(',')[3])))

aggregate_transaction = map_transaction.reduceByKey(lambda x,y : x+y)

#Contracts processing

contracts = sc.textFile("/data/ethereum/contracts")
valid_contracts = contracts.filter(contract) #getting required contracts
map_contracts = valid_contracts.map(lambda cn: (cn.split(',')[0], None))
joined_contracts = aggregate_transaction.join(map_contracts) #transaction + contract merge

top10 = joined_contracts.takeOrdered(10, key = lambda t: -t[1][0])

end = time.time()
totaltime = end-start


with open('PARTDCOMPARISON_run_5.txt', 'w') as f:
        for value in top10:
                f.write("{}:{}\n".format(value[0],value[1][0]))

	f.write("\n" + str(totaltime))



