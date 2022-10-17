from typing import Counter
from mrjob.job import MRJob
import time

class ethereum_fork(MRJob):
    def mapper(self,_,lines):
        try:
            field = lines.split(',')
            if (len(field) == 7):
                date = time.gmtime(float(field[6]))
                g_price = float(field[5])
                if (date.tm_year== 2017 and date.tm_mon== 10): #Byzantium Fork 16th October 2017
                    yield((date.tm_mday), (1,g_price))

        except:
            pass


    def reducer(self, key, value): #Aggregating number of transactions and gas price over each day of month
        counter = 0
        total_val = 0
        for i in value:
            counter+= i[0]
            total_val+= i[1]
        yield (key, (counter, total_val))


    def combiner(self, key, value):
        counter = 0
        total_val = 0
        for i in value:
            counter+= i[0]
            total_val+= i[1]
        yield (key, (counter, total_val))


if __name__=='__main__':
    ethereum_fork.run()
