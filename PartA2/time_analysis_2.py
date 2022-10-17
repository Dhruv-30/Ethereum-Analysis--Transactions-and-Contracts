from mrjob.job import MRJob
import time

class time_analysis_2(MRJob):

	def mapper(self,_, line):
		fields = line.split(',')
		try:
			if len(fields)==7:
				timefield = int(fields[6]) #get time field
				gas_price = int(fields[5]) #get gas price
				months = time.strftime("%m", time.gmtime(timefield)) #get months
				years = time.strftime("%y", time.gmtime(timefield)) #get year
				yield((months,years),(gas_price,1)) #yield time stamp and gas price,1st field
		except:
			pass
	def reducer1(self, date, price): #date price count
		average = 0
		count = 0
		for p, b in price:
			average = (average*count+p*b)/(count + b)
			count = count +b
		return(date, (average,count))

	def combiner(self, date, price):
		yield self.reducer1(date,price)


	def reducer2(self,date,price):     #date average count
		date, (average,count) = self.reducer1(date,price)
		yield(date,average)

if __name__ == '__main__':
	time_analysis_2.run()
