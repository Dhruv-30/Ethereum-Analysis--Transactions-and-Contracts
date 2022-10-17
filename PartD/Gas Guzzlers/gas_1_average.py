from mrjob.job import MRJob
import time

class gas_avg(MRJob):

	
	def mapper(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 7:
				gas_price = float(fields[5])
				time_field = int(fields[6])
				month = time.strftime("%m", time.gmtime(time_field)) # extract month
				year = time.strftime("%y", time.gmtime(time_field)) #extract year
				time_key = (year, month)
				value_tuple = (gas_price, 1)
				yield (time_key, value_tuple)
		except:
			pass

	def reducer(self,word,counts):

		check = 0
		value_tot = 0
		for catch in counts :
			value_tot+= catch[0]
			check+= catch[1]

		averages = float(value_tot)/ float(check) #aggregating gas over months

		yield(word,averages)





if __name__ == '__main__':

	gas_avg.run()
