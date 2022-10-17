from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class gas_2(MRJob):

	
	def mapper_1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 9:
				block = float(fields[0])
				difficulty = float(fields[3])
				gas_used = float(fields[6])
				time_field = int(fields[7])
				month = time.strftime("%m", time.gmtime(time_field))   
				year = time.strftime("%Y", time.gmtime(time_field)) 
				time_pair = (year, month)
				value_record = (0, time_pair, gas_used, difficulty) #yield gas used, time stamp and its difficulty
				yield (block, value_record)

			elif len(fields) == 5:
				block = float(fields[3])
				yield (block, (1,"In")) #existance check
		except:
			pass

	def reducer_1(self,_,counts):

		value_list = []
		flag = False

		for i in counts:

			if i[0]==0 :
				value_list.append((i[1],i[2], i[3]))
			elif i[0]==1:
				flag = True
				

		if flag:
			if value_list :
				for j in value_list:
					yield (j[0],(j[1],j[2])) #reducing intersecting blocks to their values

	def mapper_2(self, key,values):
		yield (key, (values[0], values[1], 1))

	def reducer_2(self,word,counts):

		counting = 0
		total_dif = 0
		total_gas = 0
		for i in counts :
			total_gas += i[0]
			total_dif += i[1]
			counting+= i[2]

		avg_dif = float(total_dif)/ float(counting)
		avg_gas = float(total_gas)/ float(counting)

		yield(word,(avg_gas, avg_dif)) #averaging total difficulty and total gas used

										


	def combiner_2(self,word,counts):
	
		counting = 0
		dif_total = 0
		gas_total = 0
		for each in counts :
			gas_total += each[0]
			dif_total += each[1]
			itc += each[2]

		interm_vals = (gas_total, dif_total, counting)

		yield(word,interm_vals)

	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, combiner = self.combiner_2, reducer = self.reducer_2)]




if __name__ == '__main__':

	gas_2.run()
