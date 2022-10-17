from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class scam_lucrative(MRJob):
	def mapper_1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				to_address = fields[2] 
				value = float(fields[3]) #Value transferred in Wei
				yield to_address, (value,1) 
			
			else:
				line = json.loads(lines)
				keys = line["result"] #result dictionary when json is read in python
				for i in keys:
					record = line["result"][i]
					scam_category = record["category"]
					addresses = record["addresses"]

					for j in addresses:
						yield j, (scam_category,2) 

		except:
			pass

	def reducer_1(self, key, values): #aggregating transaction value of scams to each address since multiple addresses can be involved in more than 1 scam
		transaction_value=0
		category=None

		for k in values:
			if k[1] == 1:
				transaction_value = transaction_value + k[1]
			else:
				category = k[0]
		if category is not None:
			yield category, transaction_value #finding the intersection of addresses involved in scams along with their transaction values 

	def mapper_2(self,key,value):
		yield(key,value)

	def reducer_2(self, key, value):
		yield(key,sum(value)) #aggregation of all the transaction value to each category

	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == '__main__':
	scam_lucrative.run()
				
				
	
	
