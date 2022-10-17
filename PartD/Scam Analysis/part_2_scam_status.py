from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class scam_status(MRJob):
	def mapper_1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				to_address = fields[2]
				value = float(fields[3])
				yield to_address, (1,value)	#mapping address and value		
			else:
				line = json.loads(lines)
				keys = line["result"]
				for k in keys:
					record = line["result"][k] #result dictionary
					category = record["category"]
					addresses = record["addresses"]
					status = record["status"]
					for a in addresses:
						yield a, (2, category,status) #mapping address with status,category
		except:
			pass

	def reducer_1(self,_, values):
		tran_value=0
		category=None
		status = None
		for k in values:
			if k[0] == 1:
				tran_value = tran_value + k[1]
			else:
				category = k[1]
				status = k[2]
		if category is not None and status is not None:
			yield (status,category), tran_value #aggregating status, category to the intersection of address between two datasets

	def mapper_2(self,key,value):
		yield(key,value)
	def reducer_2(self, key, value):
		yield(key,sum(value)) #aggregating transacation value category/status

	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == '__main__':
	scam_status.run()
				
				
	
	
