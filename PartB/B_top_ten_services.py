from mrjob.job import MRJob
from mrjob.step import MRStep

class top_ten_service(MRJob):
	def mapper_1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				address1 = fields[2]
				value = int(fields[3]) #to_address
				yield address1, (1,value) #aggregate values in transaction
			elif len(fields) == 5:
				address2 = fields[0] #address
				yield address2, (2,1) #assign 2 to contracts dataset addresses
		except:
			pass
	def reducer_1(self, key, values): #validity check between two datasets, get intersection
		check = False
		everything = []
		for i in values:
			if i[0]==1:
				everything.append(i[1])
			elif i[0] == 2:
				check = True
		if check:
			yield key, sum(everything)

	def mapper_2(self, key,value): #aggregation of joint
		yield None, (key,value)

	def reducer_2(self, _, keys):
		sortedvalues = sorted(keys, reverse = True, key = lambda x: x[1])
		for i in sortedvalues[:10]:
			yield i[0], i[1]

	def steps(self): #assign job steps
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == '__main__':
	top_ten_service.run()
			
