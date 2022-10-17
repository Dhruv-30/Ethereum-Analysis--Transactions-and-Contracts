from mrjob.job import MRJob
from mrjob.step import MRStep

class top_ten_miner(MRJob):
	def mapper_1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9: #blocks dataset
				miner_add = fields[2]
				size = fields[4]
				yield (miner_add, int(size)) #mapping all addresses to their sizes

		except:
			pass

	def reducer_1(self, miner_add, size):
		try:
			yield(miner_add, sum(size)) #aggregating sizes to addresses

		except:
			pass


	def mapper_2(self, miner_add, sum_size):
		try:
			yield(None, (miner_add, sum_size)) #aggregating miner addresses and size with "None" key
		except:
			pass

	def reducer_2(self, _, msize):
		try:
			sortsize = sorted(msize, reverse = True, key = lambda x:x[1]) #sorting
			for i in sortsize[:10]:
				yield(i[0],i[1])
		except:
			pass
	

	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == '__main__':
	top_ten_miner.run()	
	
		

