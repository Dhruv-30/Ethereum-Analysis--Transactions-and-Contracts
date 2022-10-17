from mrjob.job import MRJob
import time

class time_analysis_p1(MRJob):
	
	def mapper(self, _,line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				timefield = int(fields[6])
				months = time.strftime("%m", time.gmtime(timefield)) #getmonth
				year = time.strftime("%y", time.gmtime(timefield)) #getyear
				yield ((months,year), 1)
		except:
			pass

	def reducer(self,word,counts):
		yield(word,sum(counts))

if __name__ == '__main__':
	time_analysis_p1.run()
