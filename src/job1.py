
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.util import log_to_stream, log_to_null
from mrjob.protocol import JSONValueProtocol
from mr3px.csvprotocol import CsvProtocol
import csv
import logging

log = logging.getLogger(__name__)

class MrJob1(MRJob):
    
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = CsvProtocol
    
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):  
        log_to_stream(name='mrjob', debug=verbose, stream=stream)
        log_to_stream(name='__main__', debug=verbose, stream=stream)

    def mapper(self, _, line):
        category = line.get('category')
        if category is not None:
            yield (category, 1)
        yield ('#reviews', 1)
    def combiner(self, key, valuelist):
        total = sum(valuelist)
        yield (key, total)

    def reducer(self, key, valuelist):
        total = sum(valuelist)
        yield None, (key, total)  

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper, 
                combiner = self.combiner,
                reducer = self.reducer)
        ]

if __name__ == '__main__':
    MrJob1.run()
