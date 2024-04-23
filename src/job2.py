
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.util import log_to_stream, log_to_null
from mrjob.protocol import JSONValueProtocol, TextProtocol
import logging
import json
import re
from csv import reader

log = logging.getLogger(__name__)


pathway = '../data/'


# read in output from previous job as dictionary
rev_per_cat = {}
with open(pathway + 'temp.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    for row in csv_reader:
        rev_per_cat[row[0]] = int(row[1])


# read stopwords
stopwords = set(word.strip() for word in open(pathway + 'stopwords.txt'))

class MRJob2(MRJob):
    
    FILES = [pathway + 'stopwords.txt', pathway + 'temp.csv']
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        # super(ChiSquareJob, self).configure_args()
        super().configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")
    
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):  
        log_to_stream(name='mrjob', debug=verbose, stream=stream)
        log_to_stream(name='__main__', debug=verbose, stream=stream)

    def mapper_preprocessing(self, _, line):
        ''' 
        This function preprocesses the data and counts the number of times each term appears in each category
        It takes in a line of data and returns a tuple with a key-value pair where the key is a tuple of the term and the category
        and the value is 1'''
        
        review = line['reviewText']
        category = line['category']
        
        # tokenize
        tokens = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review)
        
        # case folding
        tokens = [token.lower() for token in tokens]
        
        # stopword filtering
        stopwords = None
        with open(self.options.stopwords, "r") as f:
            stopwords = set(line.strip() for line in f)
        
        filtered_tokens = [token for token in tokens if token not in stopwords]

        for token in filtered_tokens:
            yield (token, category), 1
            
    def combiner_count_term_per_category(self, key, valuelist):
        total = sum(valuelist)
        yield key, total
    
    def reducer_count_term_per_category(self, key, valuelist):
        '''
        This function takes in a key-value pair where the key is a tuple of the term and the category
        and the value is a list of 1s. It returns a key-value pair where the key is the term and the value is the total count of the term in the category
        '''
        total = sum(valuelist)
        yield key, total             

    def mapper_transform(self, key, category_count):
        '''
        This mapper creates a new key-value pair where the key is the term and the value is a dictionary with the category as the key and the count as the value
        '''
        term, category = key
        yield term, {category: category_count}

    def reducer_count_terms(self, term, list_of_dicts):
        '''
        This function takes in a key-value pair where the key is the term and the value is a list of dictionaries where each dictionary has the category as the key and the count as the value
        It returns a key-value pair where the key is the term and the value is a tuple with the total count of the term and a dictionary with the category as the key and the count as the value
        '''
        term_dictionary = {k:v for item in list_of_dicts for (k,v) in item.items()}
        values = term_dictionary.values()
        total = sum(values)
        
        yield term, (total, term_dictionary)
        
    def mapper_chi_squared_statistic(self, term, sum_dictionary):
        '''
        This function takes in a key-value pair where the key is the term and the value is a tuple with the total count of the term and a dictionary with the category as the key and the count as the value
        It returns a key-value pair where the key is the category and the value is a dictionary with the term as the key and the chi-squared value as the value
        '''
        
        count_all_cat, count_each_cat = sum_dictionary
        
        for cat, count_cat in count_each_cat.items():
            N = rev_per_cat['#reviews']         
            A = count_cat
            B = count_all_cat - A
            C = rev_per_cat[cat] - A
            D = N - rev_per_cat[cat] - B
            chi_squared = (N * ((A*D - B*C)**2)) / ((A+B) * (A+C) * (B+D) * (C+D))
            
            yield cat, {term: chi_squared}
    
    def reducer_top75_terms(self, category, term_dictionary):
        '''
        This function takes in a key-value pair where the key is the category and the value is a list of dictionaries where each dictionary has the term as the key and the chi-squared value as the value
        It returns a key-value pair where the key is the category and the value is a dictionary with the top 75 terms and their chi-squared values
        '''
        term_dictionary = {k:v for list_item in term_dictionary for (k,v) in list_item.items()}
        top75 = {k:v for k,v in sorted(term_dictionary.items(), key = lambda x: x[1], reverse = True)[:75]}     
        top75_keys = sorted(top75.keys())
        
        yield category, top75
        yield "category", top75_keys
    
    def reducer_intermediary_result(self, category, top75__terms):
        '''
        This function takes in a key-value pair where the key is the category and the value is a list of dictionaries where each dictionary has the term as the key and the chi-squared value as the value
        It returns a key-value pair where the key is the category and the value is a dictionary with the top 75 terms and their chi-squared values
        '''
        if category != "category":
            top75__terms = {k:v for list_item in top75__terms for (k,v) in list_item.items()}
            top75__terms_formatted = str(top75__terms).replace(" ", "").replace(",", " ").replace("\'", "").strip("\{\}")
            yield "", (category, top75__terms_formatted)
        else:
            one_list = []
            for item in top75__terms:
                one_list.append(item)
            one_list = [l for sublist in one_list for l in sublist]
            merged_dict = str(sorted(list(set(one_list)))).replace("\'", "").replace(",", " ").strip("\[\]")
            yield "", (category, merged_dict)

    def reducer_final_result(self, _, output_list):
        '''
        This function takes in a key-value pair where the key is an empty string and the value is a list of tuples where each tuple has the category as the first element and the top 75 terms and their chi-squared values as the second element
        It returns a key-value pair where the key is the category and the value is a dictionary with the top 75 terms and their chi-squared values, sorted in alphabetic order by category
        '''
        for key, val in sorted(output_list):
            if key != "category":
                yield key, val
            else:
                yield val, ""


    def steps(self):
        return [
            MRStep(
                # preprocess and initialize with 1 the number of times each term appears in each category
                mapper  = self.mapper_preprocessing, 
                combiner = self.combiner_count_term_per_category,
                reducer = self.reducer_count_term_per_category),
            MRStep(
                # transform the data and count the total number of times each term appears
                mapper  = self.mapper_transform, 
                reducer = self.reducer_count_terms),
            MRStep(
                # calculate the chi-squared statistic for each term in each category
                mapper  = self.mapper_chi_squared_statistic,
                reducer = self.reducer_top75_terms),
            MRStep(
                # format the output
                reducer = self.reducer_intermediary_result),
            MRStep(
                # format the output
                reducer = self.reducer_final_result)
        ]

if __name__ == '__main__':
    MRJob2.run()
