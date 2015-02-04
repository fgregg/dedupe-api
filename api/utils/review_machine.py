import numpy
import rlr
from collections import OrderedDict

class ReviewMachine(object):
    def __init__(self, clusters):
        self.example_dtype = [('id', 'S36', 1), 
                              ('attributes', 'f4', 2), 
                              ('label', 'f4', 1),
                              ('score', 'f4', 1),
                              ('viewed', 'i4', 1)]
        self.examples = numpy.fromiter(self.row_generator(clusters), 
                                       dtype=self.example_dtype)
        self.weight = numpy.array([0,0]), 0
        self.labeled_count = 0
        
    def row_generator(self, clusters):
        for row in clusters:
            yield (row[0], row[1:3], numpy.nan, 0, 0,)

    def label(self, entity_id, label):

        self.examples['label'][self.examples['id'] == entity_id] = label

        labels = self.examples['label'][~numpy.isnan(self.examples['label'])].astype('i4')
        
        attributes = self.examples['attributes'][~numpy.isnan(self.examples['label'])]

        self.weight = rlr.lr(labels, attributes, 0.1)
        self.labeled_count += 1
        self._score()
        return self.weight
    
    def _score(self):
        weights, bias = self.weight
        self.examples['score'] = numpy.dot(self.examples['attributes'], weights)

    def predict(self, example):
        if self.weight is not None:
            weights, bias = self.weight
            score = numpy.dot(example, weights)
            score = numpy.exp(score + bias) / ( 1 + numpy.exp(score + bias) )
            return score
        return 0.0

    def get_next(self):
        unlabeled_idx = self.examples['viewed'] == 0
        try:
            cluster_id = self.examples['id'][unlabeled_idx]\
                    [numpy.argmin(self.examples['score'][unlabeled_idx])]
            self.examples['viewed'][self.examples['id'] == cluster_id] = 1
            return cluster_id
        except ValueError:
            return None

    def predict_remainder(self, threshold=0.5):
        weights, bias = self.weight
        unlabeled = numpy.isnan(self.examples['label'])
        score = numpy.dot(self.examples['attributes'][unlabeled], weights)
        score = numpy.exp(score + bias) / ( 1 + numpy.exp(score + bias) )
        accepted = score[score > threshold]
        rejected = score[score <= (1 - threshold)]
        if len(accepted):
            false_pos = numpy.mean(1 - accepted) * len(accepted)
        else:
            false_pos = 0
        if len(rejected):
            false_neg = numpy.mean(rejected) * len(rejected)
        else:
            false_neg = 0
        return false_pos, false_neg
