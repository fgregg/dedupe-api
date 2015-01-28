import numpy
import rlr

class ReviewMachine(object):
    def __init__(self):
        self.labels = []
        self.examples = []
        self.weight = None

    def train(self, labels, examples):

        self.labels.extend(labels)
        self.examples.extend(examples)

        labels = numpy.array(self.labels, 
                             dtype=numpy.int32)
        examples = numpy.array(self.examples, 
                               dtype=numpy.float32)

        self.weight = rlr.lr(labels, examples, 0.1)
        
        return self.weight
    
    def predict(self, examples):
        
        weights, bias = self.weight
        scores = numpy.dot(examples, weights)
        scores = numpy.exp(scores + bias) / ( 1 + numpy.exp(scores + bias) )
        return scores
