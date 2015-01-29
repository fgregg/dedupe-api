import numpy
import rlr
from collections import OrderedDict

class ReviewMachine(object):
    def __init__(self, entity_examples):
        """ 
        Entity examples should be a dict where the key is the entity_id
        and the value is a dict like so:

        {"<entity_id>": {
            "label": None, # None by default, will be labelled either 1 or 0 based on user input
            "attributes": [], # 2 member list: length of cluster and max confidence score from cluster
            "score": 1.0 # Calculated on the fly by mutiplying attributes by learned weight
            }
        }
        
        Score is used to sort entities on the fly

        """
        self.examples = entity_examples
        self.weight = 1.0
        self.labeled_count = 0

    def label(self, entity_id, label):

        self.examples[entity_id]['label'] = label

        labels = [d['label'] for d in self.examples.values() \
                if d['label'] is not None]
        examples = [d['attributes'] for d in self.examples.values() \
                if d['label'] is not None]

        labels = numpy.array(labels, 
                             dtype=numpy.int32)
        examples = numpy.array(examples, 
                               dtype=numpy.float32)

        self.weight = rlr.lr(labels, examples, 0.1)
        self._sort()
        self.labeled_count += 1
        return self.weight
    
    def _score(self):
        weights, bias = self.weight
        examples = [d['attributes'] for d in self.examples.values()]
        scores = numpy.dot(examples, weights)
        scores = numpy.exp(scores + bias) / ( 1 + numpy.exp(scores + bias) )
        entity_ids = [k for k,v in self.examples.items()]
        for idx, entity_id in enumerate(entity_ids):
            self.examples[entity_id]['score'] = scores.tolist()[idx]

    def _sort(self):
        self._score()
        self.examples = OrderedDict(sorted(self.examples.items(), key=lambda x: x[1]['score']))

    def predict(self, example):
        weights, bias = self.weight
        score = numpy.dot(example, weights)
        score = numpy.exp(score + bias) / ( 1 + numpy.exp(score + bias) )
        return score
    
    def get_next(self):
        for entity_id, example in self.examples.items():
            if not self.examples[entity_id].get('checked_out'):
                if example['label'] is None:
                    self.examples[entity_id]['checked_out'] = True
                    return entity_id
