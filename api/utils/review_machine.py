import numpy as np
from collections import OrderedDict
from rlr import lr as learner

class ReviewMachine(object):
    def __init__(self, clusters):
        n_attributes = 4
        self.example_dtype = [('id', 'S36', 1), 
                              ('attributes', 'f4', n_attributes), 
                              ('label', 'f4', 1),
                              ('score', 'f4', 1),
                              ('viewed', 'i4', 1)]
        self.examples = np.fromiter(self.row_generator(clusters), 
                                       dtype=self.example_dtype)
        self.weight = np.ones(n_attributes), 0

        seed_positive, seed_negative = np.random.choice(self.examples['id'],
                                                        2)
        self.labels = OrderedDict(((seed_positive, 1),
                                   (seed_negative, 0)))

        
       
    def scoreCluster(self, scores):
        #http://stats.stackexchange.com/questions/20108/link-between-variance-and-pairwise-distances-within-a-variable

        N = len(scores)

        scores = np.array(scores)
        scores = 1 - scores
        scores *= (N - 1)

        score = (np.sum(scores)**2)/(2*N**2)
        score = (1 - np.sqrt(score))
        return score

    def row_generator(self, clusters):
        for row in clusters:
            cluster_score = self.scoreCluster(row[1])
            yield (row[0], 
                   (len(row[1]), cluster_score, max(row[1]), min(row[1])), 
                   np.nan, 
                   np.nan, 
                   0,)

    def label(self, entity_id, cluster_label):

        byte_eid = entity_id.encode('utf-8')

        self.examples['label'][self.examples['id'] == byte_eid] = cluster_label
        self.labels[byte_eid] = cluster_label

        ids, labels = list(zip(*list(self.labels.items())[-60:]))
    
        attributes = self.examples['attributes'][np.in1d(self.examples['id'],
                                                            ids)]

        if 1 in labels and 0 in labels :
            self.weight = learner(labels, attributes, 1)
        self._score()
        return self.weight
    
    def _score(self):
        weights, bias = self.weight
        self.examples['score'] = np.dot(self.examples['attributes'], weights)

    def predict(self, example):
        if self.weight is not None:
            weights, bias = self.weight
            score = np.dot(example, weights)
            score = np.exp(score + bias) / ( 1 + np.exp(score + bias) )
            return score
        return 0.0

    def get_next(self):
        unlabeled_idx = self.examples['viewed'] == 0
        try:
            cluster_id = self.examples['id'][unlabeled_idx]\
                    [np.argmin(self.examples['score'][unlabeled_idx])]
            self.examples['viewed'][self.examples['id'] == cluster_id] = 1
            return cluster_id
        except ValueError:
            return None

    def predict_remainder(self, threshold=0.5):
        weights, bias = self.weight
        unlabeled = np.isnan(self.examples['label'])
        score = np.dot(self.examples['attributes'][unlabeled], weights)
        score = 1 / ( 1 + np.exp(-(score + bias)))
        accepted = score[score >= threshold]
        rejected = score[score < threshold]
        if len(accepted):
            false_pos = np.mean(1 - accepted) * len(accepted)
        else:
            false_pos = 0
        if len(rejected):
            false_neg = np.mean(rejected) * len(rejected)
        else:
            false_neg = 0
        return false_pos, false_neg
