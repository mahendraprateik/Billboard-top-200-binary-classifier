from pylab import * 
import pandas as pd
import numpy as np
from sklearn import svm
from sklearn import grid_search
from sklearn import cross_validation
from sklearn import feature_selection
from sklearn.pipeline import Pipeline
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import auc

#Importing the dataset and assigning the dependent and independent variables
df = pd.read_csv('fullnorm.csv')
X = df.iloc[:,[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]]
Y = df.iloc[:,[1]]

pipe=Pipeline([
    #('selector', feature_selection.SelectKBest(feature_selection.f_classif)),
    ('learner', svm.SVC()),
])

params={
    #'selector__k': (30,50,100,250),
    'learner__C': 10.**arange(-5,6,1),
    'learner__gamma': 10.**arange(-6,6,1),
}

"""
params={
    #'selector__k': (30,50,100,250),
    'learner__C': 10.**arange(-5,6,2),
    'learner__gamma': 10.**arange(-6,6,2),
}
"""

#from Data import *
def myscore(model, a, b):
    p=model.predict(a)
    return precision_recall_fscore_support(b, p, average='binary')[2]

params_scores=[]
for param in grid_search.ParameterGrid(params):
    pipe.set_params(**param)
    scores=cross_validation.cross_val_score(pipe, X, Y, cv=cross_validation.ShuffleSplit(len(Y), 10, 0.1, 0.5), scoring = myscore)
    params_scores.append([param, mean(scores), std(scores)])
    print(params_scores[-1])

