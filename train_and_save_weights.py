from pyspark import SparkConf,SQLContext,SparkContext
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
import sys

def main(inputs,outputs,sc):
    
	path_train_data =inputs+'/data_training'
	path_test_data= inputs +'/data_test'
	train_data = MLUtils.loadLabeledPoints(sc, path_train_data).repartition(sc.defaultParallelism * 20)
	test_data = MLUtils.loadLabeledPoints(sc, path_test_data).repartition(sc.defaultParallelism * 20) 
	#label and vector
	model = NaiveBayes.train(train_data, 100.0)

	
	# Make prediction and test accuracy.
	predictionAndLabel = train_data.map(lambda p: (model.predict(p.features), p.label))
	accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / train_data.count()
	print('model accuracy {}'.format(accuracy))
	model.save(sc, outputs+'/rating_predictor')


if __name__ == "__main__":
    inputs=sys.argv[1]
    output=sys.argv[2]
    conf = SparkConf().setAppName('train model')
    sc = SparkContext(conf=conf)
    main(inputs,output,sc)