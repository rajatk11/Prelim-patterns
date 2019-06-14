__author__ = 'rajatkathpalia'

# Predict stock moves based on cosine similarity

import numpy as np
import sklearn.pipeline
from sklearn import metrics
from sklearn.model_selection import train_test_split
import pandas as pd
import os



def read_files(folder_loc) :
    train_data_file = os.path.join(folder_loc, 'X_train.csv')
    train_labels_file = os.path.join(folder_loc, 'y_train.csv')
    test_data_file = os.path.join(folder_loc, 'X_test.csv')
    test_labels_file = os.path.join(folder_loc, 'X_test.csv')
    infer_data_file = os.path.join(folder_loc, 'X_infer.csv')

    train_data = pd.read_csv(train_data_file, header=0, index_col=0)
    train_labels = pd.read_csv(train_labels_file, header=0, index_col=0)
    test_data = pd.read_csv(test_data_file, header=0, index_col=0)
    test_labels = pd.read_csv(test_labels_file, header=0, index_col=0)
    infer_data = pd.read_csv(infer_data_file, header=0, index_col=0)



    return train_data, train_labels, test_data, test_labels, infer_data


def similar_neighbours(train_data, train_labels, test_data) :
    cosine_sim1 = sklearn.metrics.pairwise.cosine_similarity(train_data, test_data)
    test_preds_ind = []
    similarity_vals = []
    for i in range(test_data.shape[0]) :
        max_val = np.max(cosine_sim1[:, i])
        max_ind = np.where(cosine_sim1[:, i] == max_val)[0]
        ap_val = train_labels[max_ind][0][0]
        test_preds_ind.append(ap_val)
        similarity_vals.append(max_val)

    return test_preds_ind


def main() :

    max_tolerance_val = .76
    data_folder = '/Users/rajatkathpalia/Documents/Dumps/Stock_pred/similarity/'
    training_data, training_labels, test_data, test_labels, infer_data = read_files(data_folder)

    test_preds, _ = similar_neighbours(training_data, training_labels, test_data)

    test_preds = np.array(test_preds)

    cos_sim_mse = sklearn.metrics.mean_squared_error(test_preds, test_labels)

    if cos_sim_mse < max_tolerance_val :
        pred, sim_vals = similar_neighbours(training_data, training_labels, infer_data)
        print('Cosine Similarity based prediction : ', pred)
        print('Similarity : ', sim_vals)
    else :
        print('Error beyond tolerance level')

if __name__ == "__main__" : main()
