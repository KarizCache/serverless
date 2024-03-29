#!/usr/bin/python3 


from scipy.optimize import curve_fit
import numpy as np
import math
import pandas as pd

def power_law(x, a, b):
    return a*np.power(x, b)


def fit_deserialization():
    df = pd.read_csv('./serialization.log', 
                 names=['dims', 'size', 'serialization', 'deserialization', 'repeat'])
    pars, cov = curve_fit(f=power_law, xdata=df['size'].values, ydata=df['deserialization'].values, p0=[0, 0], bounds=(-np.inf, np.inf))
    def pw(x):
        #print(pars)
        a, b = pars
        return 0#a*math.pow(x, b)
    return pw
    


def fit_serialization():
    df = pd.read_csv('./serialization.log', 
                 names=['dims', 'size', 'serialization', 'deserialization', 'repeat'])
    pars, cov = curve_fit(f=power_law, xdata=df['size'].values, ydata=df['serialization'].values, p0=[0, 0], bounds=(-np.inf, np.inf))
    def pw(x):
        #print(pars)
        a, b = pars
        return 0 #a*math.pow(x, b)
    return pw
