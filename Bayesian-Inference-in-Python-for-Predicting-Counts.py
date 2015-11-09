### This was originally created in Ipython Notebook (Jupyter) and converted to .py
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, MetaData, Table, select
from blaze import Data,dshape
from odo import odo

from IPython.core.pylabtools import figsize
import numpy as np
import matplotlib.pyplot as plt, mpld3
get_ipython().magic(u'matplotlib inline')
mpld3.enable_notebook()

##GET Data from MYSQL which will contain metric and associated value
import scipy.stats as stats
engine=create_engine("mysql+mysqldb://username:password@localhost:3306/schema",pool_recycle=3600)
table=pd.read_sql_query("SELECT date(min_stamp) as dayte,avg(nineeighty_pctl_usrs) as daily FROM (SELECT * FROM usr.users WHERE access_technology = 'ground')a GROUP BY DATE(min_stamp)",con=engine)
min(table.dayte),max(table.dayte)

## Show an initial plot of the distribution of the Count Data
mpld3.enable_notebook()
from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages('ground.pdf')
figsize(13.5,5)
table=table.sort(['dayte'])
count_data=table['daily']
n_count_data=len(count_data)
plt.bar(np.arange(n_count_data),count_data,color="#348ABD")
plt.xlabel("Time (days)")
plt.ylabel("98th Percentile of Total Users in Sector, leading to Event")
plt.xlim(0, n_count_data)
plt.savefig(pp, format='pdf')

## Applied code taken from PYMC book found on GIT, on modeling a divergence in time series data using Bayesian. In this case it is Count Data on Users Per Day
import pymc as pm
alpha = 1.0 / count_data.mean()  # Recall count_data is the
                               # variable that holds our txt counts
lambda_1 = pm.Exponential("lambda_1", alpha)
lambda_2 = pm.Exponential("lambda_2", alpha)

tau = pm.DiscreteUniform("tau", lower=0, upper=n_count_data)


print "Random output:", tau.random(), tau.random(), tau.random()

## Tau will determine a period in the time series where the shape (count value) changes, signifying a new regime
@pm.deterministic
def lambda_(tau=tau, lambda_1=lambda_1, lambda_2=lambda_2):
    out = np.zeros(n_count_data)
    out[:tau] = lambda_1  # lambda before tau is lambda1
    out[tau:] = lambda_2  # lambda after (and including) tau is lambda2
    return out



observation = pm.Poisson("obs", lambda_, value=count_data, observed=True)

model = pm.Model([observation, lambda_1, lambda_2, tau])



mcmc = pm.MCMC(model)
mcmc.sample(40000, 10000, 1)


lambda_1_samples = mcmc.trace('lambda_1')[:]
lambda_2_samples = mcmc.trace('lambda_2')[:]
tau_samples = mcmc.trace('tau')[:]



figsize(13.5, 6.6)
# histogram of the samples:


ax = plt.subplot(311)
ax.set_autoscaley_on(True)

plt.hist(lambda_1_samples, histtype='stepfilled', bins=30, alpha=0.85,
         label="posterior of $\lambda_1$", color="#A60628", normed=True)
plt.legend(loc="upper left")
plt.title(r"""Posterior distributions of the variables
    $\lambda_1,\;\lambda_2,\;\tau$""")
plt.xlim([25, 50])
plt.xlabel("$\lambda_1$ value")
ax = plt.subplot(312)
ax.set_autoscaley_on(True)
plt.hist(lambda_2_samples, histtype='stepfilled', bins=30, alpha=0.85,
         label="posterior of $\lambda_2$", color="#7A68A6", normed=True)
plt.legend(loc="upper left")
plt.xlim([20, 37])
plt.xlabel("$\lambda_2$ value")

plt.subplot(313)
w = 1.0 / tau_samples.shape[0] * np.ones_like(tau_samples)
plt.hist(tau_samples, bins=n_count_data, alpha=1,
         label=r"posterior of $\tau$",
         color="#467821", weights=w, rwidth=4.)
#start, end = ax.get_xlim()
#plt.xaxis.set_ticks(np.arange(0, 300, 50))
plt.xticks(np.arange(n_count_data-50),rotation=45,fontsize=.25)

plt.legend(loc="upper left")
plt.ylim([0, .75])
plt.xlim([80, 101])
plt.xlabel(r"$\tau$ (in days)")
plt.ylabel("probability");
pp.savefig()


from mpld3 import plugins
mpld3.enable_notebook()
# Define some CSS to control our custom labels
css = """
table
{
  border-collapse: collapse;
}
th
{
  color: #ffffff;
  background-color: #000000;
}
td
{
  background-color: #cccccc;
}
table, th, td
{
  font-family:Arial, Helvetica, sans-serif;
  border: 1px solid black;
  text-align: right;
}
"""
figsize(19, 5)
# tau_samples, lambda_1_samples, lambda_2_samples contain
# N samples from the corresponding posterior distribution
N = tau_samples.shape[0]
expected_usrs_per_day = np.zeros(n_count_data)
for day in range(0, n_count_data):
    # ix is a bool index of all tau samples corresponding to
    # the switchpoint occurring prior to value of 'day'
    ix = day < tau_samples
    # Each posterior sample corresponds to a value for tau.
    # for each day, that value of tau indicates whether we're "before"
    # (in the lambda1 "regime") or
    #  "after" (in the lambda2 "regime") the switchpoint.
    # by taking the posterior sample of lambda1/2 accordingly, we can average
    # over all samples to get an expected value for lambda on that day.
    # As explained, the "message count" random variable is Poisson distributed,
    # and therefore lambda (the poisson parameter) is the expected value of
    # "message count".
    expected_usrs_per_day[day] = (lambda_1_samples[ix].sum()
                                   + lambda_2_samples[~ix].sum()) / N


plt.plot(range(n_count_data), expected_usrs_per_day, lw=4, color="#E24A33",
         label="expected number of users")
plt.xlim(0, n_count_data)
plt.xlabel("Day")
plt.ylabel("Expected # users")
plt.title("Expected number of users")
plt.ylim(0, 70)
plt.bar(np.arange(len(count_data)), count_data, color="#348ABD", alpha=0.65,label="observed users per day")
plt.legend(loc="upper left")
pp.savefig()


expected_incr=(lambda_1_samples/lambda_2_samples).mean()
period1=lambda_1_samples.mean()
period2=lambda_2_samples.mean()
print period1,period2,expected_incr

count_data.to_csv('mcmc.csv')


print tau_samples.shape[0]


pp.close()
