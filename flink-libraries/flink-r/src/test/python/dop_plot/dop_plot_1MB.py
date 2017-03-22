import numpy as np
import pandas as pd
import seaborn as sns

p1 = np.array([149412, 114479, 124967, 132158, 102835, 126897, 118127, 117560, 118395, 127244])/1000.0
p2 = np.array([112400, 61724, 96976, 52582, 92229, 57668, 88037, 58100, 99862, 45286])/1000.0
p3 = np.array([91447, 55274, 94329, 63250, 69626, 73165, 49047, 68309, 61305, 80117])/1000.0
p4 = np.array([36367, 35221, 55426, 39817, 37563, 56073, 35892, 40784, 34206, 40884])/1000.0
data = [p1, p2, p3, p4]

rdf = pd.DataFrame()
xax = 'DOP'
yax = 'Time (s)'
for i, row in enumerate(data):
    df = pd.DataFrame({yax: pd.Series(row)})
    df[xax] = i+1
    rdf = pd.concat([rdf, df], ignore_index=True)

ax = sns.regplot(x=xax, y=yax, data=rdf, x_estimator=np.mean)
sns.plt.title('1 MB Lorem Ipsum Text Filter')
fig = ax.get_figure()
fig.savefig("dop_plot_1MB.png")
