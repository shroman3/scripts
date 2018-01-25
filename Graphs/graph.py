'''
Created on Jan 25, 2018

@author: shroman
'''

import sys
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.rcsetup import validate_fontsize
from matplotlib.patches import Patch
import matplotlib as mpl

mpl.rc('font',family='Times')

shatch1=[ "", "", "", "", '/', '-']
shatch2=[ "", "", "", '/', '-']
scolor1=['white','lightgray','gray','black','lightpink','lightblue']
scolor2=['lightgray','gray','black','lightpink','lightblue']
sfontsize = 11
hatch_times = 3

def graph_random(filename):
	data = pd.read_csv(filename)
# 	plt.figure()
# 	plt.figure(num=None,  dpi=80, facecolor='w', edgecolor='k')
	ax = data.plot(kind='bar', x='k',legend=True,color=scolor2, edgecolor='black', linewidth=1, width=0.9, figsize=(5, 3))#colormap="Greens"
	
	shandles=[]
	for shatch, scolor, sname in zip(shatch2,scolor2, list(data)[1:]):
		shandles.append(Patch(facecolor=scolor, hatch=shatch*hatch_times,edgecolor='black', label=sname))
	plt.legend(handles=shandles,fontsize=sfontsize,edgecolor='black', ncol=3,framealpha=None, shadow=None, fancybox=False, loc=1, borderaxespad=0., handlelength=0.8)
	
	plt.ylabel("Random access latency (ms)", fontsize=14)
	plt.xlabel("(k : share size)", fontsize=sfontsize)
# 	for i,thisbar in enumerate(ax.patches):
# 		hatch = shatch[i/(len(data.index.values))]
# 		thisbar.set_hatch(hatch*2)
# 		thisbar.set_color(scolor[i%5])
	plt.xticks(rotation=0, fontsize = sfontsize)
	ymin,ymax = ax.get_ylim()
	ax.set_ylim(ymin,ymax+0.25*ymax)
	ax.grid(color='grey', linestyle='-', axis='y')
	ax.set_axisbelow(b=True)
# 	plt.title(title,fontsize = 24)
	plt.yticks(fontsize = sfontsize)
	
	for i,thisbar in enumerate(ax.patches):
		# Set a different hatch for each bar (add +1 for when using unsafe)
		hatch = shatch2[int(i/3)]
		thisbar.set_hatch(hatch*hatch_times)

	plt.tight_layout()	
	fname = filename.split(sep=".")[0]
	plt.savefig(fname + ".png", dpi=200)
	plt.show();
	print("s")

def graph_decode(filename):
	data = pd.read_csv(filename)
# 	plt.figure()
# 	plt.figure(num=None,  dpi=80, facecolor='w', edgecolor='k')
	ax = data.plot(kind='bar', x='k',legend=True,color=scolor2, edgecolor='black', linewidth=1, width=0.9, figsize=(5, 3))#colormap="Greens"
	
	shandles=[]
	for shatch, scolor, sname in zip(shatch2,scolor2, list(data)[1:]):
		shandles.append(Patch(facecolor=scolor, hatch=shatch*hatch_times,edgecolor='black', label=sname))
	plt.legend(handles=shandles,fontsize=sfontsize,edgecolor='black', ncol=3,framealpha=None, shadow=None, fancybox=False, loc=1, borderaxespad=0., handlelength=0.8)
	
	plt.ylabel("Decode throughput (MB/s)", fontsize=14)
	plt.xlabel("k", fontsize=sfontsize)
# 	for i,thisbar in enumerate(ax.patches):
# 		hatch = shatch[i/(len(data.index.values))]
# 		thisbar.set_hatch(hatch*2)
# 		thisbar.set_color(scolor[i%5])
	plt.xticks(rotation=0, fontsize = sfontsize)
	ymin,ymax = ax.get_ylim()
	ax.set_ylim(ymin,ymax+0.25*ymax)
	ax.grid(color='grey', linestyle='-', axis='y')
	ax.set_axisbelow(b=True)
# 	plt.title(title,fontsize = 24)
	plt.yticks(fontsize = sfontsize)
	
	for i,thisbar in enumerate(ax.patches):
		# Set a different hatch for each bar (add +1 for when using unsafe)
		hatch = shatch2[int(i/3)]
		thisbar.set_hatch(hatch*hatch_times)

	plt.tight_layout()	
	fname = filename.split(sep=".")[0]
	plt.savefig(fname + ".png", dpi=200)
	plt.show();

def graph_encode(filename):
	data = pd.read_csv(filename)
# 	plt.figure()
# 	plt.figure(num=None,  dpi=80, facecolor='w', edgecolor='k')
	ax = data.plot(kind='bar', x='k',legend=True,color=scolor1, edgecolor='black', linewidth=1, width=0.9, figsize=(5, 3))#colormap="Greens"
	
	shandles=[]
	for shatch, scolor, sname in zip(shatch1,scolor1, list(data)[1:]):
		shandles.append(Patch(facecolor=scolor, hatch=shatch*hatch_times,edgecolor='black', label=sname))
	plt.legend(handles=shandles,fontsize=sfontsize,edgecolor='black', ncol=3,framealpha=None, shadow=None, fancybox=False, loc=1, borderaxespad=0., handlelength=0.8)
	
	plt.ylabel("Decode throughput (MB/s)", fontsize=14)
	plt.xlabel("k", fontsize=sfontsize)
# 	for i,thisbar in enumerate(ax.patches):
# 		hatch = shatch[i/(len(data.index.values))]
# 		thisbar.set_hatch(hatch*2)
# 		thisbar.set_color(scolor[i%5])
	plt.xticks(rotation=0, fontsize = sfontsize)
	ymin,ymax = ax.get_ylim()
	ax.set_ylim(ymin,ymax+0.25*ymax)
	ax.grid(color='grey', linestyle='-', axis='y')
	ax.set_axisbelow(b=True)
# 	plt.title(title,fontsize = 24)
	plt.yticks(fontsize = sfontsize)
	
	for i,thisbar in enumerate(ax.patches):
		# Set a different hatch for each bar (add +1 for when using unsafe)
		hatch = shatch1[int(i/3)]
		thisbar.set_hatch(hatch*hatch_times)

	plt.tight_layout()	
	fname = filename.split(sep=".")[0]
	plt.savefig(fname + ".png", dpi=200)
	plt.show();


if __name__ == "__main__":
# 	if (len(sys.argv) != 2):
# 		filename = "decode_basic_c.csv"
# # 		filename = "decode_rand_c.csv"
# 	else:
# 		filename =sys.argv[1]

# 	graph_random("decode_rand_c.csv")
# 	graph_decode("decode_basic_c.csv")
	graph_encode("encode_basic_c.csv")