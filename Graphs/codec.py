import matplotlib.pyplot as plt
import numpy as np
import todb
import os.path
import sqlite3
import pandas as pd

# some = np.genfromtxt("codec.csv", delimiter=',', usecols=(0,1,2,3,4,5,6,7,8), converters={})
FNAME = "codec.csv"
TABLE_FILE = "codec.db"
TABLE = "codec"
metric = "(2048000.0/time)"
metric_name = "throughput"
metric1 =  "(1024000.0/time)"
metric1_name = "throughput"
metric2 = "(4000.0/time)"
metric2_name = "throughput"
# metric3 = "(rq_throughput/1000000.0)"
# metric3_name = "rqs"
  

datastructures= ["BB" ,"PSS" ,"AES","CHA","AONT_AES","NONE", "SSS"]
experiment = ["write", "read", "fread", "rcr", "deg1", "deg2", "sf1", "sf2" ,"enc"]
# logtype = ["throughput", "encode", ]

# logdatastructures = ["abtree","bst","citrus","skiplistlock"]
# rludatastructures = ["citrus","lazylist"]

# def is_valid_key_range(ds,key_range):
#	 if key_range == "10000000" and ds!= "abtree": return False
#	 if key_range == "1000000" and ds!= "abtree": return False
#	 if key_range == "100000" and ds not in logdatastructures: return False
#	 if key_range == "10000" and ds in logdatastructures: return False
#	 if key_range == "1000" and ds in logdatastructures: return False
#	 if clean and key_range == "100000" and ds== "abtree": return False
#	 return True

codecToAlgs = { "BB" :		 ["AES", "htm_rwlock", "lockfree"],
			 "PSS" :			["rwlock", "htm_rwlock", "lockfree"],
			 "SSS" :		 ["rwlock", "htm_rwlock", "lockfree", "timnat"],
			}

codecToName = { "BB"  :	"S-RAID",
			 "PSS" :	"Packed",
			 "AES" :	"AES-256",
			 "CHA" :	"ChaCha",
			 "AONT_AES" : "AONT_RS",
			 "NONE" :   "RS",
			 "SSS" :   "Shamir"
			}
			
codecToHatch = { "BB" :		 [ "", '.', '/', 'O', '-', '*' ],
				"PSS" :		 [ "", '.', '/', 'O', '-', '*' ],
				"AES" :		 [ "", '.', '/', '-', 'O', '-', '*' ],
				"CHA" :		 [ "", '.', '/', 'x', 'O', '-', '*' ],
				"AONT_AES" :	[ "", '.', '/', 'x', 'O', '-', '*' ],
				"NONE" :		[ "", '.', '/', '-', 'O', '-', '*' ],
				"SSS" :		 [ "", '.', '/', '-', 'o', '-', '*' ]
			}

codecToColor = {"BB" :	  ['C0','C1','C2','C5'],
			 "PSS" :		['C0','C1','C2','C5'],
			 "AES" :		['C0','C1','C2','C3','C5'],
			 "CHA" :		['C0','C1','C2','C4','C5'],
			 "AONT_AES" :   ['C0','C1','C2','C4','C5'],
			 "NONE" :	   ['C0','C1','C2','C3','C5'],
			 "NONE" :	   ['C0','C1','C2','C3','C5']
			}
		   
codecToMarker = {"BB" :		["o", "^", "s", "x"],
			 "PSS" :		["o", "^", "s", "x"],
			 "AES" :		["o", "^", "s", "P", "x"],
			 "CHA" :		["o", "^", "s", "X", "x"],
			 "AONT_AES" :   ["o", "^", "s", "X", "x"],
			 "NONE" :	   ["o", "^", "s", "P", "x"],
			 "SSS" :		["o", "^", "s", "P", "x"]

			}

markerToSize = {	"o":22, #lock
					"^":24, #HTM
					"s":22, #lockfree
					"+":26, 
					"D":18, 
					"x":24, #unsafe
					"X":22, #RLU
					"P":22  #timnat
				}

codecToStyle = {"BB" :		  ['-', '-', '-', ':'],
			 "PSS" :			['-', '-', '-', ':'],
			 "AES" :			['-', '-', '-', '--', ':'],
			 "CHA" :			['-', '-', '-', '--', ':'],
			 "AONT_AES" :	   ['-', '-', '-', '--', ':'],
			 "NONE" :		   ['-', '-', '-', '--', ':'],
			 "SSS" :			['-', '-', '-', '--', ':']
			}
hatches_mult = 2
clean=1

def plot_bar(conn, exp, logtype, r, metric, metric_name):
	query = ("SELECT codec AS c, random AS rand, k, z, AVG("+metric+") AS y" 
			 " FROM codec" 
			 " WHERE exp = '"+exp+"'" +
			 		" AND k=2" +
					" AND log_type='"+logtype +"'" +
					" AND r="+r+
					" AND (z=0 OR z=2)"
			 " GROUP BY c, rand, k ORDER BY k, c, rand")
	state = ["exp"+exp,"logtype"+logtype]
	title = ""
	if not clean:
		title = "\n" + " ".join(state)
	
	df = pd.read_sql(query, conn)
	df = df.set_index(['k', 'rand', 'c'])
	plt.figure()
	ax = df.plot(kind='bar', y='y',legend=False, color='white', edgecolor='black', linewidth=2, title=title)
# 	plt.xticks(rotation=45)
# 	plt.xlabel("k")
	plt.title(title,fontsize = 24)
	plt.yticks(fontsize = 22)
	filename = "bar-"+metric_name+"-"+exp+"-"
	filename += "-".join(state)
	ymin,ymax = ax.get_ylim()
#  	if codec == "skiplistlock" : 
#  		ax.set_aspect(0.04)
# 		 plt.yticks(np.arange(ymin, ymax, 10))
#  	if codec == "lflist" : 
# 		 ax.set_aspect(1.4)
# 		 plt.yticks(np.arange(ymin, ymax, 0.25))
# 	 Loop over the bars
	for i,thisbar in enumerate(ax.patches):
		# Set a different hatch for each bar (add +1 for when using unsafe)
		hatch = codecToHatch['AES'][i%7]
		thisbar.set_hatch(hatch*hatches_mult)
	ax.set_ylim(ymin,ymax+0.1*ymax)
# 	if clean:
# 		plt.xticks([], [])
	ax.grid(color='grey', linestyle='-')
	if not clean: 
		ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handleheight=2)
		plt.savefig(filename+".png", bbox_extra_artists=(ax.get_legend(),), bbox_inches='tight')
	else:
		plt.savefig(filename+".png",bbox_inches='tight')
	plt.close('all')






if __name__ == "__main__":
	if not os.path.isfile(TABLE_FILE):
		print("doesnt exist") 
		todb.todb(FNAME)
	
	conn = sqlite3.connect(TABLE_FILE)
	plot_bar(conn, "enc", "encode", "2", metric2, metric2_name)
