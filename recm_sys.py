import sys
import operator
from pyspark import SparkContext
from itertools import combinations


sc = SparkContext(appName ="recm")

inputfile = sys.argv[1]
outputfile = sys.argv[2]

rdd = sc.textFile(inputfile)
rdd2= rdd

rdd = rdd.map(lambda x:x.split(',')).flatMap(lambda x:[(x[0],y) for y in x[1:]])

def generate_hash(x):
    hashbuckets =[]
    for i in range(20):
        h = ((3*int(x[1]))+(13*i))%100
        hashbuckets.append(((x[0],i),h))
    return hashbuckets

def create_bands(h):
    return [((h[0],0),h[1][0:4]),((h[0],1),h[1][4:8]),((h[0],2),h[1][8:12]),((h[0],3),h[1][12:16]),((h[0],4),h[1][16:20])]


def modify(input):
    key = input[0]
    value = input[1]
    final = [key[0],value]
    return (key[1],final)


def candidate_pair(input):
    keys = input[0]
    values = input[1]
    dict = {}
    for x in values:
        dict[str(x[1])] = []
    for x in values:
        key = str(x[0])
        value = str(x[1])
        if value in dict:
            dict[value].append(key)
    similar_users = []
    for k,v in dict.items():
        if len(v) > 1:
            similar_users.append(v)
    lsh_pair = []
    for x in similar_users:
        y = (list(combinations(x,2)))
        lsh_pair += y
    return lsh_pair


def user_to_user(x):
    import operator
    dict_pair = {}
    lis = {}
    for i in x:
        intx = float(len(set(setdict[i[0]]).intersection(set(setdict[i[1]]))))
        uni = len(set(setdict[i[0]]).union(set(setdict[i[1]])))     
        jx = intx / uni
        if i[0] not in dict_pair.keys():
            dict_pair[i[0]] = {i[1]:jx}
        else:
            dict_pair[i[0]][i[1]] = jx
        if i[1] not in dict_pair.keys():
            dict_pair[i[1]] = {i[0]:jx}
        else:
            dict_pair[i[1]][i[0]] = jx  
    for key,v in dict_pair.items():
        l = sorted(v.items(), key=operator.itemgetter(1), reverse = True)[:5]
        lis[key] = [i[0] for i in l]
    return lis


def movie_recommendation(x):
    #import operator
    movie_recommendation = []
    for k,v in x.items():
        c = {}
        for i in v:
            w = setdict[i]
            for p in w:
                if p not in c:
                    c[p] = 1
                else:
                    c[p] +=1
        f = sorted(c.items(), key=operator.itemgetter(0))
        f = sorted(f, key=operator.itemgetter(1), reverse = True)[:3]
        key = sorted([i[0] for i in f])
        movie_recommendation.append([k,key])
    return movie_recommendation


#Generating the 20 hash functions
rdd = rdd.flatMap(generate_hash).groupByKey().map(lambda x:(x[0],min(list(x[1]))))
rdd = rdd.map(lambda x:(x[0][0],(x[0][1],x[1]))).groupByKey().map(lambda x:(x[0],sorted(list(x[1]))))

#Sorting the hash functions into bands. four(4) hash functions per band
rdd = rdd.flatMap(create_bands)
#Transforming the rdd to have band number as key and then group by band
rdd = rdd.map(modify).groupByKey().map(lambda x:(x[0],list(x[1])))

#Finding candidate pair based on similarity of signature within each band
rdd = rdd.flatMap(candidate_pair).distinct()

#Transforming original rdd
setfile = rdd2.map(lambda x:x.split(',')).flatMap(lambda x:[(x[0],int(y)) for y in x[1:]]).groupByKey().map(lambda x:(x[0],list(x[1])))
setlist = sc.broadcast(setfile.collect())

setdict = {}
def convertToDict():
    dict = {}
    k = setlist.value
    for x in k:
        dict[x[0]] = list(x[1])
    global setdict
    setdict = dict


convertToDict()

f = rdd.collect()

b = user_to_user(f)
output = movie_recommendation(b)
to_file = []

for x in output:
    to_file.append([int(x[0][1:]),x[1]])
to_file = sorted(to_file)
f = open(outputfile,"a+")
for x in to_file:
    f.write(str('U')+str(x[0])+", "+str(x[1])[1:-1]+"\n")
