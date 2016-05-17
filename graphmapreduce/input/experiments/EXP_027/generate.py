
import random
import sys
import os


def generate_tuple(arity):
    t = []
    for i in range(arity):
        t.append(random.randrange(0,n))
        
    return t
        
def generate_relation(dir, name, arity):
    os.makedirs(dir+"/"+name)
    f = open(dir + "/" + name +"/" + name + ".txt","w")
    for i in range(n):
        t = generate_tuple(arity)
        t = map(str, t)
        line = reduce(lambda x,y: x + "," + y, t) 
        f.write(line + "\n")
        
    f.close()
    

        
n = int(sys.argv[1])
dir = sys.argv[2]

relations = {
    "R": 3,
    "S": 1,
    "T": 1,
    "U": 1,
    "V": 1,
}

for relation in relations.keys():
    generate_relation(dir, relation,relations[relation])
    