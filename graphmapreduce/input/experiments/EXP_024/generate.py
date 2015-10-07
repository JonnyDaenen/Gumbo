
import random
import sys


def generate_tuple(arity):
    t = []
    for i in range(arity):
        t.append(random.randrange(0,n))
        
    return t
        
def generate_relation(name, arity):
    f = open(name +"/" + name + ".txt","w")
    for i in range(n):
        t = generate_tuple(arity)
        t = map(str, t)
        line = reduce(lambda x,y: x + "," + y, t) 
        f.write(line + "\n")
        
    f.close()
    

        
n = int(sys.argv[1])

relations = {
    "R": 2,
    "S": 1,
    "T": 1,
    "U": 1,
    "V": 1,
    "W": 1,
    "G": 2,
    "A": 1,
}

for relation in relations.keys():
    generate_relation(relation,relations[relation])
    