import numpy as np

with open("/home/jens/Workspace/flink/flink-libraries/flink-r/logs/log.txt") as f:
    content = f.readlines()
    coll = {}
    for line in content:
        if line.startswith("[1] \"[419] +"):
            splits = line.split(',')
            name = splits[1]
            if name not in coll:
                coll[name] = []
            val = float(splits[2])
            coll[name] += [val]

    for key in coll:
        collect = np.array(coll[key])
        print(key + ': \t' + str(collect.sum()))

