
f = open("english.stop")

dict = {}
for line in f:
    # print(line.rstrip("\n"))
    temp = line.rstrip("\n")
    dict[temp] = 1
    # dict[temp.upper()] = 1
    # dict[temp.title()] = 1

f.close()

with open("enwiki-latest-all-titles-in-ns0") as f:
    for line in f:
        line = line.rstrip("\n")
        line = line.split("_")
        line = [x for x in line if not(x.lower() in dict)]
        line = "_".join(line)
        if (line != ""):
            print(line)
