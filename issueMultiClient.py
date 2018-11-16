from multiprocessing import Pool
import os
from time import gmtime, strftime

dir0="outputs"+ strftime("%Y-%m-%d-%H:%M:%S", gmtime())

def f(x):
    return x*x

def call_client_update(x):
    command = ""
    # if True:
    command = "./java/target/surfstore/bin/runClient ./configs/configDistributed.txt upload $(pwd)/data/1.txt 1>> ./{}/output{}.txt 2>> ./{}/error{}.txt".format(dir0,x,dir0,x)
    # else:
    #     command = "./java/target/surfstore/bin/runClient ./configs/configDistributed.txt delete 1.txt 1> ./{}/output{}.txt 2> ./{}/error{}.txt".format(dir0,x,dir0,x)
    for i in range(10):
        if(i==3 and x==2):
            os.system("./java/target/surfstore/bin/runClient ./configs/configDistributed.txt crash 2>> ./{}/crash.txt".format(dir0))
        if(i==8 and x==5):
            os.system("./java/target/surfstore/bin/runClient ./configs/configDistributed.txt restore 2>> ./{}/crash.txt".format(dir0))
        os.system(command)
        os.system("./java/target/surfstore/bin/runClient ./configs/configDistributed.txt getversion 1.txt 1>> ./{}/output{}.txt".format(dir0,x))


if __name__ == '__main__':
    # with Pool(5) as p:
    #     print(p.map(f, [1, 2, 3]))

    # a = os.system("echo $path > output1.txt")
    # print(a)

    if not os.path.exists(dir0):
        os.makedirs(dir0)

    # command = "./java/target/surfstore/bin/runClient ./configs/configDistributed.txt upload $(pwd)/data/1.txt 1> ./{}/output{}.txt 2> ./{}/error{}.txt".format(dir0,111,dir0,111)
    # os.system(command)

    with Pool(10) as p:
        p.map(call_client_update,list(range(10)))