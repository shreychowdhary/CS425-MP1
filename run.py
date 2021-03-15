import subprocess
import time
import argparse
import signal
import filecmp

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('N',type=int)
parser.add_argument("rate",type=float)
args = parser.parse_args()
pids = []
for i in range(1,args.N+1):
    print(i)
    time.sleep(.2)
    pid = subprocess.Popen("exec python3 -u gentx.py {} | ./node node{} 123{} ./config/{}config{} > output{}.txt".format(args.rate,i,i,args.N,i,i),shell=True)
    pids.append(pid)


def compare_files(indicies):
    for i in indicies:
        print (i)
        file1 = "output{}.txt".format(i)
        file2 = "output{}.txt".format(i+1)
        same = filecmp.cmp(file1,file2,shallow=False)
        if not same:
            return False
    return True

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    for pid in pids:
        pid.kill()
    time.sleep(.1)
    if compare_files(range(1,args.N+1)):
        print("SAME")
    else:
        print("WRONG")
    exit()

signal.signal(signal.SIGINT, signal_handler)

time.sleep(100)
for pid in pids[:len(pids)//2]:
    pid.kill()

time.sleep(100)
for pid in pids:
    pid.kill()
print(len(pids))
print("SAME"+str(compare_files(range(1,args.N//2))))
print("SAME"+str(compare_files(range(args.N//2,args.N))))
exit()