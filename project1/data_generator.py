from random import shuffle


class DataTuple:
    def __init__(self, key):
        self.key     = key
        self.payload = "  " + key * 8 + "0000000"


def runTest(outputFile, totalTuple):
    dataList = []
    for i in range(1, totalTuple+1):
        dataList.append(DataTuple(str(i).zfill(10)))
    shuffle(dataList)
    with open(outputFile, 'w') as writeFile:
        for t in dataList:
            writeFile.write(t.key + t.payload + '\n')
    print("writing to " + outputFile + " is done\n")


if __name__ == "__main__":
    runTest("input_1GB.test", (int)((10**9)/100))
    runTest("input_2GB.test", (int)(2*(10**9)/100))
