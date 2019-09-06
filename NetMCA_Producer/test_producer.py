from confluent_kafka import Producer
import os
from time import sleep
import datetime
Path = "/home/ccallaghan@tlabsad.tlabs.ac.za/netmca/ftp/newfiles/"
RegionFilename = "Regions.config"
BootstrapServers = 'localhost'
GroupID = "NetMCA"



class Element:
    def __init__(self, DescriptionString):
        DescriptionList = DescriptionString.split()
        self.ProcessVariableName = DescriptionList[0]
        self.ElementName = DescriptionList[1]
        Count=2
        self.Regions=[]
        while (Count < len(DescriptionList)):
            self.Regions.append([int(DescriptionList[Count]), int(DescriptionList[Count+1])])
            Count += 2
        self.MyProducer = Producer({'bootstrap.servers':BootstrapServers, 'group.id': GroupID})

    def Process(self, Values):
        Total = 0
        Sign = 1
        for Region in self.Regions:
            if Region[0]<0:
                Sign=-1
            else:
                Sign=1
            #print (abs(Region[0]), " ", abs(Region[1])+1)
            for Count in range(abs(Region[0]), min(abs(Region[1])+1, len(Values)-1)):
                Total += Sign*Values[Count]
        print(self.ElementName, " ", Total)
        self.MyProducer.produce(topic=self.ElementName, value=''+str(Total))



OldFiles = os.listdir(Path)

RegionsFile = open(RegionFilename)

AllRegions = []

for Line in RegionsFile:
    if (len(Line)>1) and (Line[0]!='#'):
        AllRegions.append(Element(Line))

print(AllRegions)

for Region in AllRegions:
    print (Region.ProcessVariableName, Region.Regions)


#print (OldFiles)

Cooker = Producer({'bootstrap.servers':'localhost', 'group.id': 'HEX', 'default.topic.config': {'auto.offset.reset': 'latest'}, 'auto.commit.interval.ms': 500})

#Cooker.subscribe(["test", "archive"])

Count = 1
DateLine = ""
FileTime = datetime.datetime(2000, 1, 1, 0, 0, 0) #Generic 'null' date
while (Count < 20):
    sleep(1)
    NewFiles =  os.listdir(Path)
    ActualNewFiles = list(set(NewFiles)-set(OldFiles))
    if len(ActualNewFiles)>0:
        for Filename in ActualNewFiles:
            Values = []
            StrValues = []
            FullPath = os.path.join(Path, Filename)
            File = open(FullPath, "r")
            NewLine = File.readline()
            while NewLine:
                if (NewLine[0]=='$'):
                    if (NewLine.startswith("$DATE_MEA:")):
                        DataLine = File.readline().lstrip().rstrip()
                        FileTime = datetime.datetime.strptime(DataLine, "%m-%d-%Y %H:%M:%S")
                    elif (NewLine.startswith("$MEAS_TIM:")):
                        DataLine = File.readline().lstrip().rstrip()
                        Times = DataLine.split()
                        ActiveTime = int(Times[0])
                        ActualTime = int(Times[1])
                    elif (NewLine.startswith("$DATA:")):
                        DataLine = File.readline().lstrip().rstrip()
                        MinMax = DataLine.split()
                        Min = int(MinMax[0])
                        Max = int(MinMax[1])
                        while len(Values) <= (Max-Min):
                            DataLine = File.readline().lstrip().rstrip()
                            for Num in DataLine.split():
                                Values.append(int(Num))
                                StrValues.append(Num)
                NewLine = File.readline()
            print("Raw Data: " + " ".join(StrValues), type("Raw Data: "))
            for Region in AllRegions:
                Region.Process(Values)
            Cooker.produce(topic="archive", value=("Raw Data: " + " ".join(StrValues)))
            print(Values)
        OldFiles=NewFiles
    Count = Count+1
    Cooker.flush()

    if True:
        print ("Tick\nTock")
    else:
        for Index in range(len(Scroll)):
            print (Scroll[Index].value())
