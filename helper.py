import config
from datetime import datetime

class Sparkfn:
    @staticmethod
    def splitData(row):
        wordlist = row[0].split(",")
        result = list()
        for word in wordlist:
            result.append(word.strip())
        return result

    @staticmethod
    def addSessionId(dataSet):
        session=0
        t=[]
        for x in dataSet:
             if x[3] == config.sessionIndecator:
                    session+=1
             x.append(session)
             t.append(x)
        return t

    @staticmethod
    def getPattern(url):
         for pattern in config.patterns:
              if pattern in url:
                   return(pattern)
         return None

    @staticmethod
    def is_numeric(s):
            try:
                float(s)
                return True
            except ValueError:
                return False

    @staticmethod
    def to_timestamp(ts):
         return int(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').timestamp())







