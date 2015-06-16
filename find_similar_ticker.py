#! -*- coding: cp932 -*-

import sys, os, csv
import httplib, urlparse
import math
from subprocess import Popen, call
from datetime import datetime, timedelta

PATH_TO_DAT = "dat"

class Storage(dict):
    def __getattr__(self, key): 
        if self.has_key(key): 
            return self[key]
        raise AttributeError, repr(key)

    def __setattr__(self, key, value): 
        self[key] = value

    def __repr__(self):     
        return '<Storage ' + dict.__repr__(self) + '>'

class Downloader:
    def __init__(self, path):
        self.path_to_dat = path

    def _execute(self, cmd):
        try:
            ret = call(cmd, shell=True)
            if ret < 0:
                print >>sys.stderr, "The child process was exited by signal.", -ret
                sys.exit(1)
        except OSError, e:
            print >>sys.stderr, "failed to execute:", e
            sys.exit(1)

    def _download(self, url, lzhfile, csvfile):
        print "downloading %s" % lzhfile
        self._execute("curl -O --silent %s" % url)
        self._execute("lha eq %s" % lzhfile)
        self._execute("mv %s dat/" % csvfile)
        self._execute("rm %s" % lzhfile)

    def _is_exist_stock_data(self, url):
        (scheme, location, objpath, param, query, fid) = urlparse.urlparse(url, 'http')
        header = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'}
        con = httplib.HTTPConnection(location)
        con.request('GET', objpath, "", header)
        response = con.getresponse()
        if response.status == 302 or response.status == 404:
            return False
        else:
            return True

    def _date_2_string(self, date):
        csvfile = "t%s.csv" % date.strftime("%y%m%d")
        lzhfile = "T%s.lzh" % date.strftime("%y%m%d")
        url = "http://souba-data.com/data_day/%sd/%s_%sd/T%s.lzh" % (
            date.strftime("%Y"),
            date.strftime("%y"),
            date.strftime("%m"),
            date.strftime("%y%m%d"))
        return csvfile, lzhfile, url

    def download_data_of_25_days(self):
        counter = 0
        target_date = datetime.today()

        if not self.path_to_dat in os.listdir("."):
            os.mkdir(self.path_to_dat)

        while True:
            target_date =  target_date - timedelta(1)
            while target_date.weekday() == 5 or target_date.weekday() == 6:
                target_date = target_date - timedelta(1)

            csvfile, lzhfile, url  = self._date_2_string(target_date)
            if csvfile in os.listdir(self.path_to_dat):
                break

            if not self._is_exist_stock_data(url):
                continue
            
            self._download(url, lzhfile, csvfile)

            # check whether the downloaded file exists or not.
            if csvfile in os.listdir(self.path_to_dat):
                counter += 1

            if counter >= 60:
                break

class Stocks(dict):
    def __init__(self, path, days=25, before=0):
        self.path_to_dat = path
        self.days = days
        self.before = before
        self._make_dict()

    def _make_dict(self):
        if not self.path_to_dat in os.listdir("."):
            print >>sys.stderr, "Stock data not found. Please add [-d] option to download data."
            sys.exit(1)

        csvfiles = os.listdir(self.path_to_dat)
        csvfiles.sort()
        s = -1 * self.days - self.before
        e = len(csvfiles) - self.before
        csvfiles = csvfiles[s:e]

        for csvfile in csvfiles:
            stream = file(os.path.join(self.path_to_dat, csvfile), "rb")
            for line in csv.reader(stream):
                description = unicode(line[3][5:], 'cp932').encode('utf-8')

                if self.has_key(int(line[1])):
                    self[int(line[1])].opening_prices.append(int(line[4]))
                    self[int(line[1])].high_prices.append(int(line[5]))
                    self[int(line[1])].low_prices.append(int(line[6]))
                    self[int(line[1])].closing_prices.append(int(line[7]))
                    self[int(line[1])].volumes.append(float(line[8]))
                    self[int(line[1])].filenames.append(csvfile)
                else:
                    self[int(line[1])] = Storage({
                        'description': unicode(line[3][5:],'cp932').encode('utf-8'),
                        'opening_prices': [int(line[4])],
                        'high_prices': [int(line[5])],
                        'low_prices': [int(line[6])],
                        'closing_prices': [int(line[7])],
                        'volumes': [float(line[8])],
                        'filenames': [csvfile],
                        })

        for k, v in self.items():
            if len(v.closing_prices) != self.days:
                del self[k]

def correlation(xs, ys):
    avgx = sum(xs) / float(len(ys))
    avgy = sum(ys) / float(len(ys))
    l = sum([(x - avgx) * (y - avgy) for x, y in zip(xs, ys)])
    m = sum([(x - avgx) ** 2 for x in xs])
    n = sum([(y - avgy) ** 2 for y in ys])
    if m == 0 or n ==0:
        return 0
    else:
        return l / (math.sqrt(m) * math.sqrt(n))

def main(code):
    stock = Stocks(PATH_TO_DAT)
    if not stock.has_key(code):
        print >>sys.stderr, "%i is not found." % code
        error_message()

    target = stock[code].closing_prices
    results = []
    for k, v in stock.items():
        if k == code:
            continue
        results.append((k, correlation(target, v.closing_prices)))
    results.sort(lambda x, y: cmp(y[1] ,x[1]))

    for x, y in results[:10]:
        print x, stock[x].description, y

def error_message():
    print >>sys.stderr, "Usage: find_similar_stock.py [code]"
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        error_message()

    if sys.argv[1] == "-d":
        d = Downloader(PATH_TO_DAT)
        d.download_data_of_25_days()
        print "done."

    elif not sys.argv[1].isdigit():
        error_message()

    else:
        main(int(sys.argv[1]))
