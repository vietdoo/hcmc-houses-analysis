import requests
import sys
import json
import pandas as pd
import time
import numpy as np
import random

class Crawler:
    def __init__(self, CITY_CODE = 13000, AREA_CODE = 13096):
        self.CITY_CODE = CITY_CODE
        self.AREA_CODE = AREA_CODE
        self.DEFAULT = DEFAULT = 'https://gateway.chotot.com/v1/public/ad-listing?'
        self.ERROR_LIMIT = 6
        self.user_agents = [ 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36', 
            'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148', 
            'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36' 
        ] 

    def run(self):
        error = 0
        data = []
        previous = time.time()
    
        while (True):
            page = 0
            o = -20
            sys.stdout.write('Scanning area: %d\n' % (self.AREA_CODE))
            sys.stdout.flush()
            
            while (True):
                try:
                  
                    page = page + 1
                    o = o + 20
                    url = self.DEFAULT + 'region_v2' + str(self.CITY_CODE) + '&area_v2=' + str(self.AREA_CODE) + '&cg=1000&o=' + str(o) + '&page=' + str(page) + '&st=s,k&limit=20&key_param_included=true'
                    headers = {'User-Agent': random.choice(self.user_agents)}
                    #print(headers)
                    r = requests.get(headers = headers, url = url)
                    #print(r.content)
                    r.json()
                    if 0 == len(r.json()['ads']):
                        #sys.stdout.write('\n%s' % 'Close')
                        break
                    data.extend(r.json()['ads'])
                    delta = time.time() - previous
                    quantity = int(20 / delta)
                    previous = time.time()
                    sys.stdout.write('Number of items: %d (Total: %d | Speed: %d items / second)\r' % (page * 20,  len(data), quantity))
                    sys.stdout.flush()
                except:
                    pass
                
                time.sleep(np.random.choice([x/10 for x in range(3,12)]))

            
            if (page == 1):
                error += 1
            else:
                sys.stdout.write('\n')
                
            if (error > self.ERROR_LIMIT):
                break
            self.AREA_CODE += 1
            
        sys.stdout.write('\nFinish %d items' % (len(data)))
        sys.stdout.flush()
        return data
    
# start_time = time.time()

# bot = Crawler()
# data2 = bot.run()

# print("--- %s seconds ---" % (time.time() - start_time))