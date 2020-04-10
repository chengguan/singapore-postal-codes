import requests
import time
from multiprocessing import Pool

def pcode_to_data(pcode):
    if int(pcode) % 1000 == 0:
        print(pcode)
    
    page = 1
    results = []
    
    while True:
        try:
            response = requests.get('http://developers.onemap.sg/commonapi/search?searchVal={0}&returnGeom=Y&getAddrDetails=Y&pageNum={1}'
                                    .format(pcode, page)) \
                               .json()
        except requests.exceptions.ConnectionError as e:
            print('Fetching {} failed. Retrying in 2 sec'.format(pcode))
            
            time.sleep(2)
            continue
            
        results = results + response['results']
    
        if response['totalNumPages'] > page:
            page = page + 1
        else:
            break
            
    return results

import json

if __name__ == '__main__':
    pool = Pool(processes=5)
    
    ref_sector_code = [
        1, 2, 3, 4, 5, 6,     # D01
        7, 8,                 # D02
        14, 15, 16,           # D03
        9, 10,                # D04
        11, 12, 13,           # D05
        17,                   # D06
        18, 19,               # D07
        20, 21,               # D08
        22, 23,               # D09
        24, 25, 26, 27,       # D10
        28, 29, 30,           # D11
        31, 32, 33,           # D12
        34, 35, 36, 37,       # D13
        38, 39, 40, 41,       # D14
        42, 43, 44, 45,       # D15
        46, 47, 48,           # D16
        49, 50, 81,           # D17
        51, 52,               # D18
        53, 54, 55, 82,       # D19
        56, 57,               # D20
        58, 59,               # D21
        60, 61, 62, 63, 64,   # D22
        65, 66, 67, 68,       # D23
        69, 70, 71,           # D24
        72, 73,               # D25
        77, 78,               # D26
        75, 76,               # D27
        79, 80                # D28
        ]
    postal_codes = []
    
    for each in ref_sector_code:
        each_sector = range(0,10000)
        each_sector = ['{0:06}'.format(p+(each*10000)) for p in each_sector]
        postal_codes = postal_codes + each_sector
    
    all_buildings = pool.map(pcode_to_data, postal_codes)
    all_buildings.sort(key=lambda b: (b['POSTAL'], b['SEARCHVAL']))

    jstr = json.dumps([y for x in all_buildings for y in x], indent=2, sort_keys=True)

    with open('buildings.json', 'w') as f:
        f.write(jstr.encode('utf-8'))

