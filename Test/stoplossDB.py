import sys
import pymongo
import os

#Example Stop price DB
#stop price array - {'MIN': 3.5909999999999997, 'TSI': 5.310499999999999, 'VEON': 2.299, 'WIT': 4.313, 'ELVT': 4.5125, 'DHF': 2.964, 'IDRA': 3.3914999999999997, 'PETX': 4.5125}
### Standard URI format: mongodb://[dbuser:dbpassword@]host:port/dbname

uri = str(os.environ.get('MONGODB_URI'))

client = pymongo.MongoClient(uri)
db = client.get_default_database()
stopprice = db['stopprice']

def stopprice_update(sym,price):
    global client, db, stopprice
    
    query = {'stock': sym}
    newvalue = {'stock': sym, 'stopprice': price}
    stopprice.replace_one(query,newvalue,True)
    return

def stopprice_delete(sym):
    global client, db, stopprice
    
    query = {'stock': sym}
    #newvalue = {'stock': sym, 'stopprice': price}
    stopprice.delete_one(query)
    return

def stopprice_read(sym):
    global client, db, stopprice
    z = 0.0
    query = {'stock': sym}

    if stopprice.count_documents(query) >= 1:
        print("entry exists for sym - {}".format(sym))
        for doc in stopprice.find(query):
            return doc['stopprice']
        
    else:
        print("Entry doesn't exists for sym - {}".format(sym))
        return z
'''
abc = 'test1'
if stopprice_read(abc) == 0.0:
    stopprice_update(abc,'106.55')
print ("final result - {}".format(stopprice_read(abc)))
'''