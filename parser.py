import sys
import getopt
import re
import time
import mwparserfromhell
import configparser
import pymongo
import json
import datetime
import requests

session = requests.Session()

class Data:
    def __init__(self, config):
        connUrlTemplate = "mongodb+srv://{user}:{password}@trainingcluster.3uayb.mongodb.net/{dbname}?retryWrites=true&w=majority"
        db_name = config['db_name']
        connUrl = connUrlTemplate.format(user = config['user'], password = config['password'], dbname = db_name)
        self.client = pymongo.MongoClient(connUrl)
        self.db = self.client[db_name]
        self.getBaseData()
        self.charaImageTemplate = "http://game-a.granbluefantasy.jp/assets_en/img_mid/sp/assets/npc/f/{characterId}_01.jpg"
        self.weaponImageTemplate = "http://game-a.granbluefantasy.jp/assets_en/img_mid/sp/assets/weapon/ls/{weaponId}.jpg"
        self.summonImageTemplate = "http://game-a.granbluefantasy.jp/assets_en/img_mid/sp/assets/summon/party_main/{summonId}_01.jpg"

    def getBaseData(self):
        self.elements = self.db['Element']
        self.races = self.db['Race']
        self.rarities = self.db['Rarity']
        self.styles = self.db['Style']
        self.weaponTypes = self.db['WeaponType']


def sessionGet(url, params = {}):
    request = session.get(url = url, params = params)

    if request.status_code != 200:
        print('Got status code', request.status_code, 'for', url)
  
    return request

def getConfigData():
    configParser = configparser.ConfigParser()
    configParser.read('config.ini')
    config = dict()
    config['db_name'] = configParser['MONGO']['db_name']
    config['user'] = configParser['MONGO']['user']
    config['password'] = configParser['MONGO']['password']

    return config

def parseBaseData(table, fields): 
    results = []
    ids = set()
    limit = 500
    offset = 0

    while True:
        request = sessionGet(
            url = 'https://gbf.wiki/api.php',
            params = {
                'action': 'cargoquery',
                'tables': table,
                'fields': fields,
                'order_by': 'id',
                'format': 'json',
                'limit': limit,
                'offset': offset
            })
        request_json = request.json()

        if 'warnings' in request_json:
            print(request_json['warnings'])
            sys.exit(1)
        elif 'error' in request_json:
            print(request_json['error'])
            sys.exit(1)

        offset += limit
        for res in request_json['cargoquery']:
            obj = res['title']
            unique = obj['id']
            if not unique in ids:
                results += [obj]
                ids.add(unique)
        
        if len(request_json['cargoquery']) != limit:
            break

    return results

def updateBaseSummons(data, summons):
    to_insert = []
    for summon in summons:
        element = data.elements.find_one({"name":summon['element'].capitalize()})
        rarity = data.rarities.find_one({"name":summon['rarity']})
        s = {
            "name":summon['name'],
            "maxUncap": summon['evo max'],
            "baseUncap": summon['evo base'],
            "imgUrl": data.summonImageTemplate.format(summonId = summon['id']),
            "element":element,
            "rarity":rarity
        }
        to_insert += [s]
    result = data.db['BaseSummon'].insert_many(to_insert)

def updateBaseWeapons(data, weapons):
    to_insert = []
    for weapon in weapons:
        element = data.elements.find_one({"name":weapon['element'].capitalize()})
        rarity = data.rarities.find_one({"name":weapon['rarity']})
        wType = data.weaponTypes.find_one({"name":weapon['type'].capitalize()})
        w = {
            "name": weapon['name'],
            "maxUncap": weapon['evo max'],
            "baseUncap": weapon['evo base'],
            "imgUrl": data.weaponImageTemplate.format(weaponId = weapon['id']),
            "element": element,
            "rarity": rarity,
            "weaponType": wType
        }
        to_insert += [w]
    result = data.db['BaseWeapon'].insert_many(to_insert)

def updateBaseCharacters(data, characters):
    to_insert = []
    for character in characters:
        element = data.elements.find_one({"name":character['element'].capitalize()})
        rarity = data.rarities.find_one({"name":character['rarity']})
        style = data.styles.find_one({"name":character['type']})
        race = []
        weapon = []

        rTokens = character['race'].split(',')
        for token in rTokens:
            r = data.races.find_one({"name":token})
            race += [r]
        wTokens = character['weapon'].split(',')
        for token in wTokens:
            w = data.weaponTypes.find_one({"name":token})
            weapon += [w]
        
        c = {
            "name": character['name'],
            "maxUncap": character['max evo'],
            "imgUrl": data.charaImageTemplate.format(characterId = character['id']),
            "race": race,
            "element": element,
            "rarity": rarity,
            "style": style,
            "weaponType": weapon
        }
        to_insert += [c]
    result = data.db['BaseCharacter'].insert_many(to_insert)
    
def setBaseSummons(data):
    print("Fetching summon data...")
    summons = parseBaseData('summons','id,name,evo_base,evo_max,rarity,element')
    print("Preparing data and inserting in database...")
    updateBaseSummons(data,summons)

def setBaseWeapons(data):
    print("Fetching weapon data...")
    weapons = parseBaseData('weapons','id,name,evo_base,evo_max,rarity,element,type')
    print("Preparing data and inserting in database...")
    updateBaseWeapons(data, weapons)

def setBaseCharacters(data):
    print("Fetching characters data...")
    characters = parseBaseData('characters','id,name,max_evo,rarity,element,type,race,weapon')
    print("Preparing data and inserting in database...")
    updateBaseCharacters(data, characters)

def main():
    gbfData = Data(getConfigData())
    setBaseSummons(gbfData)
    setBaseWeapons(gbfData)
    setBaseCharacters(gbfData)

if __name__ == "__main__":
    main()