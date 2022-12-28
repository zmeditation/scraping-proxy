#!/usr/bin/python

from datetime import datetime
import threading
import os
try:
	import requests
except:
	print("Installing missing libraries..")
	os.system("pip install requests")
finally:
	import requests
from mysql.connector import errorcode
from mysql.connector.errors import Error

from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import mysql.connector

import random

# For use in signaling
shutdown_event = threading.Event()
ips = [
    'http://ArtemYProxy:ArtemYPassword_session-rgnb9dqx_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-g3x8388o_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-pz6669ke_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-abhois72_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-yqm49i0s_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-s5eu84nd_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-43n4ftoo_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-tlpdrzb6_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-99l8q944_lifetime-24h@geo.iproyal.com:12321',
    'http://ArtemYProxy:ArtemYPassword_session-mqrbuct2_lifetime-24h@geo.iproyal.com:12321'
    ]

# Set username, password, etc
dbconfig = {
  "user":"root",
  "password":"",
  "database":"mydb"
}  

def create_table():
  query = u"""CREATE TABLE mytb
    (rowid INTEGER NOT NULL primary key AUTO_INCREMENT,
    token_address CHAR(100),
    token_id CHAR(100),
    id CHAR(100),
    user CHAR(100),
    status CHAR(100),
    uri CHAR(100),
    name CHAR(100),
    description CHAR(100),
    image_url CHAR(100),
    m_name CHAR(100),
    m_image CHAR(100),
    m_rarity CHAR(100),
    m_series CHAR(100),
    m_artists CHAR(100),
    m_edition CHAR(100),
    m_writers CHAR(100),
    m_dropDate CHAR(100),
    m_mintDate CHAR(100),
    m_publisher CHAR(100),
    m_startYear CHAR(100),
    m_characters CHAR(100),
    m_comicNumber CHAR(100),
    m_description CHAR(100),
    m_coverArtists CHAR(100),
    m_totalEditions CHAR(100),
    c_name CHAR(100),
    c_icon_url CHAR(100),
    created_at CHAR(100),
    updated_at CHAR(100),
    m_brand CHAR(100),
    m_licensor CHAR(100),
    m_editionType CHAR(100));"""

  cnx = mysql.connector.connect(**dbconfig)
  cursor = cnx.cursor()
  cursor.execute("DROP TABLE IF EXISTS mytb;")
  cursor.execute(query)
  cursor.close()
  cnx.close()
  print("Created table")

def rnd_user(num, threadid=1):
  cnx = mysql.connector.connect(**dbconfig)
  cnx.autocommit = True
  cursor = cnx.cursor() 
  for x in range(num+1, num+800000):
      print("Thread Id:", threadid, "Data Counter:", x)
    #   proxy = random.randint(0, len(ips) - 1)
      proxy = threadid-1
      proxies = {
        'https': ips[proxy]
      }
      r = requests.get('https://api.x.immutable.com/v1/assets/0xa7aefead2f25972d80516628417ac46b3f2604af/'+ str(x))
      if r:
          data=r.json()
          if data:
              m_artists=None
              m_writers=None
              m_publisher=None
              m_startYear=None
              m_characters=None
              m_comicNumber=None
              m_coverArtists=None
              m_name=None
              m_image=None
              m_rarity= None
              m_series=None
              m_edition=None
              m_dropDate=None
              m_mintDate=None
              m_description=None
              m_totalEditions=None
              m_brand=None
              m_licensor=None
              m_editionType=None
              token_address=None
              token_id=None    
              id=None
              user=None
              status=None
              uri=None
              name=None
              description=None
              image_url=None
              created_at=None
              updated_at=None
              c_name=None    
              c_icon_url=None

              if 'metadata' in data:
                  if  'artists' in data['metadata']:
                      m_artists=data['metadata']['artists']
                  if  'writers' in data['metadata']:     
                      m_writers=data['metadata']['writers']
                  if  'publisher' in data['metadata']:
                      m_publisher=data['metadata']['publisher']
                  if  'startYear' in data['metadata']:
                      m_startYear=data['metadata']['startYear']
                  if  'characters' in data['metadata']:
                      m_characters=data['metadata']['characters']
                  if  'comicNumber' in data['metadata']:
                      m_comicNumber=data['metadata']['comicNumber']
                  if  'coverArtists' in data['metadata']:
                      m_coverArtists=data['metadata']['coverArtists']
                  if  'name' in data['metadata']:
                      m_name=data['metadata']['name']
                  if  'image' in data['metadata']:
                      m_image=data['metadata']['image']
                  if  'rarity' in data['metadata']:
                      m_rarity=data['metadata']['rarity']
                  if  'series' in data['metadata']:
                      m_series=data['metadata']['series']
                  if  'edition' in data['metadata']:
                      m_edition=data['metadata']['edition']
                  if  'dropDate' in data['metadata']:
                      m_dropDate=data['metadata']['dropDate']
                  if  'mintDate' in data['metadata']:
                      m_mintDate=data['metadata']['mintDate']
                  if  'description' in data['metadata']:
                      m_description=data['metadata']['description']
                  if  'totalEditions' in data['metadata']:
                      m_totalEditions=data['metadata']['totalEditions']
                  if  'brand' in data['metadata']:
                      m_brand=data['metadata']['brand']
                  if  'licensor' in data['metadata']:
                      m_licensor=data['metadata']['licensor']
                  if  'editionType' in data['metadata']:
                      m_editionType=data['metadata']['editionType']
              if  'token_address' in data:
                  token_address=data['token_address']
              if  'token_id' in data:    
                  token_id=data['token_id']
              if  'id' in data:      
                  id=data['id']
              if  'user' in data:  
                  user=data['user']
              if  'status' in data:  
                  status=data['status']
              if  'uri' in data:  
                  uri=data['uri']
              if  'name' in data:  
                  name=data['name']
              if  'description' in data:  
                  description=data['description']
              if  'image_url' in data:  
                  image_url=data['image_url']
              
              if  'created_at' in data:  
                  created_at=data['created_at']
              if  'updated_at' in data:  
                  updated_at=data['updated_at']
              if 'collection' in data:
                  if  'name' in data['collection']:  
                      c_name=data['collection']['name']
                  if  'icon_url' in data['collection']:  
                      c_icon_url=data['collection']['icon_url']    
          
              params = (token_address,token_id,id,user,status,uri,name,description,image_url,m_name,m_image,m_rarity,m_series,m_artists,m_edition,m_writers,m_dropDate,m_mintDate,m_publisher,m_startYear,m_characters,m_comicNumber,m_description,m_coverArtists,m_totalEditions,c_name,c_icon_url,created_at,updated_at,m_brand,m_licensor,m_editionType)
          
              cursor.execute("INSERT INTO mytb (token_address, token_id, id, user, status, uri, name, description, image_url, m_name, m_image, m_rarity, m_series, m_artists, m_edition, m_writers, m_dropDate, m_mintDate, m_publisher, m_startYear, m_characters, m_comicNumber, m_description, m_coverArtists, m_totalEditions, c_name, c_icon_url, created_at, updated_at, m_brand, m_licensor, m_editionType) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", params)
  cnx.close()

def main():

  # Make sure user account works
  try:
    cnx = mysql.connector.connect(**dbconfig)
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      cnx = mysql.connector.connect(
          host="localhost",
          user="root",
          password=""
      )
      conn = cnx.cursor()
      conn.execute("CREATE DATABASE IF NOT EXISTS mydb")
      print("Created Database. Please run again.")
    else:
      print("Other Error: %s" % err)
    os._exit(1)
  else:
    cnx.close()

  # Create the table
  create_table()

  # Hold threads
  threads = []
  threadId = 1

  # Loop/create/start threads
  for x in range(10):
    t = threading.Thread(target=rnd_user, args=(x*800000,threadId,))
    t.start()
    threads.append(t)
    threadId += 1
  
  print("Waiting for threads to complete...")

  try:
    for i in threads:
      i.join(timeout=1.0)
  except (KeyboardInterrupt, SystemExit):
    print("Caught Ctrl-C. Cleaning up. Exiting.")
    shutdown_event.set()

main()