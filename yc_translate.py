import requests
import time
import datetime
import jwt
import psycopg2
from progress.bar import IncrementalBar


def gettoken():
	service_account_id = "SERVICE_ACCOUNT_ID" # service account ID
	key_id = "RESOURCE_KEY_ID" # Resource key ID, which belongs service account.
	with open("private.key", 'r') as private: # private key file
		private_key = private.read() 

	now = int(time.time())
	payload = {
		'aud': 'https://iam.api.cloud.yandex.net/iam/v1/tokens',
		'iss': service_account_id,
		'iat': now,
		'exp': now + 360}
	encoded_token = jwt.encode(
		payload,
		private_key,
		algorithm='PS256',
		headers={'kid': key_id})
	jtoken = { 'jwt': encoded_token }
	iam_token = requests.post(payload['aud'], json = {"jwt": encoded_token}, headers = {"Content-Type": "application/json"})
	response = iam_token.json()
	expiresat = response['expiresAt'][:-11] # remove nanoseconds (sick, who need ns?)
	token = response['iamToken'] # got iam token
	return expiresat, token

def translate (entext, token):
	text = {
		"folderId": "FOLDER_ID", # Yandex Cloud folder ID
		"texts": entext,
		"targetLanguageCode": "ru", # Change 'ru' to language code you need
	}
	headers = {
		"Authorization": "Bearer " + token,
		"Content-Type": "application/json"
	}
	transurl = "https://translate.api.cloud.yandex.net/translate/v2/translate"
	translation = requests.post(transurl, json = text, headers = headers)
	t = translation.json()
	return t

expire, token = gettoken()
db = psycopg2.connect(dbname='DB1_NAME', user='USERNAME', password='PASSWORD', host='localhost')
dbd = psycopg2.connect(dbname='DB1_NAME', user='USERNAME', password='PASSWORD', host='localhost')
pg = db.cursor()
pg.execute("select count(FIELDNAME) from TABLENAME where NEWFIELDNAME is null") # We have to count rows with empty NEWFIELDNAME. Remember to have 'Null' there
count = pg.fetchone()[0]
pgins = dbd.cursor()
bar = IncrementalBar('Processing translate', max = count)
pg.execute("select * from TABLENAME where NEWFIELDNAME is null")
expiresAt = time.mktime(datetime.datetime.strptime(expire, "%Y-%m-%dT%H:%M:%S").timetuple())

while True:
	row = pg.fetchone()
	if row == None:
		break

	time = expiresAt - datetime.datetime.now().timestamp()
	if time < 3000: # Check if token is being expired soon and need to be updated
		expire, token = gettoken() # Update token if so.

	trans = translate(row[1], token)
	pgins.execute("update TABLENAME set NEWFIELDNAME=%s where id=%s",(trans['translations'][0]['text'],row[0]))
	dbd.commit()
	bar.next()

print("\n All done!")
