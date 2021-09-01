import mysql.connector

cnx = mysql.connector.connect(user='ben', password='new_auto_20',
                              host='localhost',
                              database='civicrm_contact')

query = ("SELECT display_name FROM civicrm_contact LIMIT 5")

cursor = cnx.cursor()

cursor.execute(query)

for x in cursor:
	print(x)

cursor.close()
cnx.close()



