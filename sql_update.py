from os import strerror
import mysql.connector

cnx = mysql.connector.connect(user='ben', password='new_auto_2020', host='localhost', database='wordpress')

# query = ("SELECT id FROM civicrm_contact WHERE contact_type='Individual'")

query = ("""
SELECT contact_id, postal_code  
FROM civicrm_address 
WHERE contact_id in (SELECT id FROM civicrm_contact WHERE contact_type='Individual')
AND postal_code != 'SW1A 0AA'
""")

cursor = cnx.cursor()

cursor.execute(query)


pcs = []
ids = []

for x in cursor:
    ids.append(str(x[0]))
    pcs.append(str(x[1]))

pc_id = dict(zip(ids, pcs))

cursor.close()
cnx.close()
# Looks like this: {'5': 'TN14 6LX', '4': 'SW22BP'}

pc_pcon ={
    'TN146LX': '923',
    'SW22BP': '983'
}

final_dict = {}

for key in pc_id.keys():
    value = pc_id[key]
    k = value.strip().replace(' ', '').replace(' ', '')
    if k in pc_pcon.keys():
        final_dict[key] = pc_pcon[k]
    else:
        pass

print(final_dict)

query = """
    INSERT INTO civicrm_relationship (contact_id_a, contact_id_b, relationship_type_id, start_date, end_date, is_active, description, is_permission_a_b, is_permission_b_a, case_id)
    VALUES"""

for contact_id_a in final_dict.keys():
    contact_id_b = final_dict[contact_id_a]
    print(contact_id_a)
    print(contact_id_b)

    query = query + """
    ({con_id_a}, {con_id_b}, 16, NULL, NULL, 1, NULL, 0, 0, NULL),
    """.format(con_id_a = contact_id_a, con_id_b = contact_id_b)

# query = query + ';'
print(query)


cnx_2 = mysql.connector.connect(user='ben', password='new_auto_2020', host='localhost', database='wordpress')

cursor2 = cnx_2.cursor()
cursor2.execute(query)
cursor2.close()
cnx_2.close()