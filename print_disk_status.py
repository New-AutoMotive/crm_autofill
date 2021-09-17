import slack
import os
from credentials import slack_token
os.chdir('/home/ben/crm_autofill/crm_autofill')

dsk = ''

with open('disk_status.txt', 'r') as f:
    line = f.readlines()
    dsk = line[3]
dsk_l = dsk.split(' ')
dsk_l = [x for x in dsk_l if len(x) > 1]

size = dsk_l[1]
used = dsk_l[2]
free = dsk_l[3]
percent_used = dsk_l[4]
percent_used_int = int(percent_used[:2])

if percent_used_int < 60:
    message = "Server disk usage update: the server has a {s} disk, of which {u} is used, and {f} is free. The disk is {p} full. The server has adequate space available.".format(s=size, u=used, f=free, p=percent_used)
elif (percent_used_int >= 60) & (percent_used_int <80):
    message = "Server disk usage update: the server has a {s} disk, of which {u} is used, and {f} is free. The disk is {p} full. \n \nAction recommended: increase disk size.".format(s=size, u=used, f=free, p=percent_used)
elif percent_used_int >= 80:
    message = "Server disk WARNING: the server has a {s} disk, of which {u} is used, and {f} is free. The disk is {p} full. \n \nACTION REQUIRED: increase disk size soon to prevent server crashing.".format(s=size, u=used, f=free, p=percent_used)

client = slack.WebClient(token=slack_token)
client.chat_postMessage(channel = "civi-crm", text = message)
