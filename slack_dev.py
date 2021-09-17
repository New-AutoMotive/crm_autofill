import slack
from credentials import slack_token

client = slack.WebClient(token=slack_token)
client.chat_postMessage(channel='#civi-crm', text='Testing...')