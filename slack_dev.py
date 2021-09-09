import slack
from credentials import slack_token

client = slack.WebClient(token='slack_token')
client.chat_postMessage(channel='#civi-crm', text='Hello! I am the New AutoMotive CiviCRM slack bot... this is just a test - I cannot do much just yet.')