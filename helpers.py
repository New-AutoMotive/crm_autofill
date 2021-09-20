# Some useful functions
from credentials import slack_token
import slack

# Print a message to slack's civi-crm channel

def print_to_slack(message, slack_token):
    client = slack.WebClient(token = slack_token)
    client.chat_postMessage(channel = "civi-crm", text = message)


