# Some useful functions


# Print a message to slack's civi-crm channel

def print_to_slack(message):
    client.chat_postMessage(channel = "civi-crm", text = message)


