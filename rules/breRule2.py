import json

print('Loading function 2 to complete rule 1')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    return event
