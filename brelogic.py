
import boto3
import json

client = boto3.client('dynamodb')
lambdaclient = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')

from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print ('----------------------------')

    try:
        #iterate over the records 
        for record in event['Records']:
            #handle event types 
            if record['eventName'] == 'INSERT':
                print ('Handle INSERT event')
                #Get Image content
                newImage = record['dynamodb']['NewImage']
                #parse values 
                newrulenum = newImage['rulenum']['S']
                #print it out
                print('New row added with rulenum. ' + newrulenum)
                get_rules(newrulenum)
                print ('Done handling the INSERT event')
        print ('----------------------------')
    except Exception as e:
        print(e)
        print ('----------------------------')
        print('Error getting object {}')
        raise e

    
def get_rules(rulenum):
    table = dynamodb.Table('RulesTable')

    result = table.query(
        KeyConditionExpression=Key('rulenum').eq(rulenum)
    )
    print(result['Items'])
    for item in result['Items']:
            #handle function types 
            if item['function'] == 'func1':
                #do something - maybe call function 1
                print("---- function 1")
                response = lambdaclient.invoke(
                    FunctionName = 'arn for function 1',
                    InvocationType ='RequestResponse',
                    Payload = json.dumps(item)
                    )
                valueReturned = response['Payload']
                print(valueReturned.read())
            elif item['function'] == 'func2':
                #do something - maybe call function 1
                print("---- function 2")
            elif item['function'] == 'func3':
                #do something - maybe call function 1
                print("---- function 3")

