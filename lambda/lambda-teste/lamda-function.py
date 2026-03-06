import json

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('O teste de versionamento 1.0 foi um sucesso!')
    }
