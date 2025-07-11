import boto3
import uuid
import os
import json
import traceback

def lambda_handler(event, context):
    try:
        # Entrada
        body = event.get('body')
        if isinstance(body, str):
            body = json.loads(body)

        tenant_id = body['tenant_id']
        pelicula_datos = body['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]

        # Inserción en DynamoDB
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Log de INFO 
        log = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Pelicula creada correctamente",
                "Pelicula": pelicula,
                "resultado_dynamodb": response.get("ResponseMetadata", {})
            }
        }
        print(json.dumps(log))
        # Respuesta HTTP 
        return {
            'statusCode': 200,
            'body': json.dumps({
                'pelicula': pelicula,
                'dynamo_response': response
            })
        }

    except Exception as e:
        # Log de ERROR 
        error_log = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": str(e),
                "evento_original": event,
                "traceback": traceback.format_exc()
            }
        }
        print(json.dumps(error_log))
        # Respuesta HTTP de error
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
