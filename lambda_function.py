import json
import boto3

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # S3 バケットのオブジェクトを取得
    bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']
    # 検証用 直接バケットとキーを設定することでそのファイルの csv 化が可能
    # bucket = ''
    # key = ''
    
    # 日時取得
    daytime =  key[-25:].replace('.json', '')
    
    # JSON取得
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    data = json.loads(data)
    
    # コンタクトID取得
    customer_metadata = data['CustomerMetadata']
    contact_id = customer_metadata['ContactId']
    
    # "Transcript"の中にある"Content"を抽出
    transcript_content = []
    
    for item in data['Transcript']:
        participant_id = item['ParticipantId']
        content = item['Content']
        
        # csv生成
        row = daytime + ',' + contact_id + ',' + participant_id + ',' + content
        transcript_content.append(row)
    output_str = '\n'.join(transcript_content)
    
    

    # 出力キーを作成
    # 出力バケット・キー名は任意で変更可能
    output_bucket = ''
    output_key = key.replace('Analysis/Voice/', 'Transcript/Voice/CSV/').replace('.json', '.csv')

    # S3バケットに出力
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=output_str)
    
    return 0
