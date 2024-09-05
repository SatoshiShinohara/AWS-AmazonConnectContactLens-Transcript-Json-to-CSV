import json
import boto3

AMAZON_CONNECT_INSTANCE_ID = '＜Amazon Connect インスタンス ID＞'
OUTPUT_S3_BUCKET = '＜csv ファイル出力先 S3 バケット＞'

def lambda_handler(event, context):
    
    # PutObject 以外は処理不要のため終了する
    reason = event.get('detail').get('reason')
    if reason != 'PutObject':
        return 0
    
    s3 = boto3.client('s3')
    
    # S3 バケットのオブジェクトを取得
    bucket = event.get('detail').get('bucket').get('name')
    key = event.get('detail').get('object').get('key')
    
    # 日時取得
    daytime =  key[-25:].replace('.json', '')
    
    # JSON取得
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response.get('Body').read().decode('utf-8')
    data = json.loads(data)
    
    # コンタクトID取得
    customer_metadata = data.get('CustomerMetadata')
    contact_id = customer_metadata.get('ContactId')
    
    ## Amazon Connect から情報を取得
    client = boto3.client('connect')
    
    # Contact 情報取得
    response = client.describe_contact(
        InstanceId=AMAZON_CONNECT_INSTANCE_ID,
        ContactId=contact_id
    )
    
    queue_id = response.get('Contact').get('QueueInfo').get('Id')
    user_id = response.get('Contact').get('AgentInfo').get('Id')
    
    # キュー情報取得
    response = client.describe_queue(
        InstanceId=AMAZON_CONNECT_INSTANCE_ID,
        QueueId=queue_id
    )
    
    queue_name = response.get('Queue').get('Name')
    
    # オペレーター情報取得
    response = client.describe_user(
        UserId=user_id,
        InstanceId=AMAZON_CONNECT_INSTANCE_ID
    )
    
    user_name = response.get('User').get('Username')
    
    # "Transcript"の中にある"Content"を抽出
    transcript_content = []
    
    for item in data['Transcript']:
        participant_id = item.get('ParticipantId')
        content = item.get('Content')
        sentiment = item.get('Sentiment')

        # csv生成
        row = daytime + ',' + contact_id + ',' + queue_name + ',' + user_name + ',' + participant_id + ',' + content + ',' + sentiment
        transcript_content.append(row)
    output_str = '\n'.join(transcript_content)

    # 出力キーを作成
    output_key = key.replace('Analysis/Voice/', 'Transcript/Voice/CSV/').replace('.json', '.csv')

    # S3バケットに出力
    s3.put_object(Bucket=OUTPUT_S3_BUCKET, Key=output_key, Body=output_str)
    
    return 0
