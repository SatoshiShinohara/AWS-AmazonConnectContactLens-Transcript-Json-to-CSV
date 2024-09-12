import json
import boto3

AMAZON_CONNECT_INSTANCE_ID = '＜Amazon Connect の ID＞'
INPUT_S3_BUCKET = '＜json が出力される Amazon Connect の S3 バケット＞'
OUTPUT_S3_BUCKET = '＜csv を出力する S3 バケット＞'
OVERRIDE = True

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # INPUT_S3_BUCKET の json ファイル一覧を取得
    response = s3.list_objects_v2(
        Bucket=INPUT_S3_BUCKET,
        Prefix ='Analysis/Voice/'
    )
    
    contents = response.get('Contents')
    
    # 取得したファイルの数だけループ
    for content in contents:
        # S3 バケットのオブジェクトを取得
        bucket = INPUT_S3_BUCKET
        key = content.get('Key')
        
        # 出力ファイル名作成
        output_key = key.replace('Analysis/Voice/', 'Transcript/Voice/CSV/').replace('.json', '.csv')
        
        # 出力ファイルの有無をチェック
        try:
            s3.head_object(Bucket=OUTPUT_S3_BUCKET, Key=output_key)

            # 上書きフラグが True なら処理を続行する / False ならそのファイルの処理をスキップする
            if OVERRIDE != True:
                continue
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print ('Object does not exist.')
            else:
                print ('An error occurred: {e}')
                return 1

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

        # S3バケットに出力
        s3.put_object(Bucket=OUTPUT_S3_BUCKET, Key=output_key, Body=output_str)
        
        # 処理内容を出力
        print ('s3://' + INPUT_S3_BUCKET + key + ' -> s3://' + OUTPUT_S3_BUCKET + output_key)
        
    return 0
