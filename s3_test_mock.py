import unittest

import boto3
from moto import mock_s3


@mock_s3
class TestMockClassLevel(unittest.TestCase):

    def test(self):
        s3 = boto3.client('s3')

        s3.create_bucket(Bucket="bkt1")
        s3.create_bucket(Bucket="bkt2")

        s3.put_object(Bucket="bkt1", Key="key1", Body="AAAAAA")
        s3.put_object(Bucket="bkt1", Key="key3", Body="CCCCCC")
        s3.put_object(Bucket="bkt2", Key="key2", Body="BBBBBB")

        print_bucket('bkt1')
        print_bucket('bkt2')

        self.assertTrue(check_key('bkt1', 'key1'))
        self.assertFalse(check_key('bkt2', 'key1'))

        s3.copy_object(Bucket='bkt2', Key='key3Copy', CopySource={'Bucket': 'bkt1', 'Key': 'key3'})

        print_bucket('bkt1')
        print_bucket('bkt2')

        s3.delete_object(Bucket='bkt1', Key='key3')

        print_bucket('bkt1')
        print_bucket('bkt2')

        s3.delete_object(Bucket='bkt1', Key='key1')

        print_bucket('bkt1')
        print_bucket('bkt2')


def print_bucket(bucket):
    '''
    Imprime no console o conteúdo de cada objeto(key) em um bucket e seu conteúdo(body)
    :param bucket: nome do bucket que será impresso
    :return: void: o conteúdo será impresso na tela
    '''
    print('\n \033[1;32m' + f' CONTEÚDO DO BUCKET {bucket}'.center(100, '*') + '\x1b[0m')
    s3 = boto3.client("s3")

    if s3.list_objects_v2(Bucket=bucket)['KeyCount'] == 0:
        print("BUCKET SEM ARQUIVOS")
    else:
        for key in s3.list_objects(Bucket=bucket)['Contents']:
            body = s3.get_object(Bucket=bucket, Key=key['Key'])['Body'].read().decode("utf-8")
            print(f'{key["Key"]} = {body}')
    print("\033[93m=" * 100)


def check_key(bucket, key):
    '''
    Verifica se um objeto(key) existe em um bucket
    :param bucket: nome do bucket
    :param key: nome do objeto a ser procurado
    :return: boolean indicando a presença do objeto
    '''
    s3 = boto3.client("s3")
    return key in list(map(lambda a: a['Key'], s3.list_objects(Bucket=bucket)['Contents']))
