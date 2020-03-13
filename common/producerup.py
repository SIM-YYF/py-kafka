from copy import deepcopy

from confluent_kafka import Producer, KafkaError
import json
import time
import pandas as pd
import datetime
import random

if __name__ == '__main__':

    p = Producer({
        'bootstrap.servers': 'localhost:9092',
    })


    def acked(err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))


    # 每次读取20条数据
    csv_reader = pd.read_csv('up.csv',
                             chunksize=20,  # 每次读取20数据
                             skiprows=0,  # 忽略标题行
                             sep=',',
                             low_memory=True,  # 低内存加载
                             nrows=10  # 只读2行数据
                             )
    for df in csv_reader:  # df为一个DataFrame

        for i in range(1):

            _records = df.to_dict(orient='records')
            for r in _records:
                record_value = json.dumps(r)
                print(record_value)
                p.produce('flink-st', value=record_value.encode('utf-8'), on_delivery=acked)
                p.poll(timeout=0)

            p.flush(timeout=10)
            time.sleep(10)

