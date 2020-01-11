from confluent_kafka import Producer, KafkaError
import json
import time
import pandas as pd
import datetime

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
    csv_reader = pd.read_csv('断泵记录-2018-1-4.csv',
                             chunksize=20,  # 每次读取20数据
                             skiprows=0,  # 忽略标题行
                             sep=',',
                             low_memory=True,  # 低内存加载
                             nrows=10  # 只读2行数据
                             )
    for df in csv_reader:  # df为一个DataFrame

        print('-----------指定列数据变量-------------')
        # 指定列，从DataFrame取值
        series = df[['timestamp', 'T01', 'T02']]
        # 对某列的数据求值
        t_01_ser = series['T01']
        t_01_np_array = t_01_ser.values
        t_01_list = t_01_ser.values.tolist()
        print(t_01_list)

        print('-----------对每列的数据遍历-------------')
        # 对每列的数据遍历
        for col in df.items():
            print(col[0] + ' = ' + str(col[1].values.tolist()))
        print('-----------对每行数据遍历-------------')
        # 对每行数据遍历
        for row in df.values:
            print(row.tolist())

        print('-----------对DataFrame分组-------------')
        grouped = df.groupby('timestamp')
        for name, group in grouped:
            print(name, group.to_dict(orient='records'))

        print('-----------DataFrame转为dict-------------')
        _records = df.to_dict(orient='records')
        for r in _records:
            time_stamp = r['timestamp']
            del r['timestamp']
            print(r)
            for key, value in r.items():
                print(f'{key} = {value}')
                _point_data = dict()
                _point_data.update({'tags': {'node_id': key}, 'fields': {'value': value}, 'time':  time.mktime(datetime.datetime.strptime(time_stamp, '%Y-%m-%d %H:%M:%S').timetuple())})
                record_key = "alice"
                record_value = json.dumps(_point_data)
                p.produce('test', key=record_key, value=record_value.encode('utf-8'), on_delivery=acked,
                          timestamp=int(round(time.time() * 1000000)), headers={})
                p.poll(timeout=0)

            p.flush(timeout=10)
