
import sys
import time
import uuid
import redis
import random
import logging
import config


class Worker:
 
    def __init__(self, name_channel='warnings', genr_num='generate', appl_num='application'):
        self.name_channel = name_channel
        self.genr_num = genr_num
        self.appl_num = appl_num
        self.time_working_programm = 10
        self.generate_time_sleep = 0.5

        self.waiting_time = 1.1

    def conn_channel(self):
        self.conn_red = redis.StrictRedis(**config.REDIS)
        self.psbb_red = self.conn_red.pubsub()
        self.psbb_red.subscribe(self.name_channel)
        self.appl_num = uuid.uuid4().hex
        begin_time_generate = time.time()

        # Ожидаем 5 секунд с момента подключения в канал, прежде чем взять на себя роль генератора сообщений
        waiting_generate = 5
        while True:
            message = self.psbb_red.get_message()
            if message:
                data_mess = str(message['data'])
                if ':GENERATE' in data_mess:
                    self.genr_num = data_mess[2:data_mess.find(':')]
                    break
            if time.time() - begin_time_generate > waiting_generate:
                self.genr_num = self.appl_num
                break
            time.sleep(1)

        if self.genr_num == self.appl_num:
            self.work_generate()
        else:
            self.treatment_message()


    def work_generate(self):
        begin_time_generate = time.time()
        # Отправляем в канал информацию о новом генераторе
        self.conn_red.publish(self.name_channel, self.genr_num)
        time.sleep(self.generate_time_slee)
        while True:
            # Генерируем новое сообщение и отправляем в канал
            self.conn_red.publish(self.name_channel, self.genr_num + ':GENERATE ' + self.generate_text())
            # Проверка времени работы программы
            if time.time() - begin_time_generate > self.time_working_programm:
                break
            time.sleep(self.generate_time_slee)


    def split_message_info(self, message):
        return message.split()[1]

    def get_name_new_genr(self, message):
        return message[2:message.find(':')] 

    def treatment_message(self):
        begin_time_generate = time.time()
        last_message_time = 0
        while True:
            # Получаем сообщение из канала
            message = self.psbb_red.get_message()
            if message:
                data_mess = str(message['data'])
                processed_message = self.psbb_red.get_message()
                last_message_time = time.time()
                # Проверяем отправил ли генератор сообщение и ответил ли кто-либо на него
                if self.genr_num in data_mess and not processed_message:
                    # Сообщение имеет ошибку
                    if random.randint(0, 100) <= 5:
                        self.save_message_with_error(data_mess)
                        continue
                    else:
                        # Отправляем сообщение о том, что сообщение генератора обработано
                        self.conn_red.publish(self.name_channel, self.appl_num + 
                            ':APPLICATION:PROCESSED ' + self.split_message_info(data_mess))
                        # print('processed ' + data_mess.split(' ')[1])
                        time.sleep(0.2)
                        continue
                # Если в канале появился новый генератор
                if self.genr_num not in data_mess and ':GENERATE' in data_mess:
                        self.genr_num = self.get_name_new_genr(data_mess)
                        continue
            # Если генератор не отправляет сообщение > self.waiting_time берем на себя функцию генератора
            if last_message_time and time.time() - last_message_time > self.waiting_time:
                self.genr_num = self.appl_num.split(':')[0]
                self.work_generate()
            # Проверка времени работы программы
            if time.time() - begin_time_generate > self.time_working_programm:
                break
            time.sleep(0.2)

    def save_message_with_error(self, data_mess):
        self.conn_red.publish(self.name_channel, self.appl_num
                              + ':APPLICATION:PROCESSED ' + self.split_message_info(data_mess) + ' ERROR')
        dict_to_store = {'Message': self.split_message_info(data_mess)[:-1]}
        # Записываем ошибку в базу
        self.conn_red.hmset('Error:' + self.appl_num, dict_to_store)
        time.sleep(0.2)


    def generate_text(self):
        return ''.join([chr(x) for x in list(random.randint(33, 125) for x in range(20))])


def Processing_error():
    r = redis.StrictRedis(**config.REDIS)
    d_err_from_redis = r.keys('Error:*')
    for element in d_err_from_redis:
        print(element)
        try:
            print(r.hget(element, 'Message'))
            r.hdel(element, 'Message')
            r.delete(element)
        except redis.exceptions.ResponseError as e:
            print(element, ' ERROR: ', e)



if __name__ == '__main__':
    get_err = False
    for param in sys.argv:
        if param == 'getErrors':
            get_err = True
    if get_err:
        Processing_error()
    else:
        worker_ex = Worker('warnings')
        worker_ex.conn_channel()