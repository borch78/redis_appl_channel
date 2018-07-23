

import sys
import time
import redis
import random


class Worker:
    """docstring"""
 
    def __init__(self, name_channel='warnings', genr_num='generate',appl_num='application'):
        """Constructor"""
        self.name_channel = name_channel
        self.genr_num = genr_num
        self.appl_num = appl_num


    def conn_channel(self):
        self.conn_red = redis.StrictRedis(host='localhost', port=6379)
        self.psbb_red = self.conn_red.pubsub()
        self.psbb_red.subscribe(self.name_channel)

        self.appl_num = 'APP-' + str(random.randint(0, 10000))
        begin_time_generate = time.time()
        
        waiting_generate = 5
        while True:
            message = self.psbb_red.get_message()
            if message:
                data_mess = str(message['data'])
                if ':GENERATE' in data_mess:
                    self.genr_num = data_mess[2:data_mess.find(':')]
                    break
            if time.time() - begin_time_generate > waiting_generate:
                self.genr_num = app_ident
                break
            time.sleep(1)

        if self.genr_num == self.appl_num:
            self.work_generate()
        else:
            self.treatment_message()


    def work_generate(self):
        begin_time_generate = time.time()
        #Send determine for generator
        self.conn_red.publish(self.name_channel, self.genr_num)
        time.sleep(0.5)
        while True:
            #Generate new message and send in channel
            self.conn_red.publish(self.name_channel, self.genr_num + ':GENERATE ' + self.generate_text())
            #Timeline working programm
            if time.time() - begin_time_generate > 10:
                break
            time.sleep(0.5)   


    def split_message_info(self, message):
        return message.split()[1]

    def get_name_new_genr(self, message):
        return message[2:message.find(':')] 

    def treatment_message(self):
        begin_time_generate = time.time()
        last_message_time = 0
        while True:
            #Get text message
            message = self.psbb_red.get_message()
            if message:
                data_mess = str(message['data'])
                processed_message = self.psbb_red.get_message()
                last_message_time = time.time()
                #Did generator send message? Did anyone answer?
                if self.genr_num in data_mess and not processed_message:
                    #Message have error
                    if random.randint(0, 100) <= 5:
                        self.conn_red.publish(self.name_channel, self.appl_num 
                            + ':APPLICATION:PROCESSED ' + split_message_info(data_mess) + ' ERROR')
                        dict_to_store = {'Message' : split_message_info(data_mess)[:-1]}
                        #Set error message in db
                        self.conn_red.hmset('Error:'+self.appl_num, dict_to_store)
                        time.sleep(0.2)
                        continue
                    else:
                        #Send processing message
                        self.conn_red.publish(self.name_channel, self.appl_num + 
                            ':APPLICATION:PROCESSED ' + split_message_info(data_mess))
                        # print('processed ' + data_mess.split(' ')[1])
                        time.sleep(0.2)
                        continue
                #If channel gets new generator save it identeficate
                if self.genr_num not in data_mess and ':GENERATE' in data_mess:
                        self.genr_num = get_name_new_genr(data_mess)         
                        continue
            #If generator dosen't send message > 1.1 sec appoint new generator
            if last_message_time and time.time() - last_message_time > 1.1:
                self.genr_num = self.appl_num.split(':')[0]
                self.work_generate()
            #Timeline working programm
            if time.time() - begin_time_generate > 10:
                break
            time.sleep(0.2)


    def generate_text(self):
        return ''.join([chr(x) for x in list(random.randint(33, 125) for x in range(20))])


def Processing_error():
    r = redis.StrictRedis(host='localhost', port=6379)
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