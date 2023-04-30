import clickhouse_connect
import numpy
import random
import string
import time
import matplotlib.pyplot as plt

#функция генерации случайной строки для заполнения столбцов типа str
def generate_random_string(length):
    letters = string.ascii_lowercase
    rand_string = ''.join(random.choice(letters) for i in range(length))
    return rand_string

#класс с функционалом работы с базой данных
class DataBase:
    def __init__(self, client): #конструктор класса
        self.client = client

    def create(self, client): #создание таблицы
         #SQL запрос создания таблицы postgresTable, если она еще не существует
        #с начальным заданным уникальным столбцом id
        client.command("CREATE TABLE IF NOT EXISTS clickhouseTable (id Int32) Engine MergeTree ORDER BY tuple()")
        intColumnsNames = ['a_int', 'b_int', 'c_int', 'd_int',
                            'e_int', 'f_int', 'g_int', 'h_int',
                            'i_int', 'j_int', 'k_int', 'l_int',
                            'm_int', 'n_int', 'o_int', 'p_int',
                            'q_int', 'r_int','s_int', 't_int']
        #SQL запрос добавления столбцов 
        for i in range (len(intColumnsNames)):
            client.command("ALTER TABLE  `default`.clickhouseTable ADD COLUMN IF NOT EXISTS {name} Int32".format(name = intColumnsNames[i]))
        strColumnsNames = ['a_str', 'b_str', 'c_str', 'd_str',
                            'e_str', 'f_str', 'g_str', 'h_str',
                            'i_str', 'j_str', 'k_str', 'l_str',
                            'm_str', 'n_str', 'o_str', 'p_str',
                            'q_str', 'r_str','s_str', 't_str']
        for i in range (len(strColumnsNames)):
            client.command("ALTER TABLE  `default`.clickhouseTable ADD COLUMN IF NOT EXISTS {name} String(10)".format(name = strColumnsNames[i]))


    def filling(self, client): #функция заполнения таблицы
        allRows = []
        for i in range (1, 100001):
            row = []
            row.append(i) #id
            for i in range (20):
                row.append(random.randint(0, 500000),) #заполнение случайными целочисленными значениями
            for i in range (20):
                row.append(generate_random_string(random.randint(1, 10)),) #заполнение рандомными строковыми значениями
            allRows.append(row)
        # Делаем INSERT запрос к базе данных, используя обычный SQL-синтаксис
        client.insert('clickhouseTable', allRows, column_names='*')
        

    def doQuery(self, client): #функция запроса (операции чтения)
        # Делаем SELECT запрос к базе данных, используя обычный SQL-синтаксис
        #получение результатов запроса
        result = client.query("""
                    SELECT id, a_str, o_str, k_str, a_int, f_int, t_int
                    FROM clickhouseTable 
                    WHERE (((a_int > 5000) and (b_int % 10 = 5) and (f_int/k_int > 135)
                        and (t_int % 100 > 2) and (i_int + j_int + n_int + h_int < 40000000)) or
                        ((a_str not in ('qwert', 'asd', 'aaaaa')) and (o_str in ('ffff', 'f', 'fff', 'ff', 'fffff'))
                        and (e_str not in ('poi', 'fdgdgdf', 'fff'))) or
                        ((k_str in ('aa')) and (j_str not in ('a', 'f', 'k'))))
                        and ((id > 30) or (id < 80000) or (id % 10 = 9) or (id = 17) or (id % 2 = 8))
                    ORDER BY a_int, id
                    LIMIT 40
                    """
                    )
        return result
    
    def printResults(self, client): #вывод результатов запроса в консоль
        results = self.doQuery(client)
        print(results.result_rows)

def withoutCachingOption():
    queryResalts = []  #в массив будут помещены результаты запросов
    results = [] #результаты времени
    for i in range(100):
        client = clickhouse_connect.get_client(host='localhost', port=18123, username='default', password='')
        dataBase = DataBase(client)
        # dataBase.create(client)
        # dataBase.filling(client)
        dataBase.doQuery(client)
        begin = time.perf_counter() #начало измерения времени
        queryResalts.append(dataBase.doQuery(client))
        end = time.perf_counter() #конец измерения времени
        results.append(end - begin)  #запись результата
    # plt.plot(results)
    # plt.xlabel('Attempt number')
    # plt.ylabel('Seconds') 
    # plt.show() # построение графика с помощью библиотеки mathplotlib
    theWorstRes, theBestRes, averageRes = max(results), min(results), sum(results)/len(results)
    return (theWorstRes, theBestRes, averageRes)


def withCachingOption():
    queryResalts = [] #в массив будут помещены результаты запросов
    client = clickhouse_connect.get_client(host='localhost', port=18123, username='default', password='')
    dataBase = DataBase(client)
    # dataBase.create(client)
    # dataBase.filling(client)
    dataBase.doQuery(client)
    results = [] #результаты времени
    for i in range(100):
        begin = time.perf_counter()  #начало измерения времени
        queryResalts.append(dataBase.doQuery(client))
        end = time.perf_counter() #конец измерения времени
        results.append(end - begin) #запись результата
    theWorstRes, theBestRes, averageRes = max(results), min(results), sum(results)/len(results)
    return(theWorstRes, theBestRes, averageRes)

if __name__ == "__main__":
    theWorstCachingRes, theBestCachingRes, averageCachingRes = withCachingOption()
    theWorstNotCachingRes, theBestNotCachingRes, averageNotCachingRes = withoutCachingOption()
    print("""\tResults with caching:
            the worst is {theWorstCachingRes},
            the best is {theBestCachingRes},
            average is {averageCachingRes}
        Results without caching:
            the worst is {theWorstNotCachingRes}, 
            the best is {theBestNotCachingRes}, 
            average is {averageNotCachingRes}""".format(theWorstCachingRes=theWorstCachingRes,
                                                        theBestCachingRes=theBestCachingRes,
                                                        averageCachingRes=averageCachingRes,
                                                        theWorstNotCachingRes=theWorstNotCachingRes,
                                                        theBestNotCachingRes=theBestNotCachingRes,
                                                        averageNotCachingRes=averageNotCachingRes))

