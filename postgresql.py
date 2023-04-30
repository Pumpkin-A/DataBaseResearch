import psycopg2
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
    def __init__(self, cursor): #конструктор класса
        self.cursor = cursor
    
    def create(self, cursor): #создание таблицы
        #SQL запрос создания таблицы postgresTable, если она еще не существует
        #с начальным заданным уникальным столбцом id
        cursor.execute("CREATE TABLE IF NOT EXISTS public.postgresTable (id int NOT NULL GENERATED ALWAYS AS IDENTITY)")
        intColumnsNames = ['a_int', 'b_int', 'c_int', 'd_int',
                            'e_int', 'f_int', 'g_int', 'h_int',
                            'i_int', 'j_int', 'k_int', 'l_int',
                            'm_int', 'n_int', 'o_int', 'p_int',
                            'q_int', 'r_int','s_int', 't_int']
        strColumnsNames = ['a_str', 'b_str', 'c_str', 'd_str',
                            'e_str', 'f_str', 'g_str', 'h_str',
                            'i_str', 'j_str', 'k_str', 'l_str',
                            'm_str', 'n_str', 'o_str', 'p_str',
                            'q_str', 'r_str','s_str', 't_str']
        #SQL запрос добавления столбцов 
        for i in range (len(strColumnsNames)):
            cursor.execute("ALTER TABLE  public.postgresTable ADD COLUMN IF NOT EXISTS  {name} varchar NULL".format(name = strColumnsNames[i]))
        for i in range (len(intColumnsNames)):
            cursor.execute("ALTER TABLE public.postgresTable ADD COLUMN IF NOT EXISTS {name} integer NULL".format(name = intColumnsNames[i]))
        
    def filling(self, cursor): #функция заполнения таблицы
        for i in range (100000):
            allValues = ()
            for i in range (20):
                allValues += (generate_random_string(random.randint(1, 10)),) #заполнение рандомными строковыми значениями
            for i in range (20):
                allValues += (random.randint(0, 500000),) #заполнение случайными целочисленными значениями
            # Делаем INSERT запрос к базе данных, используя обычный SQL-синтаксис
            cursor.execute("""insert into postgresTable (a_str, b_str, c_str, d_str,
                                e_str, f_str, g_str, h_str,
                                i_str, j_str, k_str, l_str,
                                m_str, n_str, o_str, p_str,
                                q_str, r_str,s_str, t_str, 
                                a_int, b_int, c_int, d_int,
                                e_int, f_int, g_int, h_int,
                                i_int, j_int, k_int, l_int,
                                m_int, n_int, o_int, p_int,
                                q_int, r_int,s_int, t_int) values (%s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s,
                                                                                %s,%s,%s,%s,%s)""", allValues)

    def doQuery(self, cursor): #функция запроса (операции чтения)
        # Делаем SELECT запрос к базе данных, используя обычный SQL-синтаксис
        cursor.execute("""
                    SELECT id, a_str, o_str, k_str, a_int, f_int, t_int
                    FROM postgresTable 
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
        results = cursor.fetchall() #получение результатов запроса
        return results

    
    def printResults(self, cursor): #вывод результатов запроса в консоль
        results = cursor.fetchall()
        for i in range (len(results)):
            print(results[i])

def withoutCachingOption(): 
    results = [] #результаты времени
    queryResalts = [] #в массив будут помещены результаты запросов
    for i in range(100):
        #соединение с базой данных
        with psycopg2.connect(dbname='postgres', user='postgres', 
                                password='asmrt122', host='localhost') as conn:
            #получение курсора 
            with conn.cursor() as cursor:
                dataBase = DataBase(cursor)
                # dataBase.create(cursor)
                # dataBase.filling(cursor)

                begin = time.perf_counter() #начало измерения времени
                queryResalts.append(dataBase.doQuery(cursor))
                end = time.perf_counter() #конец измерения времени
                results.append(end - begin) #запись результата
    # plt.plot(results)
    # plt.xlabel('Attempt number')
    # plt.ylabel('Seconds')
    # plt.show()  # построение графика с помощью библиотеки mathplotlib

    theWorstRes, theBestRes, averageRes = max(results), min(results), sum(results)/len(results)
    return (theWorstRes, theBestRes, averageRes)

def withCachingOption():
    queryResalts = [] #в массив будут помещены результаты запросов
    #соединение с базой данных
    with psycopg2.connect(dbname='postgres', user='postgres', 
                            password='asmrt122', host='localhost') as conn:
        #получение курсора 
        with conn.cursor() as cursor:
            dataBase = DataBase(cursor)
            # dataBase.create(cursor)
            # dataBase.filling(cursor)

            results = [] #результаты времени
            for i in range(100):
                begin = time.perf_counter() #начало измерения времени
                queryResalts.append(dataBase.doQuery(cursor))
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