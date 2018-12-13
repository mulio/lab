import pandas as pd
import os
import csv
from datetime import datetime
import sys
import random
import gzip
from collections import defaultdict
import cPickle as pickle


def get_unique_card_LYNX_ES(data_path, file_name_unique):
    '''
    Inputs
    ----------
    data_path: ruta con un csv que contiene transacciones
    file_name_unique: nombre de dicho csv
    
    Output
    ----------
    Counjunto con las tajetas incluidas en el csv de transacciones
    '''
    # DataFrame con las transacciones cargadas
    print 'Conjunto tarjetas unicas', file_name_unique
    print 'path', os.path.join(data_path, file_name_unique)
    df_trans = pd.read_csv(os.path.join(data_path, file_name_unique), 
                        quoting=3,  
                        error_bad_lines=False,
                        nrows=None,sep='|', 
                        header= None, 
                        compression = 'gzip')
    
    # El identificador de las tarjetas es la union de las columnas 4 y 5
    card = df_trans[4].map(str) + df_trans[5]
    # Eliminamos duplicados al devolver un conjunto
    set_card = set(card)
    return set_card


def select_LYNX_ES_card(data_path, file_name_unique):
    '''
    Inputs
    ----------
    data_path: ruta con un csv que contiene transacciones
    file_name_unique: nombre de dicho csv
    num_card: numnero de tarjetas que se van a seleccionar,
        debe ser inferior que el numero de tarjetas en el
        archivo `file_name_unique`
    
    Output
    ----------
    Counjunto con `num_card` tarjetas unicas, extraidas del archivo
        `file_name_unique`
    '''
    unique_card = get_unique_card_LYNX_ES(data_path, file_name_unique)
    #unique_card_sample = unique_card[:num_card]
    unique_card_sample = unique_card
    print len(unique_card_sample)
    return unique_card_sample
    
    
def parseo_LYNX_ES_card(data_path, output_file):
    '''
    data_path: ruta con los datos
    output_file: ruta de los datos de salida
    unique_card_sample: Set con las tarjetas unicas
    '''
    # csv.field_size_limit(sys.maxsize)
    # conjunto con el id de las transacciones para evitar transacciones repetidas
    set_ids = set()
    name = data_path + 'LYNX_Eeeee_90DAY_TRANS_2018'

    # mejorar para anios bisiestos
    days_per_month = {'01': 31, '02': 28, '03': 31, '04': 30, '05': 31, '06': 30, 
                      '07': 31, '08': 31, '09': 30, '10': 31, '11': 30, '12': 31}
 
    file = open(output_file, "w")
    file.write('transactionid,message_code,card,process_code,amount,date,mcc,' +
               'country,input_mode_0,input_mode_1,input_mode_2,input_mode_3,' +
               'input_mode_4,input_mode_5,input_mode_6,input_mode_7,' +
               'input_mode_8,input_mode_9,input_mode_10,input_mode_11,' +
               'acquirerid,authorizerid,tradeid,orig_amount,' +
               'currency,tvr_0,tvr_1,tvr_2,tvr_3,tvr_4,' +
               'cvr_0,cvr_1,cvr_2,cvr_3,cvr_4,label,SCORE_N' + '\n')
    
    
    for month in ['02', '03']:
        days = days_per_month[month]

        for day in range(1, days+1):
            strday = str(day).zfill(2)

            log = name + str(month) + strday + '.txt.gz'
            print 'log parseado',log

            try:
                log_file = gzip.open(log, 'r')
            except:
                continue
            
            for i, line in enumerate(log_file, 1):
                if i%100000 == 0:
                    print 'Linea {:,} de {}'.format(i, log)
                row = line.split('|')

                if len(row) in [106, 108, 113]:

                    cont_hora = str(row[3]).zfill(6)

                    fecha_original= row[2] + cont_hora 
                    tvr, cvr = row[51], row[52]

                    good_datetime = True
                    try:
                        datetime(int(fecha_original[:4]), int(fecha_original[4:6]), int(fecha_original[6:8]))
                    except:
                        print 'bad datetime'
                        good_datetime = False
                        
                    id_in_set = row[66] in set_ids
                    if id_in_set or not good_datetime:
                        if id_in_set:
                            print 'id repetido'
                    else:
                        # filtramos para dejar solo los cargos a la tarjeta
                        if int(row[28]) < 200000:
                            set_ids.add(row[66])

                            file.write(row[66]+","+
                                       row[27]+","+
                                       row[4]+row[5]+","+
                                       row[28]+","+
                                       row[8]+","+
                                       fecha_original+","+
                                       row[30]+","+
                                       row[11]+","+
                                       row[31]+","+
                                       row[32]+","+
                                       row[33]+","+
                                       row[34]+","+
                                       row[35]+","+
                                       row[36]+","+
                                       row[37]+","+
                                       row[38]+","+
                                       row[39]+","+
                                       row[40]+","+
                                       row[41]+","+
                                       row[42]+","+
                                       row[10]+","+
                                       row[44]+","+
                                       row[13]+","+
                                       row[50]+","+
                                       row[62]+","+
                                       tvr[:2]+","+
                                       tvr[2:4]+","+
                                       tvr[4:6]+","+
                                       tvr[6:8]+","+
                                       tvr[8:]+","+
                                       cvr[:2]+","+
                                       cvr[2:4]+","+
                                       cvr[4:6]+","+
                                       cvr[6:8]+","+
                                       cvr[8:]+","+
                                       row[26]+","+
                                       row[21]+"\n")

                else:
                    print len(row)

    file.close()

if __name__ == '__main__':
    data_path = '/usr/local/pr/dfs_mit/LYNX_Eeeee_90DAY/'
    parseo_LYNX_ES_card(data_path, '/home/system/MIT/datos_parseados/agrupacion_febrero_marzo.csv')