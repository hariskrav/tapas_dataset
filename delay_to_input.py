import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import glob
from datetime import datetime
from pathlib import Path
import argparse
import copy
import numpy as np


def read_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-d', '--day', default=False)
    parser.add_argument('-m', '--max_delay', default=100)
    args = parser.parse_args()
    return args

def clear(string, day, month):
    result = string.split(", ")
    sectors = []
    times = []

    for i in range(len(result)):
        temp = result[i].split(":")
        dt = datetime.utcfromtimestamp(int(temp[-2]))
        if day != dt.day or month != dt.month:
            continue
        if i == len(result)-1:
            sectors.append(temp[0])
            times.append(int(temp[-2]))
        elif temp[0] != 'SAME':
            sectors.append(temp[0])
            times.append(int(temp[-2]))

    # print(sectors)
    # print(times)
    if not sectors:
        return [], []
    if all(flag == 'NONE' for flag in sectors):
        return [], []

    if 'NONE' in sectors[0]:
        del sectors[0]
        del times[0]
    if 'SAME' in sectors[-1] or 'NONE' in sectors[-1]:
        del sectors[-1]
    else:
        times.append(times[-1]+60)

    if 'NONE' in sectors[-1]:
        del sectors[-1]
        del times[-1]
    # print(sectors)
    # print(times)
    # print()

    times2 = []
    for time in times:
        temp = datetime.utcfromtimestamp(time)
        # if temp.second > 29:
        #     times2.append(temp.minute + temp.hour * 60 + 1)
        # else:
        times2.append(temp.minute + temp.hour * 60)

    durations = [times2[0]]
    for i in range(1, len(times2)):
        durations.append(max(times2[i]-times2[i-1],1))

    if 'NONE' in sectors[-1]:
        durations[-1] = 1

    if len(sectors) != len(durations) -1:
        print('Wrong Length')
        exit()

    if 'NONE' in sectors[0]:
        print('NONE First')
        exit()

    for i in range(len(sectors)-1):
        if sectors[i] == sectors[i+1]:
            print('Same Sectors')
            print(sectors)
            exit()

    return sectors, durations

def main():
    args = read_args()
    day = args.day
    max_delay = int(args.max_delay)
    print(day, max_delay)

    month = day[4:6]
    if month[0] == '0':
        month = month[1]
    month = int(month)

    day2 = day[6:8]
    if day2[0] == '0':
        day2 = day2[1]
    day2 = int(day2)

    Path(day+"/0_capping").mkdir(parents=True, exist_ok=True)
    Path(day+"/0_capping_delays").mkdir(parents=True, exist_ok=True)
    Path(day+"/0_delays").mkdir(parents=True, exist_ok=True)

    Path('dataset/scenario_'+day).mkdir(parents=True, exist_ok=True)
    Path('dataset/scenario_' + day + '_only_capping').mkdir(parents=True, exist_ok=True)
    Path('dataset/scenario_' + day + '_capping_delays').mkdir(parents=True, exist_ok=True)

    Path("results/" + day + "/0_delays").mkdir(parents=True, exist_ok=True)
    Path("results/" + day + "/1_only_capping").mkdir(parents=True, exist_ok=True)
    Path("results/" + day + "/2_capping_delays").mkdir(parents=True, exist_ok=True)

    Path("delays/experiments/scenario_" + day).mkdir(parents=True, exist_ok=True)
    Path("level_capping/experiments/scenario_" + day + "_only_capping").mkdir(parents=True, exist_ok=True)
    Path("capping_delays/experiments/scenario_" + day + "_capping_delays").mkdir(parents=True, exist_ok=True)

    Path("delays/models").mkdir(parents=True, exist_ok=True)
    Path("level_capping/models").mkdir(parents=True, exist_ok=True)
    Path("capping_delays/models").mkdir(parents=True, exist_ok=True)

    flights = '*' #LPPT-LSGG-EJU67YT-20190801161000

    all_files = glob.glob(day+'/'+flights+'/delay')
    all_files.sort()
    li = []
    for filename in all_files:
        df_temp = pd.read_csv(filename, sep='\t', dtype={'FP-Key': str,"Delay(minutes)": int,'Trajectory': str})
        li.append(df_temp)
    df = pd.concat(li, axis=0, ignore_index=True)
    df['Trajectory'] = df.Trajectory.str.replace(r"{-}", 'SAME')
    df['Trajectory'] = df.Trajectory.str.replace(r"{}", 'NONE')
    df['Trajectory'] = df.Trajectory.str.replace(r"{", '')
    df['Trajectory'] = df.Trajectory.str.replace(r"}", '')
    df['Trajectory'] = df.Trajectory.str.replace(r"[", '')
    df['Trajectory'] = df.Trajectory.str.replace(r"]", '')

    df_flights = pd.DataFrame({'FP-Key': df['FP-Key'].unique()[:]})
    print('Traffic', df_flights['FP-Key'].nunique())
    df_flights.to_csv(day+'/0_delays/flights.csv', index=False)

    sector_list = []
    duration_list = []
    takeoffs = []
    max_plan = 0
    wrong = []
    for index, row in df.iterrows():
        sectors, durations = clear(row['Trajectory'], day2, month)
        if not sectors:
            sectors = ['NONE']
            durations = [1440, 1]
            if row['FP-Key'] not in wrong:
                wrong.append(row['FP-Key'])
        str1 = ' '.join(sectors)
        sector_list.append(str1)
        takeoff = durations.pop(0)
        takeoffs.append(takeoff)
        str2 = ' '.join(str(x) for x in durations)
        duration_list.append(str2)
        if len(sectors) > max_plan:
            max_plan = len(sectors)

    print('Max Plan', max_plan)
    print('Empty plans',len(wrong), wrong)
    df['Takeoffs'] = takeoffs
    df['Sectors'] = sector_list
    df['Durations'] = duration_list
    del df['Trajectory']

    df.sort_values(by=['FP-Key', 'Delay(minutes)'], inplace=True, ascending=True)
    sectors = []
    durations = []
    takeoffs = []
    ids = []
    delays =[]
    for index, row in df.iterrows():

        id = row['FP-Key']
        delay = int(row['Delay(minutes)'])
        takeoff = int(row['Takeoffs'])
        plan = row['Sectors']
        durs = row['Durations']

        if index == len(df) - 1 or row['FP-Key'] != df.at[index + 1, 'FP-Key']:
            for i in range(delay+1, max_delay+1):
                ids.append(id)
                delays.append(i)
                takeoffs.append(takeoff+i-delay)
                sectors.append(plan)
                durations.append(durs)
        elif index < len(df) - 1 and row['FP-Key'] == df['FP-Key'][index + 1]:
            for i in range(delay+1, int(df['Delay(minutes)'][index + 1])):
                ids.append(id)
                delays.append(i)
                takeoffs.append(takeoff+i-delay)
                sectors.append(plan)
                durations.append(durs)

    df2 = pd.DataFrame({'FP-Key': ids, 'Delay(minutes)': delays,
                         'Takeoffs': takeoffs, 'Sectors': sectors,
                         'Durations': durations})
    df.to_csv(day+'/0_delays/temp.csv', index=False)
    df = pd.concat([df, df2])
    df.sort_values(by=['FP-Key', 'Delay(minutes)'], inplace=True, ascending=True)
    df = df.reset_index(drop=True)

    df.to_csv(day+'/0_delays/temp2.csv', index=False)

    params = ['traffic', 'entry_period_length', 'entry_step', 'max_delay', 'number_of_periods']
    values = [df['FP-Key'].nunique(), 60, 20, max_delay, 24]
    df_params = pd.DataFrame({'hyper_parameter': params, 'value': values})

    df_params.to_csv(day+'/0_delays/scenario_'+day+'_parameters.csv', index=False)
    df_params.to_csv('dataset/scenario_'+day+'/scenario_'+day+'_parameters.csv', index=False)

    df_caps = pd.read_csv(day+'/capacities.csv')
    none_row = {'sector': 'NONE', 'capacity': 1000}
    df_caps = df_caps.append(none_row, ignore_index=True)
    print('Number of Sectors', df_caps['sector'].nunique())

    df_caps.to_csv(day+'/0_delays/scenario_'+day+'_capacities.csv', index=False)
    df_caps.to_csv('dataset/scenario_'+day+'/scenario_'+day+'_capacities.csv', index=False)

    # print(pd.concat(g for _, g in df_caps.groupby("sector") if len(g) > 1)) #check for duplicate sectors
    df_caps['SectorID'] = df_caps.index +1
    dict_caps = df_caps.set_index('sector').T.to_dict('list')

    sector_ids =[]
    capacities = []
    missing = []
    for index, row in df.iterrows():
        temp = []
        temp_cap = []
        sectors = row['Sectors'].split(" ")
        for sector in sectors:
            result = dict_caps.get(sector, [-1, -1])[1]
            if result == -1:
                missing.append(sector)
                result = dict_caps.get('NONE', [-1, -1])[1]
                temp.append(result)
                temp_cap.append('999')
            else:
                temp.append(result)
                temp_cap.append(dict_caps.get(sector, [-1, -1])[0])
        sector_ids.append(' '.join(str(x) for x in temp))
        capacities.append(' '.join(str(x) for x in temp_cap))

    df['SectorIDs'] = sector_ids
    df['Capacities'] = capacities

    df_missing = pd.DataFrame({'sector': missing}).drop_duplicates()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print("MISSING SECTORS")
        print(df_missing)
    df_inf = df_caps[df_caps.capacity > 999]
    # print(df_inf)
    del df_inf['SectorID']
    del df_inf['capacity']
    df_inf = pd.concat([df_inf, df_missing])
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #     print(df_inf)
    df_inf.sort_values(by=['sector'], inplace=True, ascending=True)
    # print(df_inf)
    df_inf.to_csv(day+'/0_delays/infinite.csv', index=False)

    df.to_csv(day+'/0_delays/temp3.csv', index=False)#, columns=['FP-Key','Delay(minutes)','Sectors','Capacities'])

    file = open(day+'/0_delays/scenario_'+day+'.csv', 'w')
    curr_id = ''
    count = 0

    for index, row in df.iterrows():
        id = row['FP-Key']
        delay = int(row['Delay(minutes)'])
        takeoff = int(row['Takeoffs'])
        plan = row['SectorIDs']
        durs = row['Durations']

        if curr_id != id:
            file.write(str(count)+',')
            curr_id = id
            count +=1

        if index == len(df) - 1 or row['FP-Key'] != df.at[index + 1, 'FP-Key']:
            to_write = 'd'+str(delay)+','+str(takeoff)+','+str(plan)+','+str(durs)+','
            file.write(to_write)
            to_write = 'model;'+str(id)+ ';' + 'NULL' +',true,'+str(max_delay)+'\n'
            file.write(to_write)
        elif index < len(df) - 1 and row['FP-Key'] == df['FP-Key'][index + 1]:
            to_write = 'd'+str(delay)+','+str(takeoff)+','+str(plan)+','+str(durs)+','
            file.write(to_write)
    file.close()

    flights_df = pd.read_csv(day+'/0_delays/scenario_'+day+'.csv', header=None, sep=",", dtype=str)
    flights_df.to_csv('dataset/scenario_'+day+'/scenario_'+day+'.csv', index=False, header=False)

    # df_temp = pd.read_csv(day+'/0_delays/scenario_'+day+'.csv', usecols =[0, 1+(max_delay+1)*4], names=["NumericID", "Temp"])
    # ids=[]
    # for index, row in df_temp.iterrows():
    #     ids.append(row['Temp'].split(";")[1])
    # df_temp['FlightID'] = ids
    # del df_temp['Temp']
    # df_temp.to_csv(day + '/0_delays/flight_id_map.csv', index=False)
    # print(df_temp)
    #
    #
    #
    # file_name = day + '/0_delays/temp3.csv'
    # df_temp3 = pd.read_csv(file_name, sep=",", dtype=str)
    #
    # to_write = day + '/0_delays/sector_id_map.csv'
    # file = open(to_write, 'w')
    # header = ''
    # for i in range(max_length):
    #     header += ",Sector_" + str(i)
    # file.write('FlightID,Delays'+header+'\n')
    #
    # for index, row in df_temp3.iterrows():
    #     flight = row['FP-Key']
    #     delay = row['Delay(minutes)']
    #     plan = row['Sectors']
    #
    #     file.write(flight + ',' +delay)
    #     sectors = plan.split(" ")
    #     for sector in sectors:
    #         file.write(',' + sector)
    #     for i in range(len(sectors), max_length):
    #         file.write(',NULL')
    #     file.write('\n')
    # file.close()

if __name__ == '__main__':
    main()