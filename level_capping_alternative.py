import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
import glob
from datetime import datetime
from itertools import chain, combinations
import argparse
import copy
import numpy as np


def read_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-d', '--day', default=False)
    parser.add_argument('-m', '--max_delay', default=100)
    args = parser.parse_args()
    return args

def powerset(string):
    ps = []
    for i in range(0,len(string)+1):
        for element in combinations(string,i):
            if not element:
                continue
            ps.append(element)
    return ps

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


    flights = '*' #LEBL-SCEL-IBE2605-20190801090500
    keep_top_k = 50

    all_files = glob.glob(day+'/'+flights+'/ranking.txt')
    all_files.sort()
    li = []
    for filename in all_files:
        trajectory_id = filename.split("/")[1]
        id_list = []
        total_ranking = []
        df_temp = pd.read_csv(filename, sep='\t', dtype=str, names=['Delay','RegulatedSectors','Ranking'])
        for index, row in df_temp.iterrows():
            id_list.append(trajectory_id)
            ranking = row['Ranking'].split(", ")[-1].split(":")[1][:-1]
            total_ranking.append(ranking)
        df_temp['FP-Key'] = id_list
        df_temp['TotalRanking'] = total_ranking
        li.append(df_temp)
    df_ranking = pd.concat(li, axis=0, ignore_index=True)

    df_ranking['RegulatedSectors'] = df_ranking.RegulatedSectors.str.replace(r"[", '')
    df_ranking['RegulatedSectors'] = df_ranking.RegulatedSectors.str.replace(r"]", '')
    df_ranking['Ranking'] = df_ranking.Ranking.str.replace(r"[", '')
    df_ranking['Ranking'] = df_ranking.Ranking.str.replace(r"]", '')
    df_ranking = df_ranking.astype({'Delay': 'int'})
    df_ranking = df_ranking.astype({'TotalRanking': 'float64'})
    df_ranking = df_ranking[0 == df_ranking.Delay]
    df_ranking = df_ranking['' != df_ranking.RegulatedSectors]
    del df_ranking['Ranking']
    df_ranking.rename(columns={"Delay": "Delay(minutes)"}, inplace=True)
    df_ranking = df_ranking[['FP-Key', 'RegulatedSectors', 'TotalRanking', 'Delay(minutes)']]
    del df_ranking['Delay(minutes)']
    df_ranking.sort_values(by=['FP-Key', 'TotalRanking'], inplace=True, ascending=True)
    df_ranking = df_ranking.reset_index(drop=True)
    # print(df_ranking)
    print('Flights that can level cap', df_ranking['FP-Key'].nunique())

    df_hotspots = pd.read_csv(day + '/0_capping/scenario_' + day + '_exp0_baseline_hotspots_flights.csv', usecols=['SectorID', 'FlightID'])
    df_hotspots = df_hotspots.drop_duplicates()
    df_hotspots.rename(columns={"FlightID": "FP-Key"}, inplace=True)
    df_hotspots.rename(columns={"SectorID": "Hotspot"}, inplace=True)
    df_hotspots = df_hotspots[['FP-Key', 'Hotspot']]
    df_hotspots.sort_values(by=['FP-Key', 'Hotspot'], inplace=True, ascending=True)
    df_hotspots = df_hotspots.reset_index(drop=True)
    # print(df_hotspots)
    print('Flights in hotspots', df_hotspots['FP-Key'].nunique())

    common = set.intersection(set(df_ranking['FP-Key']), set(df_hotspots['FP-Key']))
    print('Flights that can level cap and have hotspots', len(common))

    df_ranking = df_ranking[df_ranking['FP-Key'].isin(common)]
    df_ranking = df_ranking.reset_index(drop=True)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    # print(df_ranking)

    df_hotspots = df_hotspots[df_hotspots['FP-Key'].isin(common)]
    df_hotspots = df_hotspots.reset_index(drop=True)

    cappable_sectors = set()
    for index, row in df_ranking.iterrows():
        sectors = row['RegulatedSectors'].split(", ")
        for sector in sectors:
            cappable_sectors.add(sector)

    common_sectors = set.intersection(cappable_sectors, set(df_hotspots['Hotspot']))
    df_temp = df_hotspots[~df_hotspots['Hotspot'].isin(common_sectors)]
    print('Uncappable Sectors', df_temp['Hotspot'].unique())
    df_hotspots = df_hotspots[df_hotspots['Hotspot'].isin(common_sectors)]
    df_hotspots = df_hotspots.reset_index(drop=True)
    print('Number of cappable sectors', len(cappable_sectors))
    print('Cappable sectors', cappable_sectors)
    print('Flights with cappable hotspots', df_hotspots['FP-Key'].nunique())
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df_hotspots['FP-Key'].value_counts())
        print(df_hotspots)


    flight_ids_level_capping = []
    regulated_sectors = []
    ranking = []
    hotspots = []
    grouped = df_hotspots.groupby(['FP-Key'])
    for name, temp_df in grouped:
        sectors = temp_df['Hotspot'].unique()
        # if len(sectors) > 1:
        #     print(name)
        #     print('MORE SECTORS')
        #     exit()
        power_sectors = powerset(sectors)
        temp_df_ranking = df_ranking[name == df_ranking['FP-Key']].copy()
        temp_df_ranking.sort_values(by=['TotalRanking'], inplace=True, ascending=True)
        temp_df_ranking = temp_df_ranking.head(keep_top_k)
        li = []
        for sector in sectors:
            temp_temp = temp_df_ranking[temp_df_ranking['RegulatedSectors'].str.contains(sector)]
            li.append(temp_temp)
        temp_df_ranking = pd.concat(li, axis=0, ignore_index=True)
        temp_df_ranking = temp_df_ranking.drop_duplicates()

        for sectors in reversed(power_sectors):
            possible = temp_df_ranking.copy()
            for sector in sectors:
                possible = possible[possible['RegulatedSectors'].str.contains(sector)]
            possible = possible.drop_duplicates()
            best_score = possible['TotalRanking'].min()
            possible = possible[best_score == possible.TotalRanking]
            indices = possible['RegulatedSectors'].str.len().sort_values().index
            possible = possible.reindex(indices)
            if not possible.empty:
                flight_ids_level_capping.append(possible.iloc[0,0])
                sectors_string = possible.iloc[0,1].replace(', ', ';')
                regulated_sectors.append(sectors_string)
                ranking.append(possible.iloc[0,2])
                hotspots.append(";".join(sectors))
                break
            else:
                print('empty possible')

    df_level_capping_final = pd.DataFrame({'FP-Key': flight_ids_level_capping,
                                           'RegulatedSectors': regulated_sectors, 'Ranking': ranking,
                                           'Hotspot': hotspots})
    print(df_level_capping_final)

    print('Find replaced_by')
    all_files = glob.glob(day+'/*/sectors.xai')
    all_files.sort()
    replaced_by = []
    for filename in all_files:
        new_sectors = []
        trajectory_id = filename.split("/")[1]
        if trajectory_id not in flight_ids_level_capping:
            continue
        index = flight_ids_level_capping.index(trajectory_id)
        sectors = '[' + regulated_sectors[index].replace(';', ', ') + ']'
        with open(filename) as f:
            content = f.readlines()
        f.close()
        content = content[2:]
        for line in content:
            split_line = line.split("\t")
            if int(split_line[0]) != 0:
                break
            if split_line[1] != sectors:
                continue
            for element in split_line[2:]:
                element = element.replace("[", "")
                element = element.replace("]", "")
                element = element.replace("\n", "")
                split_element = element.split('->')
                for inner in split_element[1:]:
                    split_inner = inner.split(', ')
                    for sector in split_inner:
                        if sector not in new_sectors and sector != '':
                            new_sectors.append(sector)
        if not new_sectors:
            new_sectors.append('NULL')
        replaced_by.append(";".join(new_sectors))

    df_level_capping_final['ReplacedBy'] = replaced_by
    df_level_capping_final.to_csv(day + '/0_capping/scenario_' + day + '_level_capping.csv', index=False)

    #*************************************
    all_files = glob.glob(day+'/'+flights)
    all_files.sort()
    li = []
    for filename in all_files:
        flight_id = filename.split("/")[1]
        if '0' == flight_id[0] or '.7z' in flight_id or 'capacities' in flight_id:
            continue
        if flight_id not in flight_ids_level_capping:
            filename += '/delay'
            df_temp = pd.read_csv(filename, sep='\t', dtype={'FP-Key': str,"Delay(minutes)": int,'Trajectory': str})
            li.append(df_temp)
        else:
            print(filename)
            filename += '/levelCapping.txt'
            df_temp = pd.read_csv(filename, sep='\t', dtype={'RegulatedSectors': str, "Delay(minutes)": int, 'Trajectory': str})
            index = flight_ids_level_capping.index(flight_id)
            sectors = '[' + regulated_sectors[index].replace(';', ', ') + ']'
            df_temp = df_temp[sectors == df_temp.RegulatedSectors]
            del df_temp['RegulatedSectors']
            id_list = []
            for i in range(len(df_temp)):
                id_list.append(flight_id)
            df_temp['FP-Key'] = id_list
            df_temp = df_temp[['FP-Key', 'Delay(minutes)', 'Trajectory']]
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
    df_flights.to_csv(day+'/0_capping/flights.csv', index=False)

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
    df.to_csv(day+'/0_capping/temp.csv', index=False)
    df = pd.concat([df, df2])
    df.sort_values(by=['FP-Key', 'Delay(minutes)'], inplace=True, ascending=True)
    df = df.reset_index(drop=True)

    df.to_csv(day+'/0_capping/temp2.csv', index=False)

    params = ['traffic', 'entry_period_length', 'entry_step', 'max_delay', 'number_of_periods']
    values = [df['FP-Key'].nunique(), 60, 20, max_delay, 24]
    df_params = pd.DataFrame({'hyper_parameter': params, 'value': values})

    df_params.to_csv(day+'/0_capping/scenario_'+day+'_parameters.csv', index=False)
    df_params.to_csv('dataset/scenario_' + day + '_only_capping/'
                                               'scenario_' + day + '_only_capping_parameters.csv', index=False)

    df_caps = pd.read_csv(day+'/capacities.csv')
    none_row = {'sector': 'NONE', 'capacity': 1000}
    df_caps = df_caps.append(none_row, ignore_index=True)
    print('Number of Sectors', df_caps['sector'].nunique())

    df_caps.to_csv(day + '/0_capping/scenario_' + day + '_only_capping_capacities.csv', index=False)
    df_caps.to_csv('dataset/scenario_' + day + '_only_capping/'
                                               'scenario_' + day + '_only_capping_capacities.csv', index=False)

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
    del df_inf['SectorID']
    del df_inf['capacity']
    df_inf = pd.concat([df_inf, df_missing])
    df_inf.sort_values(by=['sector'], inplace=True, ascending=True)
    # print(df_inf)
    df_inf.to_csv(day+'/0_capping/infinite.csv', index=False)

    df.to_csv(day+'/0_capping/temp3.csv', index=False)#, columns=['FP-Key','Delay(minutes)','Sectors','Capacities'])

    # file = open(day+'/0_capping/scenario_'+day+'.csv', 'w')
    # curr_id = ''
    # count = 0
    #
    # for index, row in df.iterrows():
    #     id = row['FP-Key']
    #     delay = int(row['Delay(minutes)'])
    #     takeoff = int(row['Takeoffs'])
    #     plan = row['SectorIDs']
    #     durs = row['Durations']
    #
    #     if curr_id != id:
    #         file.write(str(count)+',')
    #         curr_id = id
    #         count +=1
    #
    #     if index == len(df) - 1 or row['FP-Key'] != df.at[index + 1, 'FP-Key']:
    #         to_write = 'd'+str(delay)+','+str(takeoff)+','+str(plan)+','+str(durs)+','
    #         file.write(to_write)
    #         to_write = 'model;'+str(id)+',true,'+str(max_delay)+'\n'                                                    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #         file.write(to_write)
    #     elif index < len(df) - 1 and row['FP-Key'] == df['FP-Key'][index + 1]:
    #         to_write = 'd'+str(delay)+','+str(takeoff)+','+str(plan)+','+str(durs)+','
    #         file.write(to_write)
    # file.close()

if __name__ == '__main__':
    main()