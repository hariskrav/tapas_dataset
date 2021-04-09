import pandas as pd
import argparse
from shutil import copyfile


def read_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-d', '--day', default=False)
    args = parser.parse_args()
    return args

def main():
    args = read_args()
    day = args.day
    print(day)

    df_original = pd.read_csv(day + '/0_delays/temp3.csv')
    df_original = df_original[df_original['Delay(minutes)'] == 0]
    del df_original['Delay(minutes)']
    df_original['Capped'] = 0
    df_original['RegulatedSectors'] = 'NULL'
    df_original['Ranking'] = 'NULL'
    df_original['Hotspot'] = 'NULL'
    df_original['ReplacedBy'] = 'NULL'
    print(df_original)

    df_capped = pd.read_csv(day + '/0_capping/temp3.csv')
    df_capped = df_capped[df_capped['Delay(minutes)'] == 0]
    del df_capped['Delay(minutes)']
    df_capped['Capped'] = 0
    print(df_capped)

    cappable_ids = pd.read_csv(day +'/0_capping/scenario_' + day + '_level_capping.csv',
                              usecols=['FP-Key']).values.flatten()
    df_capped = df_capped[df_capped['FP-Key'].isin(cappable_ids)]
    df_capped['Capped'] = 1
    print(df_capped)

    df_temp = pd.read_csv(day + '/0_capping/scenario_' + day + '_level_capping.csv').fillna('NULL')
    df_capped = pd.merge(df_capped, df_temp, on='FP-Key')
    print(df_capped)


    df = pd.concat([df_capped, df_original])
    df.sort_values(by=['FP-Key', 'Capped'], inplace=True, ascending=True)
    df = df.reset_index(drop=True)
    print(df)
    df.to_csv(day + '/0_capping/temp3_with_capped.csv', index=False)


    file = open(day + '/0_capping/scenario_' + day + '_only_capping.csv', 'w')
    curr_id = ''
    count = 0

    for index, row in df.iterrows():
        id = row['FP-Key']
        capped = int(row['Capped'])
        takeoff = int(row['Takeoffs'])
        plan = row['SectorIDs']
        durs = row['Durations']
        regulated_sectors = row['RegulatedSectors'].replace(r";", ' ')
        hotspot = row['Hotspot'].replace(r";", ' ')
        replaced_by = row['ReplacedBy'].replace(r";", ' ')
        ranking = row['Ranking']

        if curr_id != id:
            file.write(str(count) + ',')
            curr_id = id
            count += 1

        if index == len(df) - 1 or row['FP-Key'] != df.at[index + 1, 'FP-Key']:
            to_write = 'lc' + str(capped) + ',' + str(takeoff) + ',' + str(plan) + ',' + str(durs) + ','
            file.write(to_write)
            to_write = 'model;' + str(id) + ';' + regulated_sectors +  ';' + str(ranking) + ';' + hotspot + ';' + replaced_by +\
                       ',' + str(capped) + '\n'
            file.write(to_write)
        elif index < len(df) - 1 and row['FP-Key'] == df['FP-Key'][index + 1]:
            to_write = 'lc' + str(capped) + ',' + str(takeoff) + ',' + str(plan) + ',' + str(durs) + ','
            file.write(to_write)
    file.close()

    copyfile(day + '/0_capping/scenario_' + day + '_only_capping.csv',
             'dataset/scenario_' + day + '_only_capping/scenario_' + day + '_only_capping.csv')

if __name__ == '__main__':
    main()