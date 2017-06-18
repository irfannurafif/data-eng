ward_codes = {
    'E05000131': 0,
    # 'E05000134': 0,
    # 'E05000136': 0,
    # 'E05000139': 0,
}

import csv

def fil(name):
    csvfile = open('Camden_Licensing_Applications_Beta.csv')
    csvfile = open('%s.csv' % name)
    reader = csv.DictReader(csvfile)
    header = next(reader)
    writer = csv.DictWriter(open('%s_filtered.csv' % name, 'w'), fieldnames=header)
    writer.writeheader()
    for row in reader:
        code = row['Ward Code']
        if code in ward_codes:
            writer.writerow(row)
            ward_codes[code] += 1

fil('Camden_Licensing_Applications_Beta')
fil('Companies_Registered_In_Camden_And_Surrounding_Boroughs')
for code in ward_codes:
    print(code, ward_codes[code])
