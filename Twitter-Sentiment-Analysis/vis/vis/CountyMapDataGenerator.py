#Creates data file for showing country heatmap

import csv
import Constants
import operator

LOCATION_COL = 13
COUNTY_NAME_COL = 5
CITY_COL = 3
STATE_NAME_COL = 4
ACCEPTABLE_CITY_COL = 4
COUNTY_NAME_COL_GLC = 4
STATE_NAME_COL_GLC = 2
STATE_FULL_NAME_COL_GLC = 1
STATE_CODE_COL = 3
COUNTY_CODE_COL = 6
FIPS_COL = 0
POP_COL = 6
YEAR_COL = 5

cityCountyMap = {}
countyCodeMap = {}
countyTweetCountMap = {}
stateNameMap = {}
cityStateMap = {}
countyPopMap = {}

def getCorrectedState(city, state):
    st = state.strip().lower()
    ct = city.strip().lower()
    if st == "usa":
        if ct in cityStateMap:
            return cityStateMap[ct]
    elif "usa" in st:
        stripped_st = st[:-3].strip()
        if stripped_st in stateNameMap:
            return stateNameMap[stripped_st]
    elif st in stateNameMap:
        return stateNameMap[st]
    else:
        return state
    return st


zip_file = csv.reader(open(Constants.FULL_DATA_FOLDER_PATH + "zip_codes_states.csv", encoding="utf8", errors='replace'))
all_lines = [l for l in zip_file]

for line in all_lines:
    city_name = line[CITY_COL]
    county_name = line[COUNTY_NAME_COL]
    state_name = line[STATE_NAME_COL]
    id = city_name.strip().lower() + ":" + state_name.strip().lower()
    countyId = county_name.strip().lower() + ":" + state_name.strip().lower()
    val = county_name.strip().lower() + ":" + state_name.strip().lower()

    if id not in cityCountyMap and county_name != "":
        cityCountyMap[id] = val
    if countyId not in cityCountyMap and county_name != "":
        cityCountyMap[countyId] = val
    if city_name.strip().lower() not in cityStateMap:
        cityStateMap[city_name.strip().lower()] = state_name.strip().lower()

# ---------------------------------------------------------------------------------------------------------------

glc_file = csv.reader(open(Constants.FULL_DATA_FOLDER_PATH + "GLC.csv", encoding="utf8", errors='replace'))
all_lines = [l for l in glc_file]

for line in all_lines:
    county_name = line[COUNTY_NAME_COL_GLC]
    county_code = line[COUNTY_CODE_COL]
    state_code = line[STATE_CODE_COL]
    state_name = line[STATE_NAME_COL_GLC]
    full_state_name = line[STATE_FULL_NAME_COL_GLC]
    id = county_name.strip().lower() + ":" + state_name.strip().lower()
    if id not in countyCodeMap:
        countyCodeMap[id] = (county_code, state_code, county_name, state_name)
    if full_state_name.strip().lower() not in stateNameMap:
        stateNameMap[full_state_name.strip().lower()] = state_name.strip().lower()

# ---------------------------------------------------------------------------------------------------------------

location_file = csv.reader(open(Constants.FULL_DATA_FOLDER_PATH + "real_full_location_data.csv", encoding="utf8", errors='replace'))
all_lines = [l for l in location_file]

counter_nocountymap = 0
counter_nocodemap = 0
counter_pos = 0
counter_nocountycode = 0
for tweet in all_lines:
    city, state = tweet[LOCATION_COL].split(": ")
    state = getCorrectedState(city, state)
    cityid = city.strip().lower() + ":" + state.strip().lower()
    if cityid in cityCountyMap:
        val = cityCountyMap[cityid]
        county_name, state_name = val.split(":")
        if val in countyCodeMap:
            county_code, state_code, county_name, state_name = countyCodeMap[val]
            county_val = county_name + ", " + state_name
            if county_code is not "":
                counter_pos = counter_pos + 1
                if int(state_code) >= 10:
                    id = str(state_code)+ "" +str(county_code)
                else:
                    id = str(state_code)+ "" +str(county_code)
                if id not in countyTweetCountMap:
                    countyTweetCountMap[id] = (1, county_val)
                else:
                    value = countyTweetCountMap[id]
                    countyTweetCountMap[id] = (value[0] + 1, county_val)
            else:
                counter_nocountycode = counter_nocountycode + 1
        else:
            counter_nocodemap = counter_nocodemap + 1
    else:
        counter_nocountymap = counter_nocountymap + 1

print("counter_nocountymap", counter_nocountymap)
print("counter_nocodemap", counter_nocodemap)
print("counter_pos", counter_pos)
print("counter_nocountycode", counter_nocountycode)



sortedMap = sorted(countyTweetCountMap.items(), key=operator.itemgetter(1), reverse=True)
print("Top 25 Counties:")
for i in range(25):
    each = sortedMap[i][1]
    county = each[1]
    count = each[0]
    print(county, ":", count)

with open(Constants.FULL_DATA_FOLDER_PATH + "countyMap.csv", "w") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["id", "count"])
    for key, value in countyTweetCountMap.items():
        writer.writerow([key, value[0]])



