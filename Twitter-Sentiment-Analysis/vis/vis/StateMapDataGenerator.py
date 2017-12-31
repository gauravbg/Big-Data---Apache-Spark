#Creates data file for showing state heatmap

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

cityCountyMap = {}
countyCodeMap = {}
countyTweetCountMap = {}
stateNameMap = {}
cityStateMap = {}
stateCodeMap = {}
stateCountMap = {}

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
    state_name_mod = state_name.strip().lower()
    id = county_name.strip().lower() + ":" + state_name.strip().lower()
    if id not in countyCodeMap:
        countyCodeMap[id] = (county_code, state_code, county_name, state_name)
    if full_state_name.strip().lower() not in stateNameMap:
        stateNameMap[full_state_name.strip().lower()] = state_name.strip().lower()
    if state_name_mod not in stateCodeMap:
        stateCodeMap[state_name_mod] = state_code.strip().lower()


# ---------------------------------------------------------------------------------------------------------------

location_file = csv.reader(open(Constants.FULL_DATA_FOLDER_PATH + "real_full_location_data.csv", encoding="utf8", errors='replace'))
all_lines = [l for l in location_file]

counter_nostatcode = 0
counter_nocodemap = 0
counter_pos = 0
counter_nocountycode = 0
for tweet in all_lines:
    city, state = tweet[LOCATION_COL].split(": ")
    state = getCorrectedState(city, state)
    id = state.strip().lower()
    if id in stateCodeMap:
        counter_pos = counter_pos + 1
        state_code =  stateCodeMap[id]
        if state_code not in stateCountMap:
            stateCountMap[state_code] = 1
        else:
            stateCountMap[state_code] += 1
    else:
        counter_nostatcode = counter_nostatcode + 1

print("counter_nostatcode:", counter_nostatcode)
print("counter_pos:", counter_pos)

sortedMap = sorted(stateCountMap.items(), key=operator.itemgetter(1), reverse=True)
print("Top 10 States:")
for i in range(10):
    state_code = sortedMap[i][0]
    state_name = ""
    for key, value in stateCodeMap.items():
        if value ==  state_code:
            state_name = key
            break
    for key, value in stateNameMap.items():
        if state_name ==  value:
            state_name = key
            break
    count = sortedMap[i][1]
    print(state_name, ":", count)


with open(Constants.FULL_DATA_FOLDER_PATH + "stateMap.csv", "w") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["id", "count"])
    for key, value in stateCountMap.items():
        writer.writerow([key, value])



