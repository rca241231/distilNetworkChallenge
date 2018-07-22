import csv
from datetime import datetime, timedelta
from operator import itemgetter

"""
Step 1:

Get the access data and sort by timestamp
"""

with open('distilAccessLog.tsv', 'r') as tsv:
    AccessLogList = [line.strip().split('\t') for line in tsv]


for item in AccessLogList:
    item[1] = datetime.utcfromtimestamp(float(item[1]))

# Sort the list by timestamp
sorted(AccessLogList, key=itemgetter(1))


"""
Step 2:

Parse the data by IP as:
[session_id,
timespent,
first_timestamp,
last_timestamp,
pages_browsed,
last_page_browsed]
"""

session_dict = {}

for item in AccessLogList:
    # If we haven't add to the session dict, store session_id as 1, timespent,
    # pages first session, last session, pages, last page
    if item[0] not in session_dict.keys():
        session_dict[item[0]] = [[1, 0, item[1], item[1], 1, item[2]]]
    else:
        # If within 20 minutes of last session,
        # then update the length of time spent
        if item[1] < session_dict[item[0]][-1][3] + timedelta(minutes=20):
            # Update the total timespent
            session_dict[item[0]][-1][1] = (item[1] - session_dict[item[0]][-1][2]).total_seconds() / 60.0

            # Update the last timestamp
            session_dict[item[0]][-1][3] = item[1]

            # Update the page
            if item[2] != session_dict[item[0]][-1][5]:
                session_dict[item[0]][-1][5] = item[2]
                session_dict[item[0]][-1][4] += 1

        # Count as new session if not within 20 minutes
        else:
            # Increment the session id from the last one and update the fields
            session_dict[item[0]].append([session_dict[item[0]][-1][0] + 1, 0, item[1], item[1], 1, item[2]])

"""
Step 3: Compute the following and store as dim table/dictionary:

Avg pages per minute
Avg pages per session
"""

dim_ip_activities = {}
for key in session_dict.keys():
    if key not in dim_ip_activities.keys():
        pages = 0
        sessions = session_dict[key][-1][0]
        minutes = 0
        for item in session_dict[key]:
            pages += item[4]
            minutes += item[1]

        # Default to one minute if the aggregate number of minutes is 0
        minutes = minutes if minutes != 0 else 1
        dim_ip_activities[key] = [float(pages)/minutes, float(pages)/sessions]

"""
Step 4: Output to a CSV file
"""
dim_ip_activities_list = [[key] + dim_ip_activities[key] for key in dim_ip_activities.keys()]

"""
Output to a CSV file
"""
dim_ip_activities_list = [[key] + dim_ip_activities[key] for key in dim_ip_activities.keys()]

with open("dim_ip_activities.csv", 'w') as resultFile:
    writer = csv.DictWriter(resultFile, fieldnames=[
        "IP",
        "Pages per Minute",
        "Pages per Session"])
    writer.writeheader()

    wr = csv.writer(resultFile, dialect='excel')
    wr.writerows(dim_ip_activities_list)
