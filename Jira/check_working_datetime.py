import datetime


def check_working_datetime(temp_datetime, temp_date,
                           temp_time, temp_day):
    """ A function which checks if datetime is considered as "working" datetime """

    # Set starting and ending working times for each day of the week
    weekdays_start_time = datetime.time(hour=8, minute=00)
    weekdays_end_time = datetime.time(hour=20, minute=00)
    sunday_start_time = datetime.time(hour=9, minute=00)
    sunday_end_time = datetime.time(hour=18, minute=00)
    next_date = temp_date + datetime.timedelta(days=1)

    check_datetime = None

    if temp_day == "Saturday":
        check_datetime = datetime.datetime.combine(next_date, sunday_start_time)

    if temp_day == "Sunday":
        if sunday_start_time <= temp_time < sunday_end_time:
            check_datetime = temp_datetime
        if temp_time < sunday_start_time:
            check_datetime = datetime.datetime.combine(temp_date, sunday_start_time)
        if temp_time >= sunday_end_time:
            check_datetime = datetime.datetime.combine(next_date, weekdays_start_time)

    if temp_day == "Friday":
        if weekdays_start_time <= temp_time < weekdays_end_time:
            check_datetime = temp_datetime
        if temp_time < weekdays_start_time:
            check_datetime = datetime.datetime.combine(temp_date, weekdays_start_time)
        if temp_time >= weekdays_end_time:
            check_datetime = datetime.datetime.combine(next_date + datetime.timedelta(days=1), sunday_start_time)

    if temp_day == "Monday" or temp_day == "Tuesday" or temp_day == "Wednesday" or temp_day == "Thursday":
        if weekdays_start_time <= temp_time < weekdays_end_time:
            check_datetime = temp_datetime
        if temp_time < weekdays_start_time:
            check_datetime = datetime.datetime.combine(temp_date, weekdays_start_time)
        if temp_time >= weekdays_end_time:
            check_datetime = datetime.datetime.combine(next_date, weekdays_start_time)

    return check_datetime
