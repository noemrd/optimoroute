#!/usr/bin/env python3
"""
This module connects to the optimoroute API to get delivery schedules. Delivery schedules contain
routes and stops. Each route has several stops. Here is an example:
    "duration":1302,
    "vehicleLabel":"Transit Van",
    "vehicleRegistration":"777",
    "driverSerial":"",
    "distance":1234,
    "driverName":"Mia Green",
    "stops":
        [   {"locationName":"Transit Station”,
            "scheduledAt":"16:35",
            "longitude":111.11,
            "address":"",
            "latitude":33.33,
            "stopNumber":1,
            "orderNo":"",
            "locationNo":"02"
            }
            {"locationName":"Sona",
            "scheduledAt":"12:42",
            "longitude":11.1,
            "address":"",
            "latitude":131.11,
            "stopNumber":2,
            "orderNo":"",
            "locationNo":"01"
        }
We need to store a week's worth of data
"""

import requests
import sqlalchemy
import sys
import datetime
import pytz
import logging as log
from datetime import date, timedelta
from sqlalchemy import create_engine, Column, Integer, String, text, bindparam
from config import PSQL_STRING, OPTIMOROUTE_KEY


def fetch_routes(day):
    """
    This function is used to get routes from the optimoroute api
    """
    schedule = requests.request(
        "GET",
        'https://api.optimoroute.com/v1/get_routes?key={}&date={}'.format(OPTIMOROUTE_KEY, day)
    ).json()

    try:
        all_rows_routes, all_rows_stops = set_up_data(schedule, day)
    except KeyError:
        msg = "Optimoroute call failed with: '{}'".format(schedule.get('message'))
        raise OptimorouteGetRoutesException(msg)

    return all_rows_routes, all_rows_stops


def set_to_pacific(date_time):
    """
    This function is used to set the time with a Pacific timezone
    FIXME: if we ever need to use timezones other than US/Pacific
    """
    local_tz = pytz.timezone("US/Pacific")
    datetime_without_tz = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M")
    datetime_with_tz = local_tz.localize(datetime_without_tz, is_dst=True)
    return datetime_with_tz


def set_up_data(schedule, day):
    """
    This function is used to set up the data for upload in the route and route_stop tables of
    the database
    """
    all_rows_routes = []
    all_rows_stops = []
    for x in schedule["routes"]:
        first_stop_time = x["stops"][0]["scheduledAt"]
        first_date_time = set_to_pacific("{} {}".format(day, first_stop_time))
        new_row_routes = {}
        new_row_routes.update({"route_date_time": first_date_time})
        new_row_routes.update({"duration": x["duration"]})
        new_row_routes.update({"vehicle_label": x["vehicleLabel"]})
        new_row_routes.update({"vehicle_registration": x["vehicleRegistration"]})
        new_row_routes.update({"driver_serial": x["driverSerial"]})
        new_row_routes.update({"distance": x["distance"]})
        new_row_routes.update({"driver_name": x["driverName"]})
        all_rows_routes.append(new_row_routes)
        for y in x["stops"]:
            stop_time = y["scheduledAt"]
            date_time = set_to_pacific("{} {}".format(day, stop_time))
            new_row_stops = {}
            new_row_stops.update({"route_date_time": first_date_time})
            new_row_stops.update({"driver_name": x["driverName"]})
            new_row_stops.update({"location_name": y["locationName"]})
            new_row_stops.update({"schedule_at": date_time})
            new_row_stops.update({"longitude": y["longitude"]})
            new_row_stops.update({"address": y["address"]})
            new_row_stops.update({"latitude": y["latitude"]})
            new_row_stops.update({"stop_number": y["stopNumber"]})
            new_row_stops.update({"order_number": y["orderNo"]})
            if y["locationNo"] == "":
                new_row_stops.update({"location_number": -1})
            else:
                new_row_stops.update({"location_number": y["locationNo"]})
            all_rows_stops.append(new_row_stops)
    return all_rows_routes, all_rows_stops


def insert_routes(all_rows_routes):
    """
    This function is used to insert all routes in route
    """
    if len(all_rows_routes) > 0:
        engine = create_engine(PSQL_STRING, convert_unicode=True)
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table('route', metadata,
                                 Column('route_date_time',
                                        sqlalchemy.types.DateTime(timezone=True)),
                                 Column('duration', Integer),
                                 Column('vehicle_label', String),
                                 Column('vehicle_registration', String),
                                 Column('driver_serial', String),
                                 Column('distance', sqlalchemy.types.DECIMAL),
                                 Column('driver_name', String),
                                 schema='schema1'
                                 )
        ins = table.insert().values(all_rows_routes)
        engine.execute(ins)


def insert_stops(all_rows_stops):
    """
    This function is used to insert all stops info in route_stop
    """
    if len(all_rows_stops) > 0:
        engine = create_engine(PSQL_STRING, convert_unicode=True)
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table('route_stop', metadata,
                                 Column('route_date_time',
                                        sqlalchemy.types.DateTime(timezone=True)),
                                 Column('driver_name', String),
                                 Column('location_name', String),
                                 Column('schedule_at', sqlalchemy.types.DateTime(timezone=True)),
                                 Column('longitude', sqlalchemy.types.DECIMAL),
                                 Column('address', String),
                                 Column('latitude', sqlalchemy.types.DECIMAL),
                                 Column('stop_number', Integer),
                                 Column('order_number', String),
                                 Column('location_number', Integer),
                                 schema='mixalot'
                                 )
        ins = table.insert().values(all_rows_stops)
        engine.execute(ins)


def delete_routes_and_stops(datee):
    """
    This function is used to delete all routes and stops for a given day
    """
    engine = create_engine(PSQL_STRING,
                           convert_unicode=True)

    psql_string_stop = text(
        "DELETE FROM route_stop "
        "WHERE route_date_time::date=:route_date_time"
    )
    engine.execute(psql_string_stop, route_date_time=datee)

    psql_string_route = text(
        "DELETE FROM route "
        "WHERE route_date_time::date=:route_date_time"
    )
    engine.execute(psql_string_route, route_date_time=datee)


def run_each_day(day):
    """
    This function is used to run other functions for each day.
    """
    msg = ''
    all_html_messages = []

    for x in range(7):
        delete_routes_and_stops(day)
        msg = 'Successfully deleted record for day {}, if any'.format(day)
        all_html_messages.append(msg)

        all_routes = []
        all_stops = []
        successfully_fetched_schedule = 0

        try:
            all_routes, all_stops = fetch_routes(day)
            msg = (
                'Successfully fetched the delivery schedule for day {}, if any'
            ).format(day)
            all_html_messages.append(msg)
            successfully_fetched_schedule = 1

        except OptimorouteGetRoutesException:
            msg = (
                'There was an exception fetching the delivery schedule for day {}'
            ).format(day)
            log.exception(msg)
            all_html_messages.append(msg)

        if successfully_fetched_schedule:
            try:
                insert_routes(all_routes)
                msg = (
                    'Successfully inserted routes in the database for day {}, if any'
                ).format(day)
                all_html_messages.append(msg)
            except sqlalchemy.exc.IntegrityError:
                msg = (
                    'Delivery routes for {} has already been imported in the database'
                ).format(day)
                log.exception(msg)
                all_html_messages.append(msg)

            try:
                insert_stops(all_stops)
                msg = (
                    'Successfully inserted stops in the database for day {}, if any'
                ).format(day)
                all_html_messages.append(msg)

            except sqlalchemy.exc.IntegrityError:
                msg = (
                    'Delivery stops for {} has already been imported in the database'
                ).format(day)
                log.exception(msg)
                all_html_messages.append(msg)

        all_html_messages.append('<br>')
        day = (day + timedelta(days=1))

    return '<br>\n'.join(all_html_messages)


class OptimorouteGetRoutesException(Exception):
    pass


def main():
    return run_each_day(date.today())


if __name__ == "__main__":
    sys.exit(main())
