import json

import pandas as pd
import requests
import json
from flask import Flask
from flask import request
from flask_restx import Resource, Api
from flask_restx import fields
from flask_restx import inputs
from flask_restx import reqparse
import time
import datetime
import math
from flask import send_file
import matplotlib.pyplot as plt
import flask
import sqlite3
import pandas as pd
from pandas.io import sql
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
import fiona
import adjustText as aT


app = Flask(__name__)
api = Api(app)

geo_df = pd.read_csv("georef-australia-state-suburb.csv", delimiter=";")
au_df = pd.read_csv("au.csv")
holiday_df = pd.read_json("https://date.nager.at/api/v2/publicholidays/2023/AU")

location = api.model('location',{
    'street': fields.String,
    'suburb': fields.String,
    'post_code': fields.String,
})
events = api.model('events', {
    'name': fields.String,
    'date': fields.String,
    'from': fields.String,
    'to': fields.String,
    'location': fields.List(fields.Nested(location)),
    'description': fields.String
})

#eventdata =
eventdf = pd.DataFrame({
        'id': pd.Series(dtype='str'),
        'name': pd.Series(dtype='str'),
        'date': pd.Series(dtype='str'),
        'from': pd.Series(dtype='str'),
        'to': pd.Series(dtype='str'),
        'location':pd.Series(dtype='str'),
        'description':pd.Series(dtype='str')
    })
cnx = sqlite3.connect('z5295162.db')
sql.to_sql(eventdf, name='calendar', con=cnx, if_exists='append')


def write_in_sqlite(dataframe, database_file, table_name):
    """
    :param dataframe: The dataframe which must be written into the database
    :param database_file: where the database is stored
    :param table_name: the name of the table
    """

    cnx = sqlite3.connect(database_file)
    sql.to_sql(dataframe, name=table_name, con=cnx, if_exists='replace')

def read_from_sqlite(database_file, table_name):
    """
    :param database_file: where the database is stored
    :param table_name: the name of the table
    :return: A Dataframe
    """
    cnx = sqlite3.connect(database_file)
    event_df = sql.read_sql('select * from ' + table_name, cnx)
    event_df.drop(columns=['index'], inplace=True)
    return event_df

def isoverlap(from_time, to_time, date, df):
    '''
    :param from_time:
    :param to_time:
    :param date: select all day except this date from df
    :param df:
    :return: bool
    '''
    day_event = df[df['date'] == date]
    for index, row in day_event.iterrows():
        start = time.strptime(row['from'], '%H:%M')
        end = time.strptime(row['to'], '%H:%M')
        if to_time <= start:
            continue
        if from_time >= end:
            continue
        return True
    return False

parser = reqparse.RequestParser()
parser.add_argument('order', type=str, default='+id')
parser.add_argument('page', type=int, default=1)
parser.add_argument('size', type=int, default=10)
parser.add_argument('filter', type=str, default='id,name')
@api.route('/events')
class EventList(Resource):
    @api.doc(params= {'order':'<CSV-FORMATED-VALUE>', 'page':'default page is 1', 'size':'default size is 10', 'filter':'<CSV-FORMATED-VALUE>'})
    def get(self):
        '''
        invalid page error: if given page number is not accessible: 400
        invalid key error: if given filter or order key is not exist: 400
        invalid input error: if given order format is not correct: 400
        success: return event_list: 200
        '''
        # get parameters
        args = parser.parse_args()
        order_key = args['order'].split(',')
        page = args['page']
        size = args['size']
        filter_key = args['filter'].split(',')

        event_df = read_from_sqlite('z5295162.db', 'calendar')
        # check page is a number >0 and not out of max page
        max_page = math.ceil(len(event_df.index)/size)
        if max_page < page or page <= 0:
            return {"message": "Page {} is not accessible".format(page)}, 400

        # check filter
        for key in filter_key:
            if key not in event_df.keys():
                return {"message": "Property {} in filter is invalid".format(key)}, 400

        # check order and separate '+-' and property
        # and convert '+-' to True and False
        order_list = []
        for i in range(len(order_key)):
            if order_key[i][0] == '+':
                order_list.append(True)
            elif order_key[i][0] == '-':
                order_list.append(False)
            else:
                return {"message": "Input {} in order is invalid".format(order_key[i])}, 400
            order_key[i] = order_key[i][1:]
            if order_key[i] not in event_df.keys():
                return {"message": "Property {} in order is invalid".format(order_key[i])}, 400

        # sort eventdf
        tempdf = event_df.sort_values(by = order_key, ascending = order_list)

        # select correct content for page
        event_list = []
        move_row = (page-1)*size
        tempdf = tempdf.shift(move_row).dropna().head(size)

        for index, row in tempdf.iterrows():
            event = {}
            for key in filter_key:
                event[key] = row[key]
            event_list.append(event)

        next = 'nan'
        if page+1 <= max_page:
            next = '/events?order=' + args['order'] + '&page='+str(args['page']+1)+'&size='+str(args['size'])+'&filter='+args['filter']
        massage = {
            'page':page,
            'page-size':size,
            'events':event_list,
            '_links': {
                'self': {
                    'href': '/events?order=' + args['order'] + '&page='+str(args['page'])+'&size='+str(args['size'])+'&filter='+args['filter']
                },
                "next": {
                    "href": next
                }

            }
        }
        return massage, 200




    @api.response(201, 'Event Created Successfully')
    @api.response(400, 'Validation Error')
    @api.doc(description="Add a new Event")
    @api.expect(events, validate = True)
    def post(self):
        '''
        invalid input date formate error: 400
        invalid input from_time or to_time format error: 400
        time conflict error: from_time > to_time: 400
        time overlap error: input time is overlap with events in df: 400
        input key error: not exist key is given: 400
        return: last update information: 201
        '''
        event = request.json
        # if date format is not correct, return error
        try:
            date = datetime.datetime.strptime(event['date'], '%d-%m-%Y')
        except:
            return {"message": "Invalid date in Event"}, 400
        
        # if time format is not correct, return error
        try:
            from_time = time.strptime(event['from'], '%H:%M')
            to_time = time.strptime(event['to'], '%H:%M')
        except:
            return {"message": "Invalid time in Event"}, 400

        # from_time should be smaller than to_time
        if from_time >= to_time:
            return {"message": "time is conflicting",
                    "from": time.strftime('%H:%M', from_time),
                    "to": time.strftime('%H:%M', to_time),
                    }, 400

        event_df = read_from_sqlite('z5295162.db', 'calendar')
        # check time overlap
        if isoverlap(from_time, to_time, date.strftime('%d-%m-%Y'), event_df):
            return {"message": "event is overlap",
                    "date": date.strftime('%d-%m-%Y'),
                    "from": time.strftime('%H:%M', from_time),
                    "to": time.strftime('%H:%M', to_time),
                    }, 400

        # i: index used to insert row
        # id: generate a number no exist in event_df['id']
        i = len(event_df.index)
        id = 0
        while True:
            if not (str(id) in list(event_df.id)):
                break
            id += 1

        for key in event:
            if key not in events.keys():
                return {"message": "Property {} is invalid".format(key)}, 400
            event_df.loc[i, key] = event[key]
        event_df.loc[i, 'id'] = id
        event_df = event_df.astype('string')
        write_in_sqlite(event_df, 'z5295162.db', 'calendar')

        now = datetime.datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        return {
                    "id": id,
                    "last-update": current_time,
                    "_links":{
                        "self": {
                            "href": "/events/"+str(id),
                        }
                    }
               }, 201


@api.route('/events/<int:id>')
class Events(Resource):
    @api.response(404, 'Event was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get a event by its ID")
    def get(self, id):
        '''
        param id: id of event
        not exist event id is given: 404
        return: event message: 200
        '''
        event_df = read_from_sqlite('z5295162.db', 'calendar')
        # check and get index with id
        try:
            index = (event_df[event_df['id'] == str(id)].index)[0]
        except:
            api.abort(404, "Event {} doesn't exist".format(id))
        event = dict(event_df.loc[index])
        # get event date in Y-M-D
        (d, m, y) = event['date'].split('-')
        event_date = datetime.date(int(y), int(m), int(d))

        metadata = {
            'weather': '-',
            'humidity': '-',
            'temperature': '-',
            'weekend':False,
            'holiday':'-'
        }
        # check weekend
        if event_date.weekday() >= 5:
            metadata['weekend'] = True

        # check event_date is holiday
        special_day_df = holiday_df[holiday_df['date'] == event_date.strftime('%Y-%m-%d')]
        if not special_day_df.empty:
            special_day = list(dict(special_day_df['localName']).values())[0]
            metadata['holiday'] = special_day

        # calculate day_difference between today and event_date
        # if day_difference <= 7 and >= 0, provide weather information
        today = datetime.datetime.now().date()
        day_difference = (event_date - today).days
        if day_difference <= 7 and day_difference >= 0:
            # check suburb if it is existed
            # sample location: ["{'street': 'string'", "'suburb': 'string'", "'post_code': 'string'}"]
            # therefore location[1] is "'suburb': 'string'"
            # add {} -> "{'suburb': 'string'}" and use eval make it back to dict
            location = event['location'].strip('][').split(', ')
            suburb = eval('{'+location[1]+'}')['suburb']
            loc_df = geo_df[geo_df['Official Name Suburb'] == suburb]
            if not loc_df.empty :
                # get parameter for weather api
                l = list(loc_df['Geo Point'].head(1))[0].split(', ')
                lon = l[1]
                lat = l[0]
                print(lon+ ' '+lat)
                weather_api = "http://www.7timer.info/bin/api.pl?lon="+lon+"&lat="+lat+"&product=civil&output=json"
                df = pd.read_json(weather_api)
                weather_df = pd.DataFrame(df['dataseries'])
                # calculate event period in which time point
                event_time_point = day_difference * 24 + int(event['from'][:2])
                for i in weather_df.index:
                    weather_info = dict(weather_df.loc[i])['dataseries']
                    # get weather when it finds time point
                    if weather_info['timepoint'] > event_time_point:
                        metadata['weather'] = weather_info['weather']
                        metadata['humidity'] = weather_info['rh2m']
                        metadata['temperature'] = weather_info['temp2m']
                        break
        event['_metadata'] = metadata

        # link
        # sort by date then from
        event_df['date'] = pd.to_datetime(event_df['date'], format='%d-%m-%Y')
        event_df.sort_values(by = ['date', 'from'],ascending = [True, True],inplace=True)

        prev_link = 'nan'
        if str(event_df['id'].shift(1).loc[index]) != 'nan':
            prev_link = '/events/' + str(int(event_df['id'].shift(1).loc[index]))
        next_link = 'nan'
        if str(event_df['id'].shift(-1).loc[index]) != 'nan':
            next_link = '/events/' + str(int(event_df['id'].shift(-1).loc[index]))

        event_df['date'] = event_df['date'].dt.strftime('%d-%m-%Y')

        links = {
            'self': {
                'href':'/events/' + str(id)
            },
            'previous': {
                'href': prev_link
            },
            'next': {
                'href': next_link
            }
        }

        event['_links'] = links
        return event, 200

    def delete(self, id):
        '''
        param id: id of event
        not exist event id is given: 404
        return: remove success: 200
        '''
        event_df = read_from_sqlite('z5295162.db', 'calendar')
        # check and get index with id
        try:
            index = (event_df[event_df['id'] == str(id)].index)[0]
        except:
            api.abort(404, "Event {} doesn't exist".format(id))

        event_df.drop(index, inplace=True)

        write_in_sqlite(event_df, 'z5295162.db', 'calendar')
        return {"message": "Event {} was removed from the database.".format(id),
                "id": id
                }, 200

    @api.expect(events)
    def put(self, id):
        '''
        param id: id of event
        not exist event id is given: 404
        invalid change event id: 400
        invalid input date formate error: 400
        invalid input from_time or to_time format error: 400
        time conflict error: from_time > to_time: 400
        time overlap error: input time is overlap with events in df: 400
        input key error: not exist key is given: 400
        return: last update information: 200
        '''
        event_df = read_from_sqlite('z5295162.db', 'calendar')
        # check and get index with id
        try:
            index = (event_df[event_df['id'] == str(id)].index)[0]
        except:
            api.abort(404, "Event {} doesn't exist".format(id))

        event = request.json
        original_event = event_df.loc[index]

        # event id cannot be changed
        if 'id' in event and id != event['id']:
            return {"message": "Identifier cannot be changed".format(id)}, 400

        # if date format is not correct, return error
        date = datetime.datetime.strptime(original_event['date'], '%d-%m-%Y')
        if event['date'] != '-':
            try:
                date = datetime.datetime.strptime(event['date'], '%d-%m-%Y')
            except:
                return {"message": "Invalid date in Event"}, 400

        # if time format is not correct, return error
        # make sure new time is not conflict with original time
        from_time = time.strptime(original_event['from'], '%H:%M')
        to_time = time.strptime(original_event['to'], '%H:%M')
        try:
            if event['from'] != '-':
                from_time = time.strptime(event['from'], '%H:%M')
            if event['to'] != '-':
                to_time = time.strptime(event['to'], '%H:%M')
        except:
            return {"message": "Invalid time in Event",
                    "from": time.strftime('%H:%M', from_time),
                    "to": time.strftime('%H:%M', to_time),
                    }, 400

        # from_time should be smaller than to_time
        if from_time >= to_time:
            return {"message": "time is conflicting",
                    "from": time.strftime('%H:%M', from_time),
                    "to": time.strftime('%H:%M', to_time),
                    }, 400

        if event['from'] != '-' and event['to'] != '-':
            if isoverlap(from_time, to_time, date.strftime('%d-%m-%Y'), event_df[event_df['id']!=id]):
                return {"message": "event is overlap",
                        "date": date.strftime('%d-%m-%Y'),
                        "from": time.strftime('%H:%M', from_time),
                        "to": time.strftime('%H:%M', to_time),
                        }, 400

        for key in event:
            if key not in events.keys():
                return {"message": "Property {} is invalid".format(key)}, 400
            # unchanged information in request should set as '-'
            if event[key] != '-':
                event_df.loc[index, key] = event[key]

        event_df = event_df.astype('string')
        write_in_sqlite(event_df, 'z5295162.db', 'calendar')

        now = datetime.datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        return {
                   "id": id,
                   "last-update": current_time,
                   "_links": {
                       "self": {
                           "href": "/events/" + str(id),
                       }
                   }
               }, 200


parser.add_argument('format', type=str, default='json', choices=['json', 'image'])
@api.route('/events/statistics')
class Statistics(Resource):
    @api.doc(params= {'format':'json or image'})
    def get(self):
        '''
        not existed events error: 404
        :return: event statistic json or image: 200
        '''
        event_df = read_from_sqlite('z5295162.db', 'calendar')
        args = parser.parse_args()
        # total: total events number
        total = len(event_df.index)
        if total == 0:
            return {'message':'no any event exist'}, 404
        total_current_week = 0
        total_current_month = 0
        per_days = {}

        day = datetime.datetime.now().date()
        current_month = day.strftime('%m-%Y')

        while day.weekday() <= 6:
            tempdf = event_df[event_df['date'] == day.strftime('%d-%m-%Y')]
            total_current_week += len(tempdf.index)
            day += datetime.timedelta(days=1)
            if day.weekday() == 6:
                tempdf = event_df[event_df['date'] == day.strftime('%d-%m-%Y')]
                total_current_week += len(tempdf.index)
                break

        # get total_current_month events number by create new column ['month']
        # and select dataframe with same month
        tempdf = event_df
        # sort by date then from
        tempdf['date'] = pd.to_datetime(tempdf['date'], format='%d-%m-%Y')
        tempdf.sort_values(by=['date'], ascending=[True], inplace=True)
        tempdf['date'] = tempdf['date'].dt.strftime('%d-%m-%Y')
        # month format: '%m-%Y' sample: '03-2023'
        tempdf['month'] = tempdf['date'].apply(lambda x:x[3:])
        tempdf_current_month = tempdf[tempdf['month'] == current_month]
        total_current_month = len(tempdf_current_month.index)

        # group by date and count number
        tempdf = tempdf.groupby(['date'], sort = False)['date'].count().reset_index(name = 'count')
        for index, row in tempdf.iterrows():
            per_days[row['date']] = row['count']

        if args['format'] == 'json':
            return {
                        'total': total,
                        'total-current-week': total_current_week,
                        'total-current-month': total_current_month,
                        'per-day': per_days
                   }, 200
        elif args['format'] == 'image':
            tempdf.plot.barh(x = 'date', y = 'count',figsize=(10, 10))
            plt.title('All Events Statistics')
            plt.savefig("{}.png".format("Events_statistics"))
            with open("Events_statistics.png", "rb") as image_file:
                data = image_file.read()
                response = flask.make_response(data)
                response.headers["content-type"] = "image/png"
                return response
        return

parser.add_argument('date', default=datetime.datetime.now().date().strftime('%d-%m-%Y'), type=str)
@api.route('/Weather')
class WeatherMap(Resource):
    @api.doc(params= {'date':'day-month-year'})
    def get(self):
        '''
        invalid date input error: date greate 7 than today or smaller than 0:400
        return: weather forcast map
        '''
        # check input date, should in 7 days from today
        args = parser.parse_args()
        (d, m, y) = args['date'].split('-')
        input_date = datetime.date(int(y), int(m), int(d))
        today = datetime.datetime.now().date()
        # day_difference: the days between input date and today
        day_difference = (input_date-today).days
        if day_difference >= 7 or day_difference < 0:
            return {"message": "no information for {}".format(args['date'])}, 400

        # city_map {
        #   city: [city name]
        #   geometry: [point of city]
        #   label: [information of weather and temperature and city name]
        # }
        # city_map include first 10 population cities' information
        temple = au_df.sort_values(by='population', ascending = False).head(10)
        city = temple[['city', 'lat', 'lng']]
        city_map = gpd.GeoDataFrame(city, geometry=gpd.points_from_xy(city.lng, city.lat))
        # weather_and_temp: use to store label information and appended to city_map['label']
        weather_and_temp = []
        for index, row in city.iterrows():
            # query weather API for each city
            lon = str(row['lng'])
            lat = str(row['lat'])
            weather_api = "http://www.7timer.info/bin/api.pl?lon="+lon+"&lat="+lat+"&product=civillight&output=json"
            df = pd.read_json(weather_api)
            weather_df = pd.DataFrame(df['dataseries'])
            weather_dict = dict(weather_df.loc[day_difference, 'dataseries'])
            temp_max = str(weather_dict['temp2m']['max'])
            temp_min = str(weather_dict['temp2m']['min'])
            weather_and_temp.append(row['city']+'\n'+weather_dict['weather']+'\ntemp_max: '+temp_max+'\ntemp_min: '+temp_min)
        city_map['label'] = weather_and_temp

        # map_df: use to draw suburb plot
        map_df = gpd.GeoDataFrame({
            'geometry': [],
            'color': []
        })
        for index, row in geo_df.iterrows():
            print(index)
            if geo_df.loc[index].isnull().any():
                continue
            dic = json.loads(row['Geo Shape'])
            if dic['type'] == 'Polygon':
                poly = Polygon(dic['coordinates'][0])
                map_df.loc[index, 'geometry'] = poly
                map_df.loc[index, 'color'] = 'skyblue'
            elif dic['type'] == 'MultiPolygon':
                poly = Polygon(dic['coordinates'][0][0])
                map_df.loc[index, 'geometry'] = poly
                map_df.loc[index, 'color'] = 'skyblue'

        ax = map_df.plot(figsize=(20, 20), color=map_df['color'])
        texts = []
        for x, y, label in zip(city_map.geometry.x, city_map.geometry.y, city_map.label):
            texts.append(plt.text(x, y, label))
            ax.scatter(x, y, c='red')

        aT.adjust_text(texts, force_points=0.5, force_text=1, expand_points=(1.5, 1.5), expand_text=(1, 1),
                       arrowprops=dict(arrowstyle="-", color='grey', lw=0.5))
        plt.title('Weather on '+args['date'])
        plt.savefig("weather.png")
        with open("weather.png", "rb") as image_file:
            data = image_file.read()
            response = flask.make_response(data)
            response.headers["content-type"] = "image/png"
            return response

if __name__ == '__main__':
    app.run(debug=True)