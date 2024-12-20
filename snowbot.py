# requires python >= 3.6
import os
import re
import requests
from dotenv import load_dotenv
import json
from datetime import date, timedelta, datetime, timezone
import tweepy
import pandas as pd
from dateutil.parser import parse as parsedate
from dateutil.relativedelta import relativedelta
from dateutil import tz
from multiprocessing import Process

def strfdelta(tdelta):
    hours, rem = divmod(tdelta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return '{:02d}:{:02d}'.format(minutes, seconds)

def suffix(day):
    if 4 <= day <= 20 or 24 <= day <= 30:
        suff = "th"
    else:
        suff = ["st", "nd", "rd"][day % 10 - 1]
    return suff

def readable_list(seq):
    seq = [str(s) for s in seq]
    if len(seq) < 3:
        return ' and '.join(seq)
    return ', '.join(seq[:-1]) + ', and ' + seq[-1]

def log_tweet(message):
    logfile = './log.txt'
    f = open(logfile, 'a', encoding="utf-8")
    newline = ord('\n')
    f.write(f"{newline}{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}, {message}")
    f.close()

class Timer:
    def __init__(self):
        self.startTime = datetime.now()

    def printer(self, message):
        print(strfdelta((datetime.now() - self.startTime)) + " (m:s) : " + message)


class WeatherDataDownloader:
    def __init__(self, city_id, t1, today, timer):
        self.timer = timer
        self.client_id = os.getenv("FROST_CLIENT_ID")
        self.city_id = city_id
        self.endpoint = 'https://frost.met.no/observations/v0.jsonld'
        self.t1 = t1
        self.today = today
        self.datasets = self.get_dataset_options()

    def initiate_downloads(self):
        processes = [Process(target=self.fetch_data, args=(key, self.datasets[key]['elements'], self.datasets[key]['d1'], self.datasets[key]['d2'], self.datasets[key]['filename'])) for key in self.datasets]
        for process in processes:
            process.start()
        # wait for all processes to complete
        for process in processes:
            process.join()

    def fetch_data(self, key, elements, d1, d2, filename):
        self.timer.printer(f"Downloading {filename}")
        parameters = {
            'sources': self.city_id,
            'elements': elements,
            'referencetime': d1+'/'+d2
        }
        response = requests.get(self.endpoint, parameters, auth=(self.client_id, ''))
        rjson = response.json()
        if response.status_code == 200:
            self.save_to_csv(key, rjson, filename)
        else:
            log_tweet('Coverage Error! Returned status code %s' % response.status_code)
            log_tweet('Message: %s' % rjson['error']['message'])
            log_tweet('Reason: %s' % rjson['error']['reason'])
            self.timer.printer('Download of data for ' + elements + ' failed - check log.')

    def get_dataset_options(self):
        t1plus1 = datetime.strptime(self.t1, '%H:%M:%S') + timedelta(hours=1)
        t2 = t1plus1.strftime('%H:%M:%S')
        tomorrow = self.today + timedelta(days=1)
        yesterday = self.today - timedelta(days=1)
        last_year = self.today - relativedelta(years=1)
        last_season = self.today - timedelta(days=290)
        max_d2 = yesterday if self.today.day > 1 else self.today

        stats = {
            'coverage': {
                'elements': 'snow_coverage_type,surface_snow_thickness',
                'd1': last_season.strftime("%Y-%m-%d") + 'T00:00:00',
                'd2': yesterday.strftime("%Y-%m-%d") + 'T23:00:00',
                'filename': 'snowCoverage.csv'},
            'today': {
                'elements': 'over_time(thickness_of_snowfall_amount P1D),surface_snow_thickness,max(surface_snow_thickness P1M),snow_coverage_type',
                'd1': yesterday.strftime("%Y-%m-%d") + 'T' + self.t1,
                'd2': self.today.strftime("%Y-%m-%d") + 'T' + t2,
                'filename': 'today.csv'},
            'max_daily': {
                'elements': 'over_time(thickness_of_snowfall_amount P1D)',
                'd1': '1900-07-15',
                'd2': tomorrow.strftime("%Y-%m-%d"),
                'filename': 'maxDailySnowfall.csv'},
            'max_this_month': {
                'elements': 'surface_snow_thickness',
                'd1': self.today.strftime("%Y-%m") + '-01T00:00:00',
                'd2': max_d2.strftime("%Y-%m-%d") + 'T23:00:00',
                'filename': 'maxSnowThisMonth.csv'},
            'max_historic': {
                'elements': 'max(surface_snow_thickness P1M),min(surface_snow_thickness P1M),mean(surface_snow_thickness P1M)',
                'd1': '1900-07-15',
                'd2': self.today.strftime("%Y-%m-%d"),
                'filename': 'maxMonthlySnow.csv'},
            'this_day_last_year': {
                'elements': 'surface_snow_thickness, snow_coverage_type',
                'd1': last_year.strftime("%Y-%m-%d") + 'T' + self.t1,
                'd2': last_year.strftime("%Y-%m-%d") + 'T' + t2,
                'filename': 'snowLastYear.csv'}
        }
        return stats

    def save_to_csv(self, name, frost_json, filename):
        if name == 'max_historic':
            data = frost_json['data']
            df = pd.DataFrame(columns=['referenceTime', 'max', 'min', 'mean'])
            for i in range(len(data)):
                row = [data[i]['referenceTime']]
                for x in data[i]['observations']:
                    row.append(x['value'])  # max first
                a_series = pd.DataFrame(row, index=['referenceTime', 'max', 'min', 'mean']).T
                df = pd.concat((df, a_series))

            # Convert the time value to something Python understands
            df['referenceTime'] = pd.to_datetime(df['referenceTime']).dt.date
        else:
            # extract json data to dataframe - observations are in their own list. normalize first
            df = pd.json_normalize(frost_json['data'],"observations", ['referenceTime'])
            # These columns will be kept
            df = df[['referenceTime', 'elementId', 'value']]
            # Convert the time value to something Python understands
            df['referenceTime'] = pd.to_datetime(df['referenceTime'])
        # Save the result
        df.to_csv(filename, index=False)
        self.timer.printer(f'Written {filename}')

class DataProcessor:
    def __init__(self, filename, today, t1, timer):
        self.timer = timer
        self.filename = filename
        self.local = tz.gettz("Europe/Oslo")
        self.today = today
        self.yesterday = self.today - timedelta(days=1)
        self.tomorrow = self.today + timedelta(days=1)
        self.last_year = self.today - relativedelta(years=1)
        self.t1 = t1

    def load_data(self):
        return pd.read_csv(self.filename, delimiter=',', header=0)

    def daily_snow_difference(self, df):
        daily_df = df[(df['elementId'] == 'surface_snow_thickness') &
                      (df['referenceTime'].astype(str).str.endswith(self.t1 + '+00:00'))]
        if daily_df.shape[0] == 2:
            # to avoid -1 values when snow free day before
            snow_change = daily_df['value'].iloc[1] - max(0, daily_df['value'].iloc[0])
            return round(snow_change, 1)
        else:
            return False

    def today_summary(self):
        today_df = self.load_data()
        # new snow & snow change
        new_snow_df = today_df[(today_df['elementId'] == 'over_time(thickness_of_snowfall_amount P1D)') &
                               (today_df['referenceTime'] == self.today.strftime("%Y-%m-%d") + ' 00:00:00+00:00')]
        new_snow = float(new_snow_df.iloc[0]['value']) if new_snow_df.size > 0 else -2.0

        snow_change = self.daily_snow_difference(today_df) if (new_snow <= 0) else None

        # snow thickness
        snow_thick_df = today_df[(today_df['elementId'] == 'surface_snow_thickness') &
                                 (today_df['referenceTime'] == self.today.strftime("%Y-%m-%d") + ' ' + self.t1 + '+00:00')]
        snow_thick = float(snow_thick_df.iloc[0]['value']) if snow_thick_df.size > 0 else -2.0  # at time t1

        # calculate the last snow thickness if needed
        last_thick_df = today_df[(today_df['elementId'] == 'surface_snow_thickness') & (today_df['value'] >= -1)]
        if last_thick_df.size <= 0:
            last_thickness = None
            last_thick_time = None
        else:
            last_thick_df = last_thick_df.sort_values(by=['referenceTime'], ascending=False)  # order by date desc
            first_row = last_thick_df.iloc[0]
            last_thick_time = first_row['referenceTime']
            last_thickness = first_row['value']


        # snow coverage
        coverage_df = today_df[(today_df['elementId'] == 'snow_coverage_type')]
        if coverage_df.size > 0:
            coverage_df = coverage_df.sort_values(by=['referenceTime'], ascending=False)  # order by date desc
            snow_coverage = float(coverage_df.iloc[0]['value'])  # at time t1
            last_coverage_time = coverage_df.iloc[0]['referenceTime']
        else:
            snow_coverage = None
            last_coverage_time = None

        summary = {
            'New snow': new_snow,
            'Snow change': snow_change,
            'Snow thickness now': snow_thick,
            'Last snow thickness': last_thickness,
            'Last snow thickness time': last_thick_time,  # in UTC
            'Snow coverage': snow_coverage,
            'Last coverage time': last_coverage_time  # in UTC
        }
        self.timer.printer('Stats...')
        for key, value in summary.items():
            print(f"{key}: {value}")
        return summary

    def last_time_snow_alt(self, coverage):
        if coverage is None:
            # check as snow coverage values not always written when there is snow thickness data
            coverage_data = self.load_data()
            coverage_data = coverage_data[(coverage_data['elementId'] == 'surface_snow_thickness')]
            coverage_data = coverage_data.sort_values(by=['referenceTime'], ascending=False)
            last_snow = coverage_data['referenceTime'].iloc[0]
            last_snow_date = datetime.strptime(last_snow[0:10], '%Y-%m-%d').date()
            last_snowfree_date_alt = last_snow_date + timedelta(days=1)
            snowfree_days_ago_alt = (self.today - last_snowfree_date_alt).days

            if snowfree_days_ago_alt > 1:
                days_ago_alt = 1
                last_snowfree_date_alt = self.yesterday
            else:
                i = self.today.strftime("%Y-%m-%d")
                days_ago_alt = None
                for ind in coverage_data.index:
                    if (coverage_data['referenceTime'][ind] == i or coverage_data['referenceTime'][ind] ==
                            ((datetime.strptime(i, '%Y-%m-%d').date() - timedelta(days=1)).strftime("%Y-%m-%d"))):
                        i = coverage_data['referenceTime'][ind]
                    else:
                        last_snowfree_date_alt = (
                                datetime.strptime(coverage_data['referenceTime'][ind], '%Y-%m-%d').date() +
                                timedelta(days=1))
                        days_ago_alt = (self.today - last_snowfree_date_alt).days
                        break
            return_days_ago = days_ago_alt
            return_date = last_snowfree_date_alt

            self.timer.printer(f"Last recorded snow thickness was {return_days_ago} days ago on {return_date.strftime('%B %d').replace(' 0', ' ')}{suffix(return_date.day)}")
            return return_days_ago

    def last_time_snow(self, coverage_file, is_snow):
        coverage_data = pd.read_csv(coverage_file, delimiter=',', header=0)
        if is_snow:
            trimmed_coverage_data = coverage_data[
                (coverage_data['elementId'] == 'snow_coverage_type') & (coverage_data['value'] > 0)]
        else:
            trimmed_coverage_data = coverage_data[
                (coverage_data['elementId'] == 'snow_coverage_type') & (coverage_data['value'] <= 0)]
        trimmed_coverage_data = trimmed_coverage_data.sort_values(by=['referenceTime'], ascending=False)

        last_sf = trimmed_coverage_data['referenceTime'].iloc[0]
        return_date = datetime.strptime(last_sf[0:10], '%Y-%m-%d').date()
        return_days_ago = (self.today - return_date).days
        message = 'Last recorded snow coverage was' if is_snow else 'Last time it was snow-free was'
        message += f" {return_days_ago} days ago on {return_date.strftime('%B %d').replace(' 0', ' ')}{suffix(return_date.day)}"
        self.timer.printer(message)
        return return_days_ago

    def max_monthly_snow(self):
        max_monthly_df = self.load_data()
        max_tm = max_monthly_df['value'].max()
        max_tm_date = max_monthly_df.loc[max_monthly_df.value.idxmax(), 'referenceTime']
        return [max_tm, max_tm_date]

    def thickness_last_year(self):
        df = self.load_data()
        trim_df = df[(df['elementId'] == 'surface_snow_thickness')]
        thickness = trim_df.iloc[0]['value'] if trim_df.size > 0 else None
        trim_df = df[(df['elementId'] == 'snow_coverage_type')]
        coverage = trim_df.iloc[0]['value'] if trim_df.size > 0 else None
        return [thickness, coverage]

class MonthlyTweetComposer:
    def __init__(self, today, city, timer):
        self.timer = timer
        self.city = city
        self.today = today

    def compile_monthly_summary_snowpack(self, max_monthly_df):
        first = self.today.replace(day=1)
        last_month = first - timedelta(days=1)
        last_month_pad = f'{last_month.month:02}'  # zero padded
        last30 = f"{last_month.year - 30}-{'%02d' % last_month.month}-01"  # 30 years ago
        last1 = f"{last_month.year}-{'%02d' % last_month.month}-01"  # 1 month ago
        trim30 = max_monthly_df[(max_monthly_df['referenceTime'].str.endswith(last_month_pad + '-01'))]
        last_month_trim = trim30[(trim30['referenceTime'] == last1)]
        trim30 = trim30[(trim30['referenceTime'] >= last30)]

        if last_month_trim.empty or (last_month_trim.size > 0 and last_month_trim['mean'].iloc[0] == 0):
            self.timer.printer('No monthly tweets available - no snow information.')
            log_tweet('Monthly tweets not sent - no snow information this month.')
            return None

        trim30_rank_max = trim30.sort_values(by=['max'], ascending=False, ignore_index=True)  # order by max desc
        month_trim = trim30[(trim30['referenceTime'] >= last30) & (trim30['referenceTime'] != last1)]
        month_trim = month_trim.sort_values(by=['referenceTime'], ascending=False)  # order by date desc

        trim30_rank_max['rankMax'] = trim30_rank_max['max'].rank(method='min', ascending=False)
        last_month_median = month_trim['mean'].median()
        last_month_mean = last_month_trim['mean'].iloc[0]
        last_month_max = last_month_trim['max'].iloc[0]
        mean_diff = last_month_mean - last_month_median
        tweet = f"The mean snow thickness in #{self.city} during {datetime.strptime(last_month_pad, '%m').strftime('%B')} ({last_month_mean} cm) was "

        if mean_diff > 0:
            tweet += f"{int(mean_diff)} cm greater than "
        elif mean_diff == 0:
            tweet += 'equal to '
        elif mean_diff < 0:
            tweet += f"{abs(mean_diff)} cm less than "
        tweet += f"the median value of {last_month_median} cm ({last_month.year - 30} - {last_month.year - 1}). "

        max_rank = trim30_rank_max[(trim30_rank_max['referenceTime'] == last1)].values.tolist()
        joint_years = len(trim30_rank_max[(trim30_rank_max['max'] == last_month_max)].values.tolist())

        newline = ord('\n')
        tweet += f'{newline}{newline}The max snow depth last month ({last_month_max} cm) ranks '
        if joint_years > 1:
            tweet += 'joint '
        tweet += f"{int(max_rank[0][4])}{suffix(max_rank[0][4])} since {last_month.year - 30}, with the 3 deepest {datetime.strptime(last_month_pad, '%m').strftime('%b')} snowpacks being {trim30_rank_max['referenceTime'].iloc[0][0:4]} ({trim30_rank_max['max'].iloc[0]} cm), {trim30_rank_max['referenceTime'].iloc[1][0:4]} ({trim30_rank_max['max'].iloc[1]} cm) and {trim30_rank_max['referenceTime'].iloc[2][0:4]} ({trim30_rank_max['max'].iloc[2]} cm)."
        return tweet

    def compile_monthly_summary_snowfall(self, max_daily_df):
        first = self.today.replace(day=1)
        last_month = first - timedelta(days=1)
        last_month_pad = f'{last_month.month:02}'  # zero padded
        this_month = f'{self.today.month:02}'  # zero padded
        snow_days_df = pd.DataFrame()  # snowDays
        trim30 = pd.DataFrame()
        snow_days_count = 0
        snow_days_cm = 0

        for y in range(1900, last_month.year+1, 1):
            # remove the first from each of last month and add the first of this month
            # filter only last month's data
            trim30 = max_daily_df[((max_daily_df['referenceTime'].str.startswith(str(y)+'-' + last_month_pad)) &
                                   (max_daily_df['referenceTime'] != str(y)+'-'+last_month_pad+'-01')) |
                                  (max_daily_df['referenceTime'].str.startswith(str(y)+'-'+this_month+'-01'))]
            snow_days = trim30[trim30['value'] > 0]
            snow_days_cm = trim30['value'].sum()
            snow_days_count = snow_days.shape[0]
            if snow_days_cm > 0:
                new_row = pd.DataFrame([y, snow_days_cm, int(snow_days_count)], index=['year', 'snow', 'days']).T
                snow_days_df = pd.concat([snow_days_df, new_row])

        if snow_days_count == 0:
            tweet = f'It did not snow in #{self.city} last month.'
        else:
            tweet = f"Last month it snowed a total of {snow_days_cm} cm over {snow_days_count} {'day' if snow_days_count == 1 else 'days'}, "

        trim30 = trim30.sort_values(by=['value'], ascending=False, ignore_index=True)  # order by max desc
        most_snow_date = pd.to_datetime(trim30['referenceTime'].iloc[0]) - timedelta(days=1)
        most_snow_cm = trim30['value'].iloc[0]
        tweet += f"with most snow falling on {datetime.strftime(most_snow_date, '%B %d').replace(' 0', ' ')}{suffix(most_snow_date.day)} ({most_snow_cm} cm)."

        snow_days_df = snow_days_df.sort_values(by=['snow'], ascending=False, ignore_index=True)  # order by max desc
        snow_days_df['rank'] = snow_days_df['snow'].rank(ascending=False, method='min').astype(int)
        this_year_rank = snow_days_df['year'][snow_days_df['year'] == last_month.year].index[0]
        records_began = int(snow_days_df['year'].min())

        if snow_days_df['year'].iloc[0] == last_month.year:
            tweet += ' This total monthly snowfall for ' + str(datetime.strftime(last_month, '%B')) + ' ranks the highest based from data available since ' + str(records_began) + '. ' + u'\U0001F947'
        else:
            tweet += f" This total monthly snowfall for {datetime.strftime(last_month, '%B')} ranks {snow_days_df['rank'].iloc[this_year_rank]}{suffix(snow_days_df['rank'].iloc[this_year_rank])} since available records began in {records_began}, with the most snow falling in {int(snow_days_df['year'].iloc[0])} ({int(snow_days_df['snow'].iloc[0])} cm)."
        if snow_days_df['rank'].iloc[this_year_rank] == 2:
            tweet += u'\U0001F948'
        elif snow_days_df['rank'].iloc[this_year_rank] == 3:
            tweet += u'\U0001F949'
        return tweet


class TweetComposer:
    def __init__(self, summary, today, city, api, client, timer):
        self.api = api
        self.client = client
        self.summary = summary
        self.timer = timer
        self.city = city
        self.graph = 'https://cryo.met.no/sites/cryo.met.no/files/latest/snowdepth_90450_latest_en.png'
        self.today = today
        self.yesterday = self.today - timedelta(days=1)
        self.tomorrow = self.today + timedelta(days=1)
        self.local = tz.gettz("Europe/Oslo")
        self.t1 = '09:00:00'
        self.tweetLim = 280 # Twitter
        self.skeetLim = 300 # BlueSky

    def check_special_days(self):
        celebrations = {
            'HNY' : {"day": 1, "month": 1, "tweet": u' Godt nyttår! \U0001F386\n\n' },
            'night_end': {"day": 15, "month": 1, "tweet": u' Welcome back, sun! \U0001F304\n\n'},
            'earth_day': {"day": 22, "month": 4, "tweet": u' Happy Earth Day! \U0001F30D\n\n'},
            'may_17': {"day": 17, "month": 5, "tweet": u' Gratulerer med dagen! \U0001F1F3\U0001F1F4 \U0001F389\n\n'},
            'midnight_sun': {"day": 18, "month": 5, "tweet": u' Welcome to the Midnight Sun! \U00002600\n\n'},
            'summer_solstice': {"day": 21, "month": 6, "tweet": u' Happy Summer Solstice! \U0001F307\n\n'},
            'polar_night': {"day": 27, "month": 11, "tweet": u' Welcome to the Polar Night! \U0001F315\n\n'},
            'winter_solstice': {"day": 21, "month": 12, "tweet": u' Happy Winter Solstice! \U0001F303\n\n'},
            'christmas': {"day": 24, "month": 12, "tweet": u' God Jul! \U0001F384\U00002603\n\n'},
            'christmas_day': {"day": 25, "month": 12, "tweet": u' God Jul! \U0001F384\U00002603\n\n'}
        }

        for c, obj in celebrations.items():
            if self.today.day == obj['day'] and self.today.month == obj['month']:
                return obj['tweet']

    def last_known_snow(self):
        last_thick_time_utc = datetime.strptime(self.summary['Last snow thickness time'], '%Y-%m-%d %H:%M:%S%z')
        last_thick_time_local = last_thick_time_utc.astimezone(self.local)
        last_thick_time_hm = last_thick_time_local.strftime("%H%M")
        if last_thick_time_utc.date() == self.today:
            last_thick_day = 'today'
        else:
            last_thick_day = 'on ' + last_thick_time_local.strftime('%B %d').replace(' 0', ' ') + suffix(last_thick_time_local.day) + ', ' + last_thick_time_local.strftime('%Y')

        # if just snow coverage recorded (ie less than 0,5 cm deep)
        if self.summary['Last snow thickness'] == 0 and self.summary['Snow coverage'] > 0:
            if int(self.summary['New snow']) > 0:
                return u'\U0001F328 The last recorded snow thickness in #' + self.city + ' was less than 0.5 cm at ' + last_thick_time_hm + ' ' + last_thick_day + ' following ~' + str(
                    int(self.summary['New snow'])) + ' cm of fresh snowfall in the last ~24hrs.'
            else:
                return f'The last recorded snow thickness in #' + self.city + f' was less than 0.5 cm at {last_thick_time_hm} {last_thick_day}.'

        elif self.summary['Last snow thickness'] > 0:
            if int(self.summary['New snow']) > 0:
                return u'\U0001F328 The snow is roughly ' + str(self.summary['Last snow thickness']) + ' cm deep in #' + self.city + ' this morning after '+str(int(self.summary['New snow'])) + ' cm of fresh snowfall in the last ~24hrs.'
            else:
                return f"The last recorded snow thickness in #" + self.city + f" was {self.summary['Last snow thickness']} cm at {last_thick_time_hm} {last_thick_day}."
        else:
            return ''

    def is_snow_free(self):
        if self.summary['Snow coverage'] > 0 or self.summary['Snow thickness now'] > 0 or (self.summary['Last snow thickness'] is not None and self.summary['Last snow thickness'] != -1):
            return False
        else:
            return True

    def snow_arrived_back(self, days_ago):
        # Set to 3 week delay so as not to repeat the message too often e.g. in early season
        if days_ago > 21 and (self.summary['Snow coverage'] >= 1 or  self.summary['Snow thickness now'] or  self.summary['Snow last thickness'] > 0):
            last_snowfree_date = self.today - timedelta(days=days_ago)
            self.timer.printer(f"Snow arrived back?...Yes!")
            return u'\U0001F328\U0001F389 Snow has arrived back in #' + self.city + '! The last time any snow was recorded was ' + str(days_ago) + ' days ago on ' + last_snowfree_date.strftime('%B %d').replace(' 0', ' ') + suffix(last_snowfree_date.day)+'.\n\n'

    def summarize_today(self):
        if str(self.summary['Last coverage time']) < self.today.strftime("%Y-%m-%d") and str(self.summary['Last snow thickness time']) < self.today.strftime("%Y-%m-%d"):
            return 'No snow measurements have been found so far today.'

        if self.is_snow_free():
            return f'There is currently no snow in #{self.city}.'

        if self.summary['Last snow thickness'] is not None and (self.summary['Snow thickness now'] is None or self.summary['Snow thickness now'] == -2):
            return self.last_known_snow()

        elif self.summary['Snow coverage'] > 0 and self.summary['Snow thickness now'] <= -1:
            if self.summary['New snow'] > 0:
                tweet = u'\U0001F328 Following ~'+str(int(self.summary['New snow']))+' cm of fresh snowfall in the last ~24hrs, the '
            else:
                tweet = 'The '
            if self.summary['Snow coverage'] == 4:
                tweet += 'ground is completely snow-covered '
            elif self.summary['Snow coverage'] == 3:
                tweet += 'ground is mostly snow-covered '
            elif self.summary['Snow coverage'] == 2:
                tweet += 'ground is roughly half snow-covered '
            elif self.summary['Snow coverage'] == 1:
                tweet += 'ground is mostly snow-free '
            tweet += f'in #{self.city}'

            # check when coverage measurement made
            if str(self.summary['Last coverage time']) == self.today.strftime("%Y-%m-%d") + ' ' + self.t1 + '+00:00':
                tweet += '.'
            else:
                last_coverage_utc = datetime.strptime(self.summary['Last coverage time'], '%Y-%m-%d %H:%M:%S%z')
                last_coverage_local = last_coverage_utc.astimezone(self.local)
                last_coverage_hm = last_coverage_local.strftime("%H:%M")
                if last_coverage_utc.date() == self.today:
                    last_coverage_day = 'today'
                else:
                    last_coverage_day = 'on ' + last_coverage_local.strftime('%B %d').replace(' 0', ' ') + suffix(last_coverage_local.day) + ', ' + last_coverage_local.strftime('%Y')
                tweet += f' according to the last measurement taken at {last_coverage_hm} {last_coverage_day}.'
            return tweet

        if self.summary['Snow thickness now'] > -1:
            if self.summary['New snow'] > 0:
                tweet = (u'\U0001F328 Following ~' + str(int(self.summary['New snow'])) + ' cm of fresh snowfall in the last ~24hrs, the snow in #' + self.city + ' ')
            else:
                if self.summary['Snow change'] < 0:
                    tweet = u'\U0001F4C9 The snow in #' + self.city + ' decreased by ~' + str(abs(self.summary['Snow change'])) + ' cm over the last 24hrs and '
                elif self.summary['Snow change'] > 0:
                    tweet = u'\U0001F4C8 The snow in #' + self.city + ' increased by ~' + str(abs(self.summary['Snow change'])) + ' cm over the last 24hrs and '
                elif self.summary['Snow change'] == 0:
                    tweet = u'\u2696\uFE0F The snow level in #' + self.city + ' has remained unchanged over the last 24hrs and '
                else:
                    tweet = f"The snow in #{self.city} "

            if self.summary['Snow thickness now'] > 0:
                tweet += f"is currently {self.summary['Snow thickness now']} cm deep"
            elif self.summary['Snow thickness now'] == 0:
                tweet += 'is currently less than 0.5 cm deep'
            elif self.summary['Snow thickness now'] == -1.0:
                tweet += 'is currently 0 cm deep'
            elif self.summary['Snow thickness now'] < -1.0:
                tweet += 'is currently unknown'
            else:
                tweet += ''

            if self.summary['Snow coverage'] == 3:
                tweet += ', with the ground mostly snow-covered.'
            elif self.summary['Snow coverage'] == 2:
                tweet += ', with the ground partially snow-covered.'
            elif self.summary['Snow coverage'] == 1:
                tweet += ', with the ground mostly snow-free.'
            else:
                tweet += '.'
            return tweet

    def snow_free_days(self, days_ago):
        date_ago = self.today - timedelta(days=days_ago)
        self.timer.printer(f"Last snow-free {days_ago} days ago")
        if days_ago >= 14:
            return f" The last time it was snow-free in {self.city} was {days_ago} days ago on {date_ago.strftime('%B %d').replace(' 0', ' ')}{suffix(date_ago.day)}."

    def last_snowfall(self, max_daily_data):
        max_daily_data = max_daily_data.sort_values(by=['referenceTime'], ascending=False)  # order by date desc
        # only show if snow decreases as it is likely any +ve snow_change is from fresh snow after midnight.
        if self.summary['New snow'] == 0 and self.summary['Snow thickness now'] > 0 >= self.summary['Snow change']:
            for index, row in max_daily_data.iterrows():
                dt = datetime.strptime(row['referenceTime'], '%Y-%m-%d %H:%M:%S%z').date()
                dt += timedelta(days=-1)  # 24 hours period before
                if dt >= self.today:
                    continue
                if row['value'] > 0 and int((self.today - dt).days) < 7:
                    self.timer.printer(f'Last recorded snowfall >1 cm...{(self.today - dt).days} days ago')
                    break
                elif row['value'] > 0 and int((self.today - dt).days) >= 7:
                    self.timer.printer(f'Last recorded snowfall >1 cm...{(self.today - dt).days} days ago')
                    return f' The last recorded snowfall >1 cm was {(self.today - dt).days} days ago.'

    def that_much(self, max_daily_data):
        max_daily_data = max_daily_data.sort_values(by=['referenceTime'], ascending=False, ignore_index=True)
        if self.summary['New snow'] >= 10:
            for index, row in max_daily_data.iterrows():
                dt = datetime.strptime(row['referenceTime'], '%Y-%m-%d %H:%M:%S%z').date()
                dt += timedelta(days=-1)  # 24 hours period before
                if dt >= self.yesterday:
                    continue

                if row['value'] >= self.summary['New snow']:
                    self.timer.printer(f"Last time it snowed that much...on {dt.strftime('%B %d').replace(' 0', ' ')}{suffix(dt.day)}, {dt.strftime('%Y')} ({row['value']} cm).")
                    if (self.today-dt).days >= 5:
                        return f" The last time it snowed that much was on {dt.strftime('%B %d').replace(' 0', ' ')}{suffix(dt.day)}, {dt.strftime('%Y')} ({row['value']} cm)."
                    break

    def last_deeper(self, max_this_month, max_monthly_historic_df):
        if self.summary['Snow thickness now'] >= 10 and self.summary['Snow thickness now'] > max_this_month[0]:
            self.timer.printer(f'Max this month: {max_this_month[0]}')
            this_month = f'{self.today.month:02}'  # zero padded
            # filter only current month data
            monthly_trim = max_monthly_historic_df[(max_monthly_historic_df['referenceTime'].str[5:10] == (this_month + '-01'))]
            monthly_trim = monthly_trim.sort_values(by=['referenceTime'], ascending=False)  # order by date desc

            for index, row in monthly_trim.iterrows():
                self.timer.printer(
                    f"Last time it was deeper during {pd.to_datetime(int(this_month), format='%m').strftime('%B')}...was in {row['referenceTime'][0:4]} ({row['max']} cm).")
                if float(row['max']) > self.summary['Snow thickness now'] and (self.today.year - int(row['referenceTime'][0:4])) < 3:
                    break
                elif float(row['max']) > self.summary['Snow thickness now'] and (self.today.year - int(row['referenceTime'][0:4])) >= 3:
                    return f" The last time snow in {self.city} was deeper during {pd.to_datetime(int(this_month), format='%m').strftime('%B')} was in {row['referenceTime'][0:4]} ({row['max']} cm)."


    def on_this_date(self, max_daily_data):
        max_daily_data['referenceTime'] = max_daily_data['referenceTime'].str.replace(' 00:00:00+00:00', '', regex=False)
        these_days = max_daily_data[(max_daily_data['referenceTime'].str.endswith(f'{self.tomorrow.month:02}' + '-' +
                                                                                  f'{self.tomorrow.day:02}'))]
        if these_days.size == 0:
            return None
        these_snow_days = these_days[(these_days['value'] > 0)]

        if these_snow_days.size == 0:
            return None
        these_snow_days = these_snow_days.sort_values(by=['referenceTime'], ascending=False, ignore_index=True)

        # last snowed before the last decade
        if int(these_snow_days.iloc[0]['referenceTime'][0:4]) < (int(self.tomorrow.year) - 10):
            self.timer.printer('Snowed on this date...in ' + these_snow_days.iloc[0]['referenceTime'][0:4])
            return ' It last snowed on this date in ' + these_snow_days.iloc[0]['referenceTime'][0:4] + '.'

        s= 0
        for index, row in these_snow_days.iterrows():
            if int(row['referenceTime'][0:4]) == self.today.year or int(row['referenceTime'][0:4]) < (int(self.tomorrow.year) - 10):
                these_snow_days = these_snow_days.drop(index=[index])
            else:
                s += 1

        max_these_snow_days = these_snow_days['value'].max()
        max_year = these_snow_days.loc[these_snow_days.value.idxmax(), 'referenceTime']

        tweet_n = ''
        if s >= 2:
            self.timer.printer(f'Snowed on this date...{s} times')
            tweet_n = f" It has snowed {'twice' if s == 2 else f'{s} times'} on this date since {int(self.tomorrow.year) - 10}: in "

            years_to_list = these_snow_days['referenceTime'].tolist()
            for i, s in enumerate(years_to_list):
                years_to_list[i] = s[0:4]
            years_to_list.reverse()
            tweet_n += readable_list(years_to_list)  # list years
            tweet_n += f', with the most snow falling in {max_year[0:4]} ({max_these_snow_days} cm).'

        elif s == 1:
            tweet_n = f" It has snowed once on this date since {int(self.tomorrow.year) - 10}, with {these_snow_days.iloc[0]['value']} cm falling in {these_snow_days.iloc[0]['referenceTime'][0:4]}."

        return tweet_n

    def last_year(self, snow_last_year):
        thickness = snow_last_year[0]
        coverage = snow_last_year[1]
        self.timer.printer(f'This time last year...{thickness} cm deep, coverage: {coverage}')
        if thickness and (coverage or thickness >= -1):
            tweet_n = ' This time last year '
            if (thickness == -1 or thickness == 0) and coverage is not None:
                if coverage == 4:
                    tweet_n += 'the ground was completely snow-covered.'
                elif coverage == 3:
                    tweet_n += 'the ground was mostly snow-covered.'
                elif coverage == 2:
                    tweet_n += 'the ground was roughly half snow-covered.'
                elif coverage == 1:
                    tweet_n += 'the ground was mostly snow-free.'
                else:
                    tweet_n += f'{self.city} was snow-free.'
            elif thickness > 0:
                tweet_n += f'the snow was {thickness} cm deep.'
            return tweet_n
        elif coverage is None and (thickness is None or thickness == -1):
            tweet_n = f' This time last year {self.city} was snow-free.'
            return tweet_n

    def get_graph(self, savename):
        mt = requests.head(self.graph)
        url_date = parsedate(mt.headers['last-modified'])
        if url_date > datetime(self.today.year, self.today.month, self.today.day, 7, 00, 00, tzinfo=timezone.utc):
            filename = self.graph.split('/')[-1]
            url = self.graph
            self.timer.printer('Downloading ' + filename + ' as ' + savename)
            with requests.get(url, stream=True) as dFile:
                with open('./' + savename, "wb") as SI:
                    for chunk in dFile.iter_content(chunk_size=1024):
                        if chunk:
                            SI.write(chunk)
            self.timer.printer('Written ' + savename)
            return True
        else:
            log_tweet(f'Graph not downloaded: file last modified on {url_date}')
            return False

    def construct_tweet_text(self, tweet_texts):
        smaller = lambda a, b: a if a < b else b
        limit = {smaller(self.skeetLim, self.tweetLim)}.pop()
        tweet_texts = list(filter(lambda x: x is not None, tweet_texts)) # drop None
        new_join = None

        # construct tweet until tweet limit reached
        for i in range(len(tweet_texts)):
            new_join = tweet_texts[:i + 1]
            if len(''.join(new_join).encode('utf-16-le'))/2 > limit:
                new_join = tweet_texts[:i]
                del tweet_texts[i]

        self.timer.printer('Tweet length: ' + str(int(len(''.join(new_join).encode('utf-16-le'))/2)))
        return ''.join(new_join)

    def is_graph_needed(self):
        graphs = []
        if self.summary['New snow'] >= 3 or abs(self.summary['Snow change']) >= 5:
            graph_name = './' + self.today.strftime("%Y_%m_%d") + '_snowdepth_90450.png'
            if self.get_graph(graph_name):
                graphs.append(graph_name)
        return graphs

    def send_tweet(self,tweet, image):
        if image:
            image_ids = []
            for i in image:
                media = self.api.media_upload(filename=i)
                self.timer.printer(f'Uploaded media. {media.media_id}')
                image_ids.append(media.media_id)
            daily_status = self.client.create_tweet(text=tweet, media_ids=image_ids)
        else:
            daily_status = self.client.create_tweet(text=tweet)
        log_tweet('Tweeted: ' + tweet)
        log_tweet(f"TweetID: {daily_status['data']['id']}")

    def send_summary_tweets(self, tweets):
        last_tweet_id = None
        for i, t in enumerate(tweets):
            t_checked = self.construct_tweet_text([t])
            if t_checked is None:
                log_tweet('Tweet is too long: ' + t)
                continue

            # thread the summaries
            if i == 0:
                make_tweet = self.client.create_tweet(text=t_checked)
            else:
                make_tweet = self.client.create_tweet(text=t_checked, in_reply_to_tweet_id=last_tweet_id)
            log_tweet(f"Tweeted: {t_checked}")
            log_tweet(f"TweetID: {make_tweet['data']['id']} {last_tweet_id}")
            last_tweet_id = make_tweet['data']['id']


class Skeet:
    def __init__(self, today, city):
        self.today = today
        self.city = city
        self.handle = os.getenv("BSKY_USERNAME")
        self.password = os.getenv("BSKY_PASSWORD")
        self.pds = 'https://bsky.social'
        self.url = self.pds + "/xrpc/com.atproto.server.createSession"

    def bsky_login_session(self):
        resp = requests.post(self.url, json={"identifier": self.handle, "password": self.password})
        resp.raise_for_status()
        return resp.json()

    def upload_file(self, access_token, filename, img_bytes):
        ext = filename.split(".")[-1].lower()
        mimetype = "application/octet-stream"
        if ext in ["png"]:
            mimetype = "image/png"
        elif ext in ["jpeg", "jpg"]:
            mimetype = "image/jpeg"
        elif ext in ["webp"]:
            mimetype = "image/webp"

        # WARNING: a non-naive implementation would strip EXIF metadata from JPEG files here by default
        resp = requests.post(
            self.pds + "/xrpc/com.atproto.repo.uploadBlob",
            headers={
                "Content-Type": mimetype,
                "Authorization": "Bearer " + access_token,
            },
            data=img_bytes,
            )
        resp.raise_for_status()
        return resp.json()["blob"]

    def upload_images(self, access_token, image_paths, alt_text):
        images = []
        for ip in image_paths:
            with open(ip, "rb") as f:
                img_bytes = f.read()
            # this size limit specified in the app.bsky.embed.images lexicon
            if len(img_bytes) > 1000000:
                raise Exception(
                    f"image file size too large. 1000000 bytes maximum, got: {len(img_bytes)}"
                )
            blob = self.upload_file(access_token, ip, img_bytes)
            images.append({"alt": alt_text or "", "image": blob})
        return {
            "$type": "app.bsky.embed.images",
            "images": images,
        }

    def parse_tags(self, text):
        spans = []
        text_bytes = text.encode("UTF-8")
        for m in re.finditer(b'#', text_bytes):
            spans.append({
                "index": {
                    "byteStart": m.start(0),
                    "byteEnd": m.start(0) + len((u'#'+self.city).encode('UTF-8'))
                },
                "features": [{
                    "tag": self.city,
                    "$type": "app.bsky.richtext.facet#tag"
                }]
            })
        return spans

    def send_skeet(self, tweets, image):
        session = self.bsky_login_session()
        root = None # for initial post not needed
        parent = None
        for i, t in enumerate(tweets):
            # trailing "Z" is preferred over "+00:00"
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            # these are the required fields which every post must include
            post = {
                "$type": "app.bsky.feed.post",
                "text": t,
                "createdAt": now,
            }

            if root:
                post["reply"] = {
                    "root": {
                        "uri": root["uri"],
                        "cid": root["cid"],
                    },
                    "parent": {
                        "uri": parent["uri"],
                        "cid": parent["cid"],
                    }
                }

            facets = self.parse_tags(t)
            if facets:
                post["facets"] = facets

            if image:
                alt_text = 'Chart showing the snow depth in ' + self.city + ' as of ' + self.today.strftime("%d %B, %Y").replace(' 0', ' ')
                post["embed"] = self.upload_images(session["accessJwt"], image, alt_text)

            log_tweet(json.dumps(post, indent=2))

            resp = requests.post(self.pds + "/xrpc/com.atproto.repo.createRecord",
                                 headers={"Authorization": "Bearer " + session["accessJwt"]},
                                 json={
                                     "repo": session["did"],
                                     "collection": "app.bsky.feed.post",
                                     "record": post,
                                 },
                                 )

            log_tweet(json.dumps(resp.json(), indent=2))
            resp.raise_for_status()
            if i == 0:
                root = resp.json()
                parent = resp.json()
            else:
                parent = resp.json()

def main(today):
    load_dotenv()
    timer = Timer()
    timer.printer('\nScript initiated at ' + datetime.now().strftime("%H:%M:%S, %d %B, %Y"))

    # Frost
    city = 'Tromsø'
    city_id = 'SN90450'
    t1 = '09:00:00'  # time of reported snow thickness in UTC (coincide with snow_coverage_type)

    # Download datasets
    downloader = WeatherDataDownloader(city_id, t1, today, timer)
    downloader.initiate_downloads()

    # Do monthly tweet if first of the month
    processor_max_monthly_historic = DataProcessor(downloader.get_dataset_options()['max_historic']['filename'], today, t1, timer)
    max_monthly_historic_df = processor_max_monthly_historic.load_data()
    processor_max_daily = DataProcessor(downloader.get_dataset_options()['max_daily']['filename'], today, t1, timer)
    max_daily_df = processor_max_daily.load_data()
    monthly_composer = MonthlyTweetComposer(today, city, timer)

    # Create the monthly summary texts about the previous month
    summaries = []
    if today.day == 1:
        last_month_tweet_snowpack = monthly_composer.compile_monthly_summary_snowpack(max_monthly_historic_df)
        last_month_tweet_snowfall = monthly_composer.compile_monthly_summary_snowfall(max_daily_df)
        summaries = [last_month_tweet_snowpack, last_month_tweet_snowfall]

    # Process today's data first
    processor = DataProcessor(downloader.get_dataset_options()['today']['filename'], today, t1, timer)

    # return df of snow conditions today
    today_summary = processor.today_summary()

    # Count when there was last snow
    processor_lsf = DataProcessor(downloader.get_dataset_options()['coverage']['filename'], today, t1, timer)
    if today_summary['Snow coverage'] is None:
        snow_days_ago = processor_lsf.last_time_snow_alt(today_summary['Snow coverage'])
    else:
        snow_days_ago = processor.last_time_snow(downloader.get_dataset_options()['coverage']['filename'], True)


    # Initiate Twitter client
    twit_client = tweepy.Client(os.getenv("BEARER_TOKEN"), os.getenv("API_KEY"), os.getenv("API_SECRET"),
                                os.getenv("ACCESS_KEY"), os.getenv("ACCESS_SECRET"), return_type=dict)
    auth = tweepy.OAuth1UserHandler(os.getenv("API_KEY"), os.getenv("API_SECRET"), os.getenv("ACCESS_KEY"),
                                    os.getenv("ACCESS_SECRET"))
    api = tweepy.API(auth)

    # Compose and print the tweet
    composer = TweetComposer(today_summary, today, city, api, twit_client, timer)
    snow_free = composer.is_snow_free()
    timer.printer(f'Snow free: {snow_free}')

    # Don't tweet if consecutive days of no snow
    if snow_free and snow_days_ago > 1:
        if debug is True:
            timer.printer(f"{city} has been snow-free for {snow_days_ago} days - no tweet made")
        else:
            log_tweet(f"Daily tweet not sent - {city} is snow-free.")
        exit()

    # Check if snow has arrived back... (relevant at start of season)
    returned = composer.snow_arrived_back(snow_days_ago)

    # Get the summary of today's snow cover
    snow_today = composer.summarize_today()

    # The last time it was snow-free was... (usually relevant at end of season)
    if snow_free:
        snow_free_days_ago = processor.last_time_snow(downloader.get_dataset_options()['coverage']['filename'], False)
        snow_free_text = composer.snow_free_days(snow_free_days_ago)
    else:
        snow_free_text = None

    # The last recorded snowfall was x days ago
    last_snow = composer.last_snowfall(max_daily_df)

    # The last time it snowed that much
    last_record = composer.that_much(max_daily_df)

    # It has snowed x times on this date...
    processor_max_monthly = DataProcessor(downloader.get_dataset_options()['max_this_month']['filename'], today, t1, timer)
    max_this_month = processor_max_monthly.max_monthly_snow()
    historic_deep = composer.last_deeper(max_this_month, max_monthly_historic_df)
    on_this_date = composer.on_this_date(max_daily_df)

    # Thickness of the snow on this day last year
    processor_last_year = DataProcessor(downloader.get_dataset_options()['this_day_last_year']['filename'], today, t1, timer)
    snow_thickness_last_year = processor_last_year.thickness_last_year()
    on_this_day = composer.last_year(snow_thickness_last_year)

    # Append an extra message on special days
    special_day = composer.check_special_days()

    # Compile daily tweet and check it's under the character limit
    tweet_texts = [special_day, returned, snow_today, snow_free_text, last_snow, last_record, historic_deep, on_this_date, on_this_day]
    tweet = composer.construct_tweet_text(tweet_texts)
    timer.printer(tweet)

    if debug is False:
        bsky = Skeet(today, city)

        # do monthly tweet first
        if today.day == 1 and summaries:
            composer.send_summary_tweets(summaries)
            bsky.send_skeet(summaries, None)
        if tweet is not None:
            graph = composer.is_graph_needed()
            composer.send_tweet(tweet, graph)
            bsky.send_skeet([tweet], graph)
    else:
        timer.printer('Debug mode on - tweets not posted.')

    timer.printer('Finished at ' + datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    debug = True  # to publish posts or not
    if debug is True:
        start_date = date(2024, 12, 19)
        end_date = date(2024, 12, 19)
        delta = timedelta(days=1)

        while start_date <= end_date:
            print('\n', start_date)
            main(start_date)
            start_date += delta
    else:
        start_date = date.today()
        main(start_date)
