#!/usr/bin/env python3

import sys
import os
import tweepy
from datetime import datetime

# global parameters
out_file_name = ''
log_folder = 'log'
log_file_name = 'GeoTweetsCollecter.log'


class GeoTweetsCollecter(tweepy.StreamListener):
    '''
    handler for the tweets stream
    '''

    def on_status(self, status):
        row = scrape(status)
        if row != '':
            print(row)
            save(row)

    def on_error(self, status_code):
        error = 'Error: status code ({})'.format(status_code)
        print(error)
        log(error)
        return True  # not block in case of errors

    def on_timeout(self):
        timeout = 'Timeout...'
        print(timeout)
        log(timeout)
        return True  # not block in case of timeouts


def save(row):
    '''
    appends the new tweet to the end of the dataset
    '''

    header = 'id_tweet\tid_user\tcreation_date\tcoordinates\tlang\ttext\n'
    if not os.path.exists(out_file_name):
        with open(out_file_name, 'a') as f:
            f.write(header)

    with open(out_file_name, 'a') as f:
        f.write(row)


def log(s):
    '''
    append a given string to the end of the log file
    '''

    global log_folder
    global log_file_name

    log_folder_path = os.path.abspath(log_folder)
    if not os.path.isdir(log_folder_path):
        os.mkdir(log_folder_path)

    log_file_path = os.path.join(log_folder_path, log_file_name)

    if not os.path.exists(log_file_path):
        with open(log_file_path, 'a') as f:
            f.write('date\tlog\n')

    iso_date = datetime.now().isoformat()
    with open(log_file_path, 'a') as f:
        f.write("{}\t{}\n".format(iso_date, s))


def set_parameters(conf_file):
    '''
    get parameters from the configuration file
    '''

    # get path to configuration file
    conf_file = os.path.abspath(conf_file)

    # check if conf_file exist
    if not os.path.exists(conf_file):
        print('error: "{}" not exist'.format(conf_file))
        sys.exit()

    with open(conf_file, 'r') as f:
        pars = f.read().split('\n')

    parameters = {}
    for par in pars:
        if '=' in par:
            tag, value = par.split('=')
            tag = tag.strip()
            value = value.strip()

            if tag == 'OUT_FILE':
                parameters['out_file'] = value
            elif tag == 'RETRY_DELAY':
                parameters['retry_delay'] = float(value)
            elif tag == 'TIMEOUT':
                parameters['timeout'] = float(value)
            elif tag == 'COORDINATES':
                lat1, lon1, lat2, lon2 = value.split(',')
                parameters['coordinates'] = [float(lat1),
                                             float(lon1),
                                             float(lat2),
                                             float(lon2)]
            else:
                print('error: parameter "{}" not recognized'.format(tag))
                sys.exit(main(sys.argv))

    # checksum
    if len(parameters) != 4:
        print('error: some parameter is missing')
        sys.exit()

    return parameters


def authenticate(conf_file):
        '''
        get the twitter API tokens from the configuration file and authenticate
        on twitter api
        '''

        # get path to configuration file
        conf_file = os.path.abspath(conf_file)

        # check if conf_file exist
        if not os.path.exists(conf_file):
            print('error: "{}" not exist'.format(conf_file))
            sys.exit(main(sys.argv))

        with open(conf_file, 'r') as f:
            pars = f.read().split('\n')

        parameters = {}
        for par in pars:
            if '=' in par:
                tag, value = par.split('=')
                tag = tag.strip()
                value = value.strip()

                if tag == 'CONSUMER_KEY':
                    parameters['consumer_key'] = value
                elif tag == 'CONSUMER_SECRET':
                    parameters['consumer_secret'] = value
                elif tag == 'ACCESS_TOKEN':
                    parameters['access_token'] = value
                elif tag == 'ACCESS_TOKEN_SECRET':
                    parameters['access_token_secret'] = value
                else:
                    print('error: parameter "{}" not recognized'.format(tag))
                    sys.exit()

        # check if all parameters were captured, if not return error
        if len(parameters) != 4:
            print('error: some parameter is missing')
            sys.exit(main(sys.argv))

        # Authentication for twitter api
        auth = tweepy.OAuthHandler(parameters['consumer_key'],
                                   parameters['consumer_secret'])
        auth.set_access_token(parameters['access_token'],
                              parameters['access_token_secret'])

        return auth


def sanitize(s):
    '''
    remove tabs and carriage return
    '''
    while '\t' in s or '\n' in s:
        s = s.replace('\t', ' ').replace('\n', ' ')

    return s


def scrape(raw):
    '''
    take in unput the raw json comes from the stream and return a line that
    represent the tweet where each field is separated by a tab
    '''
    tweet = raw._json

    # if tweet is not geotagged with a precise coordinate return an empty str
    if tweet['coordinates'] is None:
        return ''

    # indexes are inverted to have coordinates in the form [lat, lon]
    coordinates = '{},{}'.format(tweet['coordinates']['coordinates'][1],
                                 tweet['coordinates']['coordinates'][0])

    # get tweet language if present
    lang = 'und'
    if 'lang' in tweet:
        lang = tweet['lang']

    text = get_text(tweet)

    return '{}\t{}\t{}\t{}\t{}\t{}\n'.format(tweet['id'],
                                             tweet['user']['id'],
                                             iso_date(tweet['created_at']),
                                             coordinates,
                                             lang,
                                             sanitize(text)
                                             )


def get_text(tweet):
    '''
    extract text from the tweet. If the tweet is extendend tweet, it takes
    the non-truncated field
    '''

    if 'retweeted_status' in tweet:
        if 'extended_tweet' in tweet['retweeted_status']:
            return tweet['retweeted_status']['extended_tweet']['full_text']
        else:
            return tweet['retweeted_status']['text']
    else:
        if 'extended_tweet' in tweet:
            return tweet['extended_tweet']['full_text']
        else:
            return tweet['text']


def iso_date(date):
    return str(datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y').isoformat())


def main(args):

    '''
    # args parameters
    conf_auth_file = args[1]
    conf_parameters_file = args[2]
    '''

    while True:
        try:
            home = os.path.abspath(os.path.dirname(sys.argv[0]))
            conf_auth_file = os.path.join(home, 'confs/auth.conf')
            conf_parameters_file = os.path.join(home, 'confs/parameters.conf')

            # WARNING : use of global variables
            global out_file_name

            pars = set_parameters(conf_parameters_file)

            # set outfile path
            out_file_name = os.path.abspath(pars['out_file'])

            auth = authenticate(conf_auth_file)

            # Instantiate the api object
            api = tweepy.API(auth, wait_on_rate_limit=True,
                             wait_on_rate_limit_notify=True,
                             retry_delay=pars['retry_delay'],
                             retry_errors=True,
                             timeout=pars['timeout'])

            # instantiate the listener and start the stream
            collecter = GeoTweetsCollecter()
            stream = tweepy.Stream(auth=api.auth, listener=collecter)

            stream.filter(locations=pars['coordinates'])
        except Exception as e:
            log('Connection error')
            continue


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except (KeyboardInterrupt, SystemExit):
        print('\nBye...')
