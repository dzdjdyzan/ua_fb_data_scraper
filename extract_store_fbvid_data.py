from collections import deque
import logging, sys, facebook, pymongo, datetime, os, requests, time, pickle, shutil, configparser

# Create a ConfigParser object and read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

acc_token = config.get('Facebook', 'access_token')
fields = config.get('Facebook', 'fields')

#get current date and time, the date will be used to name the db since the data will be pulled daily, one table per day
current_time = datetime.datetime.now()

# Calculate the date of the previous day
previous_day = current_time - datetime.timedelta(days=1)

db_date_prefix = ''.join(map(str, str(previous_day).split()[0].split('-')))

previous_day_str = previous_day.strftime("%Y-%m-%d")

timestr = ''.join(filter(str.isdigit, str(current_time).split()[1]))

log_dtstamp = db_date_prefix + timestr

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a file handler and set the log file name
current_working_dir = os.getcwd()
log_folder = 'extractorlogs'
log_file_path = os.path.join(current_working_dir, log_folder)
log_file = os.path.join(log_file_path, log_dtstamp + 'extract_and_store_fbvideodata.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

# Create a console handler for printing logs to the console
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

#get all raw video data from facebook page
def get_all_connections(graph, page_id, fields, state_obj, stobj_path):

    after = state_obj.get('after')

    while True:
        try:
            params = {'limit': 40, 'after': after, 'fields': fields}
            response = graph.get_connections(page_id, 'videos', **params)

            data = response.get('data', [])
            
            # Process the retrieved data
            state_obj['videos_to_add'].extend(data)

            paging = response.get('paging', {})
            previous_url = paging.get('previous')

            if not previous_url:
                logger.info(f'All video data from the FB page with ID {page_id} has been processed')
                break

            # Check if the retrieved data is from the previous day
            if previous_day_str not in previous_url:
                logger.info(f'All video data from the FB page with ID {page_id} has been processed until the previous day')
                break

            # Extract the 'until' parameter value from the previous URL
            until_index = previous_url.index('until=')
            until_value = previous_url[until_index + len('until='):]

            # Update the 'after' value for the next request
            after = until_value

            time.sleep(1)

        except facebook.GraphAPIError as e:
            if e.code == 4:
                serialize_object(state_obj, stobj_path)
                logger.error("FB Graph API rate limit reached. Try again later.")
                sys.exit()
            else:
                raise e

    # Serialize the state object before exiting the function
    serialize_object(state_obj, stobj_path)

    
# Function to increment the counter and check if the Facebook API call limit is exceeded
def inc_and_check_if_fbapi_calls_exceeded(counter):

    counter += 1
    if counter >= 200:
        raise Exception("Exceeded Facebook API call limit!")
    return counter

# Function to validate the video status
def validate_video_status(status):

    video_status = status.get('video_status')
    uploading_phase = status.get('uploading_phase')
    processing_phase = status.get('processing_phase')
    publishing_phase = status.get('publishing_phase')

    if video_status is None or uploading_phase is None or processing_phase is None or publishing_phase is None:
        return False

    if video_status != 'ready':
        return False

    if uploading_phase.get('status') is None or uploading_phase.get('status') != 'complete' or processing_phase.get('status') is None or processing_phase.get('status') != 'complete' or publishing_phase.get('status') is None or publishing_phase.get('status') != 'complete':
        return False
    
    if publishing_phase.get('publish_status') is None or publishing_phase.get('publish_status') != 'published' or publishing_phase.get('publish_time') is None:
        return False

    return True

#save script data to file
def serialize_object(obj, file_path):
    try:
        serialized_data = pickle.dumps(obj)
        with open(file_path, 'wb') as file:
            file.write(serialized_data)
        logger.info("Serialization successful.")
    except (pickle.PickleError, IOError) as e:
        logger.error(f"Serialization failed. Error: {e}")

#load script data from file
def deserialize_object(file_path):
    try:
        with open(file_path, 'rb') as file:
            serialized_data = file.read()
        deserialized_object = pickle.loads(serialized_data)
        logger.info("Deserialization successful.")
        return deserialized_object
    except (pickle.PickleError, IOError) as e:
        logger.error(f"Deserialization failed. Error: {e}")

#connect to mongo client
try:
    client = pymongo.MongoClient("MONGODATA")
    # Perform operations on the database
    # ...
    logger.info("Connected to MongoDB successfully!")

except ConnectionError as ce:
    logger.error(f"Database connection error occurred: {ce}")
    logger.error("Application terminating. Try again.")
    sys.exit()

except ServerSelectionTimeoutError as se:
    logger.error(f"Server selection timeout error occurred: {se}")
    logger.error("Application terminating. Try again.")
    sys.exit()

except Exception as e:
    logger.error(f"An error occurred: {e}")
    logger.error("Application terminating. Try again.")
    sys.exit()


'''

PROJECT "INTRODUCE STATE"

This script currently has roughly the following logic:

1) get list of facebook users from the database
2) for each user get all the videos associated with their facebook page and push to a daily database

The following is needed so that this script can resume pulling video data from facebook and pushing it to a database
at any point of execution:

-path to a file which is created by the script upon launch (or opened if the script has not completed and the state is still
 needed) in order to serialize objects containing program state data
-path to a folder which stores previous serializations of the program)

-The date/time when the script began
-name of the database
-the name of the ua_fb_pages_table
-the name of the ua_fb_videos_table
-the name of the ua_fb_reels_table

***serialize the db? chatgpt suggest serializing the collection of the db not the whole db

1) get a list of facebook users from the database

    a) queue of facebook users/pages still to be processed (FIFO)
    b) the facebook page currently being processed (id)
    c) queue of facebook users/pages that have been processed (FIFO)

2) get a list of videos for that facebook user using get_all_connections from the fb graph api

    a) queue of facebook videos still to be processed (FIFO)
    b) the facebook video currently being processed (id)
    c) queue of facebook videos that have been processed (FIFO)


'''

script_state = {}

#SERIALIZATION PATHS

serial_folder = 'script_data'
serial_folder_path = os.path.join(current_working_dir, serial_folder)
os.makedirs(serial_folder_path, exist_ok=True)

curr_serial_folder = 'in_progress_data'
curr_serial_folder_path = os.path.join(serial_folder_path, curr_serial_folder)
os.makedirs(curr_serial_folder_path, exist_ok=True)

comp_serial_folder = 'completed_data'
comp_serial_folder_path = os.path.join(serial_folder_path, comp_serial_folder)
os.makedirs(comp_serial_folder_path, exist_ok=True)

script_data_file = db_date_prefix + 'script_data.pkl'
script_data_file_path = os.path.join(curr_serial_folder_path, script_data_file)

if os.path.exists(script_data_file_path):
    #DESERIALIZE THE DATA AT KEY MOMENTS
    script_state = deserialize_object(script_data_file_path)
    logger.info("desrialized previously created data")

else:
    #SERIALIZE DATA AT KEY MOMENTS
    script_state = {
        
        'start_time': str(current_time),
        'end_time': None,
        
        'status': 'in progress',
        
        'db_name': 'UACluster',
        'page_table_name': 'ua_fb_pages_table',
        'video_table_name': db_date_prefix + 'ua_fb_videos_table',
        'reel_table_name': db_date_prefix + 'ua_fb_reels_table',
        
        'pages_to_process': deque(),
        'current_page': None,
        'pages_processed_success': deque(),
        'pages_processed_fail': deque(),
        
        'videos_to_add': deque(),
        'current_video': None,
        'videos_processed_success': deque(),
        'videos_processed_fail': deque()
    }


ua_db = client[script_state['db_name']]

#db tables
ua_fb_pages_table = ua_db[script_state['page_table_name']]
ua_fb_videos_table = ua_db[script_state['video_table_name']]
ua_fb_reels_table = ua_db[script_state['reel_table_name']]


# Initialize Facebook Graph API

try:
    graph = facebook.GraphAPI(access_token=acc_token)
    logger.info("Facebook API graph recieved!")
except requests.exceptions.RequestException as re:
    # Handle internet disconnection error
    logger.error(f"Internet connection lost. Please check your network. Error: {re}")
    logger.error("Application terminating. Try again.")
    sys.exit()

except Exception as e:
    # Handle other exceptions or application/system termination error
    logger.error(f"An unexpected error occurred. Please try again. Error: {e}")
    logger.error("Application terminating. Try again.")
    sys.exit()


try:
    page_ids = [page['page_id'] for page in ua_fb_pages_table.find({}, {'page_id': 1})]
    script_state['pages_to_process'] = deque(page_ids)
except pymongo.errors.ServerSelectionTimeoutError as se:
    # Handle database disconnection error
    logger.error(f"Database disconnection error occurred while fetching fb users. Error: {se}")
    logger.error("Application terminating. Try again")
    sys.exit()
except pymongo.errors.ConnectionFailure as ce:
    # Handle internet disconnection error
    logger.error(f"Internet disconnection error occurred while fetching fb users. Error: {ce}")
    logger.error("Application terminating. Try again")
    sys.exit()
except Exception as e:
    # Handle other application/system termination errors
    logger.error(f"An error occurred during execution while fetching fb users. Error: Error: {e}")
    logger.error("Application terminating. Try again")
    sys.exit()
    


log_file = os.path.join(log_file_path, log_dtstamp + 'extract_and_store_fbvideodata.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Loop over each page ID
while script_state['pages_to_process']:

    try:
        if script_state['current_page'] is None:
            script_state['current_page'] = script_state['pages_to_process'].popleft()

        get_all_connections(graph, script_state['current_page'], fields, script_state, script_data_file_path)

        logger.info(f"Successfully retrieved video data from page with id: {script_state['current_page']}")


    except (requests.exceptions.RequestException, facebook.GraphAPIError, Exception) as e:
        script_state['pages_processed_fail'].append(script_state['current_page'])
        serialize_object(script_state, script_data_file_path)
        if isinstance(e, requests.exceptions.RequestException):
            # Handle internet disconnection error
            logger.error(f"Internet connection lost. Please check your network. Error: {e}")
            sys.exit()
        elif isinstance(e, facebook.GraphAPIError):
            logger.error(f"Facebook GraphAPIError occurred for page ID {script_state['current_page']}: {e}")
            continue
        else:
            # Handle other exceptions or application/system termination error
            logger.error(f"An unexpected error occurred. Please try again. Error: {e}")
            sys.exit()

    while script_state['videos_to_add']:

        if script_state['current_video'] is None:
            script_state['current_video'] = script_state['videos_to_add'].popleft()
        video_data = script_state['current_video']

        video_data_row = {}
        page_id = script_state['current_page']
        #get video id
        video_id = video_data.get('id')
        #validate video
        if video_id is None:
            script_state['videos_processed_fail'].append(script_state['current_video'])
            #script_state['last_processed_video'] = { video_id : page_id }
            script_state['current_video'] = None
            logger.error(f'Invalid video data has been encountered. Skipping this video with page id: {page_id}')
            continue
        else:
            video_data_row['id'] = video_id

        status = video_data.get('status')
        if status is None:
            script_state['videos_processed_fail'].append(video_id)
            #script_state['last_processed_video'] = { video_id : page_id }
            script_state['current_video'] = None
            logger.error(f'Invalid video status data has been encountered. Skipping this video with page id: {page_id} and video id: {video_id}')
            continue

        if not validate_video_status(status):
            script_state['videos_processed_fail'].append(video_id)
            #script_state['last_processed_video'] = { video_id : page_id }
            script_state['current_video'] = None
            logger.error(f'Video status must be ready and the video must be published. Skipping this video with page id: {page_id} and video id: {video_id}')
            continue

        #CHECK WHETHER THE VIDEO IS A REGULAR FB VIDEO OR AN FB REEL AND BASED ON THIS ADD THE DATA TO AN APPROPRIATE TABLE IN THE DB
        #if video, reel, or other/permalink url for some reason not found
        permalink_url = video_data.get('permalink_url')
        if permalink_url is not None:

            title = video_data.get('title')
            if title is None:
                video_data_row['title'] = 'unknown'
                logger.error(f'Unknown video title for video with page id: {page_id} and video id: {video_id}')
            else:
                video_data_row['title'] = title

            description = video_data.get('description')
            if description is None:
                video_data_row['description'] = 'unknown'
                logger.error(f'Unknown video description for video with page id: {page_id} and video id: {video_id}')
            else:
                video_data_row['description'] = description

            # from data contains subdata: id and name of parent page
            origin = video_data.get('from')
            if origin is None:
                script_state['videos_processed_fail'].append(video_id)
                #script_state['last_processed_video'] = { video_id : page_id }
                script_state['current_video'] = None
                logger.error(f'No originator data found. Skipping this video with page id: {page_id} and video id: {video_id}')
                continue

            parent_page_id = origin.get('id')
            if parent_page_id is None:
                script_state['videos_processed_fail'].append(video_id)
                #script_state['last_processed_video'] = { video_id : page_id }
                script_state['current_video'] = None
                logger.error(f'No originator id found. Skipping this video with page id: {page_id} and video id: {video_id}')
                continue
            else:
                video_data_row['parent_page_id'] = parent_page_id

            length = video_data.get('length')
            if length is None:
                script_state['videos_processed_fail'].append(video_id)
                #script_state['last_processed_video'] = { video_id : page_id }
                script_state['current_video'] = None
                logger.error(f'No video length found. Skipping this video with page id: {page_id} and video id: {video_id}')
                continue
            else:
                video_data_row['length'] = length


            publish_time = status.get('publishing_phase').get('publish_time')
            video_data_row['time_published'] = publish_time


            universal_video_id = video_data.get('universal_video_id')
            if universal_video_id is None:
                video_data_row['universal_video_id'] = 'unknown'
                logger.error(f'Unknown universal video id, none found for video with page id: {page_id} and video id: {video_id}')
            else:
                video_data_row['universal_video_id'] = universal_video_id


            views = video_data.get('views')
            if views is None:
                script_state['videos_processed_fail'].append(video_id)
                #script_state['last_processed_video'] = { video_id : page_id }
                script_state['current_video'] = None
                logger.error(f'No viewcount data found. Skipping this video with page id: {page_id} and video id: {video_id}')
                continue
            else:
                video_data_row['views'] = views


            video_insights = video_data.get('video_insights')
            if video_insights is None:
                script_state['videos_processed_fail'].append(video_id)
                #script_state['last_processed_video'] = { video_id : page_id }
                script_state['current_video'] = None
                logger.error(f'No video insights data found. Skipping this video with page id: {page_id} and video id: {video_id}')
                continue

            video_insights_data = video_insights.get('data')
            if video_insights_data is None:
                script_state['videos_processed_fail'].append(video_id)
                #script_state['last_processed_video'] = { video_id : page_id }
                script_state['current_video'] = None
                logger.error(f'No inner video insights data found. Skipping this video with page id: {page_id} and video id: {video_id}')
                continue

            #if the video is a regular video
            if 'video' in permalink_url:
                
                video_insights_keyset = [
                    'total_video_impressions', 
                    'total_video_views', 
                    'total_video_60s_excludes_shorter_views', 
                    'total_video_30s_views_unique', 
                    'total_video_view_total_time', 
                    'total_video_avg_time_watched'
                ]

                for insight in video_insights_data:
                    name = insight.get('name')
                    if name in video_insights_keyset:
                        video_data_row[name] = insight['values'][0]['value']

                video_data_row['time_added_to_db'] = datetime.datetime.now()


                try:
                    ua_fb_videos_table.insert_one(video_data_row).inserted_id
                    script_state['videos_processed_success'].append(video_id)
                except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.ConnectionFailure, Exception) as e:
                    if isinstance(e, pymongo.errors.ServerSelectionTimeoutError):
                        # Handle database disconnection error
                        logger.error(f"Database disconnection error occurred. Could not add video with id: {video_id} and page id: {page_id}")
                        logger.error("Error: " + str(e))
                    elif isinstance(e, pymongo.errors.ConnectionFailure):
                        # Handle internet disconnection error
                        logger.error(f"Internet disconnection error occurred. Could not add video with id: {video_id} and page id: {page_id}")
                    else:
                        # Handle other application/system termination errors
                        logger.error(f"An error occurred during execution.")
                    serialize_object(script_state, script_data_file_path)
                    sys.exit()

            #if the video is a reel
            elif 'reel' in permalink_url:

                video_insights_keyset = [
                    'post_video_avg_time_watched', 
                    'post_video_view_time', 
                    'post_impressions_unique', 
                    'blue_reels_play_count'
                ]

                for insight in video_insights_data:
                    name = insight.get('name')
                    if name in video_insights_keyset:
                        video_data_row[name] = insight['values'][0]['value']

                video_data_row['time_added_to_db'] = datetime.datetime.now()


                try:
                    ua_fb_reels_table.insert_one(video_data_row).inserted_id
                    script_state['videos_processed_success'].append(video_id)
                except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.ConnectionFailure, Exception) as e:
                    if isinstance(e, pymongo.errors.ServerSelectionTimeoutError):
                        # Handle database disconnection error
                        logger.error(f"Database disconnection error occurred. Could not add reel with id: {video_id} and page id: {page_id}")
                        logger.error("Error: " + str(e))
                    elif isinstance(e, pymongo.errors.ConnectionFailure):
                        # Handle internet disconnection error
                        logger.error(f"Internet disconnection error occurred. Could not add reel with id: {video_id} and page id: {page_id}")
                    else:
                        # Handle other application/system termination errors
                        logger.error(f"An error occurred during execution.")
                    serialize_object(script_state, script_data_file_path)
                    sys.exit()

            else:
                logger.error(f'Video is not a recognizeable type. Must be either an fb video or an fb reel. Skipping this video with id: {video_id} and {page_id}')
                script_state['videos_processed_fail'].append(video_id)
            
        else:
            script_state['videos_processed_fail'].append(video_id)
            logger.error(f'Video status must have permalink_url. Skipping this video with page id: {page_id} and video id: {video_id}')
        
        #script_state['last_processed_video'] = { video_id : page_id }
        script_state['current_video'] = None

    script_state['current_page'] = None



script_state['end_time'] = str(datetime.datetime.now())
script_state['status']
serialize_object(script_state, script_data_file_path)

shutil.move(script_data_file_path, comp_serial_folder_path)

logger.info('Script complete.')
