import re

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from tap_zoom.discover import discover
# from tap_zoom.endpoints import ENDPOINTS_CONFIG

import dateutil.parser

LOGGER = singer.get_logger()

def get_bookmark(state, stream_name, default):
    return state.get('bookmarks', {}).get(stream_name, default)

def write_bookmark(state, stream_name, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = value
    singer.write_state(state)

def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)

def sync_endpoint(client,
                  catalog,
                  state,
                  required_streams,
                  selected_streams,
                  stream_name,
                  endpoint,
                  key_bag):
    persist = endpoint.get('persist', True)

    if persist:
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        mdata = metadata.to_map(stream.metadata)
        write_schema(stream)

    path = endpoint['path'].format(**key_bag)

    page_size = 1000
    page_number = 1
    while True:
        params = {
            'page_size': page_size,
            'page_number': page_number
        }

        records = []
        if stream_name == 'meetings':
            params['type'] = "past"
            start = client.start_date.replace("Z", "")
            today = datetime.utcnow() + relativedelta(days=1)
            oldest_date_available = (today - relativedelta(months=6)).strftime("%Y-%m-%d")
            if start is None or dateutil.parser.parse(start) < dateutil.parser.parse(oldest_date_available):
                start = oldest_date_available

            start_dt = parse(start)
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day)
            while True:
                from_date = start_dt.strftime("%Y-%m-%d")
                to_date = start_dt + relativedelta(months=1) - timedelta(1)
                to_date = to_date.strftime("%Y-%m-%d")

                params['from'] = from_date
                params['to'] = to_date

                month_data = client.get(path,
                                params=params,
                                endpoint=stream_name,
                                ignore_zoom_error_codes=endpoint.get('ignore_zoom_error_codes', []),
                                ignore_http_error_codes=endpoint.get('ignore_http_error_codes', []))


                start_dt = start_dt + relativedelta(months=1)

                if month_data is None:
                    continue

                #Break the loop if limit has reached for the path
                if "ZOOM_LIMIT_REACHED" in month_data:
                    break
                
                if 'data_key' in endpoint:
                    records += month_data[endpoint['data_key']]
                else:
                    records += [month_data]
                
                if start_dt > datetime.utcnow():
                    break
        else: 
            data = client.get(path,
                            params=params,
                            endpoint=stream_name,
                            ignore_zoom_error_codes=endpoint.get('ignore_zoom_error_codes', []),
                            ignore_http_error_codes=endpoint.get('ignore_http_error_codes', []))
            
            if data is None:
                return
            #End sync for stream if limit is reached for the stream
            if "ZOOM_LIMIT_REACHED" in data:
                break
            
            if 'data_key' in endpoint:
                records = data[endpoint['data_key']]
            else:
                records = [data]

        with metrics.record_counter(stream_name) as counter:
            with Transformer() as transformer:
                for record in records:
                    if persist and stream_name in selected_streams:
                        record = {**record, **key_bag}
                        record_typed = transformer.transform(record,
                                                             schema,
                                                             mdata)
                        singer.write_record(stream_name, record_typed)
                        counter.increment()
                    if 'children' in endpoint:
                        child_key_bag = dict(key_bag)
                        if 'provides' in endpoint:
                            for dest_key, obj_key in endpoint['provides'].items():
                                child_key_bag[dest_key] = record[obj_key]
                        for child_stream_name, child_endpoint in endpoint['children'].items():
                            if child_stream_name in required_streams:
                                sync_endpoint(client,
                                              catalog,
                                              state,
                                              required_streams,
                                              selected_streams,
                                              child_stream_name,
                                              child_endpoint,
                                              child_key_bag)

        if endpoint.get('paginate', True) and page_number < data.get('page_count', 1):
            # each endpoint has a different max page size, the server will send the one that is forced
            page_size = data['page_size']
            page_number += 1
        else:
            break

def update_current_stream(state, stream_name=None):  
    set_currently_syncing(state, stream_name) 
    singer.write_state(state)

def get_required_streams(endpoints, selected_stream_names):
    required_streams = []
    for name, endpoint in endpoints.items():
        child_required_streams = None
        if 'children' in endpoint:
            child_required_streams = get_required_streams(endpoint['children'],
                                                          selected_stream_names)
        if name in selected_stream_names or child_required_streams:
            required_streams.append(name)
            if child_required_streams:
                required_streams += child_required_streams
    return required_streams

def sync(client, catalog, state):
    if not catalog:
        catalog = discover()
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    selected_stream_names = []
    for selected_stream in selected_streams:
        selected_stream_names.append(selected_stream.tap_stream_id)

    required_streams = get_required_streams(client.endpoints, selected_stream_names)

    for stream_name, endpoint in client.endpoints.items():
        if stream_name in required_streams:
            update_current_stream(state, stream_name)
            sync_endpoint(client,
                          catalog,
                          state,
                          required_streams,
                          selected_stream_names,
                          stream_name,
                          endpoint,
                          {})

    update_current_stream(state)
