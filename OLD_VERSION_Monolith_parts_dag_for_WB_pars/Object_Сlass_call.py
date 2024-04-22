from os import environ

from OLD_VERSION_parts_dag_for_WB_pars.Extract_from_url_OOP import WebSearch_Transform

web_search = WebSearch_Transform(config_file_name='Config.ini',
                                config_tag='Default')

'''
To load your json use

dict_tag = WebSearch_Transform(config_file_name='Config.ini',
                                config_tag='LoadYour_WB_Json').config

json_result = web_search.Load_Custom_Json(
                    full_path_file=dict_tag['path_file'],
                                        products_path_args=['data', 'products'])

extracted_data = web_search.extract_to_CSV(json_result,
                                        args_extract=['id', 'priceU', 'feedbacks'],
                                        date='YOUR_DATE (Example: 2024-02-05)')

'''

json_result = web_search.wb_request(web_search.config,
                                    products_path_args=['data', 'products'])

extracted_data = web_search.extract_to_CSV(json_result,
                                        args_extract=['id', 'priceU', 'feedbacks'])

names_cloud = WebSearch_Transform(config_file_name='Config.ini',
                                config_tag='GoogleCloud').config

# Initialize the BigQuery client outside of the task to avoid repeated initializations
environ['GOOGLE_APPLICATION_CREDENTIALS'] = names_cloud.pop('path_json')

names_cloud['data'] = extracted_data
