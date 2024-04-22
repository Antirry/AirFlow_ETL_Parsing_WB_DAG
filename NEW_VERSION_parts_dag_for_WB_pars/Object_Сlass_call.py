from os import environ

from NEW_VERSION_parts_dag_for_WB_pars.Extract_from_url_OOP import WebSearch_Transform
# from Extract_from_url_OOP import WebSearch_Transform
import pickle

def write_result(values, file_name: str ='Task_Result.pkl') -> None:
    from pathlib import Path
    folder_path = Path(file_name[:-4])
    folder_path.mkdir(parents=True, exist_ok=True)

    # Добавление файла
    file_path = folder_path / file_name

    with file_path.open(mode="wb") as pkl_file:
        pickle.dump(values, pkl_file)


def read_result(file_name: str ='Task_Result.pkl') -> None:
    from pathlib import Path
    folder_path = Path(file_name[:-4])

    file_path = folder_path / file_name

    with file_path.open(mode="rb") as pkl_file:
        return pickle.load(pkl_file)


def _get_data_API(*Name_Tag_Path_args: list[str, list[str]]):
    name_file, config_tag, products_path_args = Name_Tag_Path_args
    web_search = WebSearch_Transform(config_file_name=name_file,
                                    config_tag=config_tag)

    json_result = web_search.wb_request(web_search.config,
                                        products_path_args=products_path_args)

    # list[WebSearch_Transform, list[dict]]
    write_result([web_search, json_result])


def _convert_res_API(args_extract: list[str]) -> str:
    #  list[WebSearch_Transform, list[dict]]
    web_search, json_result = read_result()
    extracted_data = web_search.extract_to_CSV(json_result,
                                            args_extract=args_extract)

    write_result(extracted_data)


def _names_cloud(*args_get_API: list[str]):
    name_file, config_tag = args_get_API
    names_cloud = WebSearch_Transform(config_file_name=name_file,
                                    config_tag=config_tag).config

    # Initialize the BigQuery client outside
    # of the task to avoid repeated initializations
    environ['GOOGLE_APPLICATION_CREDENTIALS'] = names_cloud.pop('path_json')

    names_cloud['data'] = read_result()

    write_result(names_cloud)


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
