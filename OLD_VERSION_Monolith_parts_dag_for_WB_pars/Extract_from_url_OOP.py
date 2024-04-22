class WebSearch_Transform:
    def __init__(self, config_file_name: str, config_tag: str) -> None:
        self.config = self._load_config(
                                        self._get_path(config_file_name),
                                        config_tag)


    def _get_path(self, config_file_name: str) -> str:
        """Get path for parts_dag folder use name and os.path

        Args:
            config_file_name (str): full name file with format(.ini)

        Returns:
            str: full path file
        """
        from os import path

        return path.join(path.abspath(path.dirname(__file__)), config_file_name)


    def _load_config(self, file_name: str, config_tag: str) -> dict:
        """From Config ini extract data for url 'https://search.wb.ru/exactmatch' site
        Args:
            file_name (str): Name Config.ini file
            config_tag (str): Tag in Config.ini file
        Returns:
            Dict: Config to RAM dict use python
        """
        from configparser import ConfigParser

        config_parser = ConfigParser()
        config_parser.read(file_name, encoding="utf-8")
        return \
        {
            i: config_parser[config_tag][i]
            for i in config_parser[config_tag]
        }


    def wb_request(self,
                    arg_for_query: dict,
                    products_path_args: list[str]) -> list[dict]:
        """Json retrieval function via a request
        to the 'https://search.wb.ru/exactmatch' site
        Args:
            arg_for_query (dict[str]): Output function MyConfig
        Returns:
            list[dict]: Products from JSON query
        """
        try:
            from requests import get
            res = get(arg_for_query.pop('base'),
                    params=arg_for_query).json()
            [res:=res[arg] for arg in products_path_args]
            return res
        except Exception as e:
            print("Error fetching data from the website"
                "(wb_request) 'https://search.wb.ru/exactmatch'\n", e, '\n')
            return None


    def extract_to_CSV(self, json_data: list[dict],
                        args_extract: list[str],
                        date: str = None) -> str:
        """Extraction of required information from Json
        Args:
            json_data (list[dict]): Output function wb_request
            args_extract (list[str]): Arguments in json to be pulled out
            date (str): Date column for loading
                        old JSON['data']['products'] files
        Returns:
            list[dict]: Formatted CSV according
            to values (args_extract) in StringIO class
        """
        try:
            from io import StringIO

            if date is None:
                from pendulum import now
                date = now(tz='Europe/Moscow').strftime('%Y-%m-%d')

            name_columns = 'date' + '|' + '|'.join(args_extract) + '\n'
            string_data = name_columns + \
            '\n'.join(
                [
                    date + '|' +
                    '|'.join(
                        map(str, (item[arg] for arg in args_extract))
                    )
                    for item in json_data
                ]
            )
            string_buffer = StringIO(string_data)
            return string_buffer
        except Exception as E:
            print("Error arguments not found (extract_json_wb):",
                json_data[0].keys(), E, sep='\n')
            return None


    def Load_Custom_Json(self, full_path_file: str,
                        products_path_args: list[str]) -> list[dict]:
        """Extract value from JSON used Products path args

        Args:
            full_path_file (str): path to json file
            products_path_args (list[str]): List key from JSON

        Returns:
            list[dict]: Extract value by key path JSON
        """
        from json import load
        with open(full_path_file, 'r') as f:
            data = load(f)
            [data:=data[arg] for arg in products_path_args]
            return data
