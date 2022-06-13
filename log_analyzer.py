import argparse
import collections
import datetime
import gzip
import io
import json
import logging
import os
import pathlib
import re
import statistics
import string
from operator import attrgetter

DEFAULT_CONFIG_PATH = '.'
REPORT_TEMPLATE_PATH = '.'

DateNamedFileInfo = collections.namedtuple('DateNamedFileInfo', ['file_path', 'file_date'])

LOG_RECORD_RE = re.compile(
    '^'
    '\S+ '  # remote_addr
    '\S+\s+'  # remote_user (note: ends with double space)
    '\S+ '  # http_x_real_ip
    '\[\S+ \S+\] '  # time_local [datetime tz] i.e. [29/Jun/2017:10:46:03 +0300]
    '"\S+ (?P<href>\S+) \S+" '  # request "method href proto" i.e. "GET /api/v2/banner/23815685 HTTP/1.1"
    '\d+ '  # status
    '\d+ '  # body_bytes_sent
    '"\S+" '  # http_referer
    '".*" '  # http_user_agent
    '"\S+" '  # http_x_forwarded_for
    '"\S+" '  # http_X_REQUEST_ID
    '"\S+" '  # http_X_RB_USER
    '(?P<time>\d+\.\d+)'  # request_time
)

default_config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "LOG_FILE": None,
    "ERRORS_LIMIT": 0.01,
}


def setup_logging(logfile):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
        filename=logfile,
        force=True
    )


def load_config(path, default_config):
    logging.info('Загрузка конфиг файла.')
    if path == '.' or not path:
        return default_config

    path = pathlib.Path(path)
    if not path.exists() or not path.is_file():
        logging.error("Конфиг файл не найден.")
        raise FileNotFoundError("Заданный конфигурационный файл не найден!")

    with path.open() as f:
        config = json.load(f)

    return {**default_config, **config}


def is_gzip_file(file_path):
    return file_path.split('.')[-1] == 'gz'


def process_line(line, patterns):
    """
    Поиск в строке текста REGEX выражений.
    :param line: исходная строка текста
    :param patterns: скомпилированные REGEX паттерны
    :return: адрес хоста и время запроса
    """
    match = patterns.match(line)
    if not match:
        return None

    log_line = match.groupdict()
    try:
        url = log_line['href']
        request_time = round(float(log_line['time']), 3)
    except (ValueError, TypeError):
        logging.error("Ошибка при чтении строки лога.")
        return None
    else:
        return url, request_time


def get_latest_log_info(files_dir):
    """
    Поиск последнего по дате лог-файла из директории по маске REGEX.
    :param files_dir: директория для лог-файлов
    :return: дата последнего лог-файла из директории
    """
    logging.info('Поиск последнего файла с логами.')
    if not os.path.isdir(files_dir):
        return None

    result = []
    pattern = re.compile(r'nginx-access-ui\.log-\D*(?P<date>\d{8})\D*\.(gz|log|txt)$')
    for filename in os.listdir(files_dir):
        match = pattern.search(filename)
        if match:
            try:
                file_date = datetime.datetime.strptime(match.group('date'), "%Y%m%d").date()
            except TypeError:
                logging.info(f'Неверный формат для даты.')
            file_path = os.path.join(files_dir, filename)
            result.append(DateNamedFileInfo(file_path=file_path, file_date=file_date))
    if result:
        return max(result, key=attrgetter('file_date'))
    else:
        return None


def get_log_records(log_path, error_limit):
    """
    Возвращает записи из лог-файла с заданным лимитом ошибок.
    :param log_path: путь до лог-файла
    :param error_limit: лимит ошибок
    :return: словарь списков из адреса хоста и времени запроса
    """
    logging.info('Чтение данных из файла с логами.')
    open_fn = gzip.open if is_gzip_file(log_path) else io.open
    errors = 0
    records = 0
    dict_url = collections.defaultdict(list)
    with open_fn(log_path, mode='r') as log_file:
        lines = log_file.readlines()
    for line in lines:
        records += 1
        url, request_time = process_line(line, patterns=LOG_RECORD_RE)
        if not url and not request_time:
            errors += 1
            continue
        dict_url[url].append(request_time)
    if errors / records > error_limit:
        logging.error("Доля ошибок превысила приемлемую")
        raise Exception("Доля ошибок превысила допустимый предел {}".format(error_limit))
    return dict_url


def get_statistics(records, report_size):
    """
    Подсчет статистики в лог-файле
    :param records: словарь записей из лога
    :param report_size: количество строк в отчете
    :return: словарь списков из параметров статистики
    """
    logging.info('Сбор статистики из лога.')
    time_overall = 0
    overall_rec = 0
    for key, value in records.items():
        time_overall += sum(value)
        overall_rec += len(value)
    stat = []
    for key, value in records.items():
        time_perc = round((sum(value) / time_overall) * 100, 3)
        count_perc = round((len(value) / overall_rec) * 100, 3)
        stat.append({
            'url': key,
            'count': len(value),
            'count_perc': count_perc,
            'time_avg': statistics.mean(value),
            'time_max': round(max(value), 3),
            'time_med': statistics.median(value),
            'time_perc': time_perc,
            'time_sum': round(sum(value), 3)})
    stat = stat[:report_size]
    return stat


def get_report_path(report_dir, file_log):
    """
    Формирование пути для отчета с данными и логах.
    :param report_dir: путь до отчета
    :param file_log: название файла с логами
    :return: полный путь до отчета
    """
    logging.info('Формирование пути для отчета.')
    report_name = 'report-{}.html'.format(file_log.file_date.strftime(format="%Y.%m.%d"))
    report_path = os.path.join(report_dir, report_name)
    return report_path


def create_report(report_dir, report_path, stat):
    """
    Создание отчета со статистикой по лог-файлам
    :param report_dir: папка с отчетом
    :param report_path: полный путь до отчета
    :param stat: словарь статистики
    :return:
    """
    logging.info('Создание отчета на основе шаблона.')
    template_path = os.path.join(report_dir, 'report.html')
    with open(template_path) as f:
        template = string.Template(f.read())
    report = template.safe_substitute(table_json=json.dumps(stat))
    with open(report_path, mode='w') as f:
        f.write(report)
    logging.info("Отчет {} создан".format(report_path))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Путь до конфиг файла.', default=DEFAULT_CONFIG_PATH)
    return parser.parse_args()


def main():
    args = get_args()
    config = load_config(args.config, default_config)
    setup_logging(logfile=config.get('LOG_FILE'))
    logging.info("Данные конфига: {}".format(config))
    file_log_latest = get_latest_log_info(config['LOG_DIR'])
    log_records = get_log_records(file_log_latest.file_path, config.get("ERRORS_LIMIT"))
    stat = get_statistics(log_records, report_size=config['REPORT_SIZE'])
    report_path = get_report_path(report_dir=config['REPORT_DIR'], file_log=file_log_latest)
    create_report(report_dir=config['REPORT_DIR'], report_path=report_path, stat=stat)


if __name__ == "__main__":
    main()
