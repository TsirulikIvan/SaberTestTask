import argparse
import dataclasses
from pathlib import Path

from datetime import datetime
from typing import Tuple

_LOG_FILENAMES = 'log_a.jsonl', 'log_b.jsonl'
_OUTPUT_FILENAME = 'merged_log.jsonl'
_TIMESTAMP_INDEX = 1
_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
_TIMESTAMP_START_INDEX = 14


def create_dir(dir_path: Path) -> None:
    if dir_path.exists():
        pass
    else:
        dir_path.mkdir(parents=True)


@dataclasses.dataclass
class TimestampData:
    ts: datetime
    line: str


def swap_lines(lines_list: list, index_1: int = 0, index_2: int = 1) -> None:
    lines_list[index_1], lines_list[index_2] = lines_list[index_2], lines_list[index_1]


def generate_merged_log(output_file_path: Path, files_paths: Tuple[Path, ...]) -> None:
    log1_path, log2_path = (dir_path.joinpath(filename) for dir_path, filename in zip(files_paths, _LOG_FILENAMES))
    with log1_path.open('r') as log1_file, log2_path.open('r') as log2_file:

        with output_file_path.joinpath(_OUTPUT_FILENAME).open('w') as merged_log:

            timestamps_buffer = []

            for lines in zip(log1_file, log2_file):

                lines = list(lines)
                lines_ts = (line.split(', ')[_TIMESTAMP_INDEX][_TIMESTAMP_START_INDEX:-1:] for line in lines)
                ts1, ts2 = (datetime.strptime(line_ts, _TIMESTAMP_FORMAT) for line_ts in lines_ts)

                if ts1 > ts2:
                    swap_lines(lines)
                    ts1, ts2 = ts2, ts1

                timestamps_buffer.insert(0, TimestampData(ts2, lines[1]))

                min_cur_index = None

                for ts in timestamps_buffer:
                    if ts.ts < ts1:
                        min_cur_index = timestamps_buffer.index(ts)
                        timestamps_buffer.insert(min_cur_index, TimestampData(ts1, lines[0]))
                        break

                if min_cur_index is not None:
                    tail_buffer = timestamps_buffer[min_cur_index + 1::]
                    tail_buffer.reverse()
                    timestamps_buffer = timestamps_buffer[:min_cur_index + 1:]

                    for ts in tail_buffer:
                        merged_log.write(ts.line)

                else:
                    timestamps_buffer.append(TimestampData(ts1, lines[0]))

            merged_log.writelines(ts.line for ts in timestamps_buffer)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Tool to merge log files')

    parser.add_argument('log_1_path',
                        metavar='<LOG 1 DIR>',
                        type=str,
                        help='path to first log file')

    parser.add_argument('log_2_path',
                        metavar='<LOG 2 DIR>',
                        type=str,
                        help='path to second log file')

    parser.add_argument('-o',
                        '--output_dir',
                        default='',
                        metavar='<MERGED LOG DIR>',
                        type=str,
                        help='path to merged log file')

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    logs_dir = Path(args.log_1_path), Path(args.log_2_path)
    create_dir(output_dir)
    generate_merged_log(output_dir, logs_dir)


if __name__ == "__main__":
    main()