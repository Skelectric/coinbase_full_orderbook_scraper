import os
import re
from pathlib import Path
from datetime import datetime
from termcolor import colored

from loguru import logger

from tools.helper_tools import s_print

import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class WorkerDataFrame:
    def __init__(self, df_type: str, *args, **kwargs):
        self.df = pd.DataFrame()
        self.df_type = df_type
        self.filename_args = None
        self.filename = None
        self.total_items = 0
        self.output_folder = kwargs.get("output_folder", "data")

    def clear(self) -> None:  # clear dataframe in-place
        self.df = self.df.iloc[0:0]

    def append_tuple(self, data: tuple) -> None:  # append tuple
        _, col = self.df.shape
        if col == len(data):
            self.df.loc[len(self.df)] = data
        else:
            raise ValueError(f"Length mismatch. There are {col} columns, but {len(data)} elements to append.")
        self.total_items += 1

    def concat(self, data: pd.DataFrame) -> None:  # append dataframe
        self.df = pd.concat([self.df, data])
        self.total_items += 1

    def save_chunk(self, csv: bool = True, update_filename_flag: bool = False) -> None:
        """Save dataframe chunk using append."""

        if not csv:
            return

        if self.total_items == 0:  # filename can't be derived if dataframe is empty
            logger.debug(f"{self.df_type} dataframe is empty. Skipping save...")
            return

        file_exists = Path(f"{self.output_folder}/{self.filename}").is_file()

        if self.filename is not None:
            # logger.debug(f"data/{self.filename} exists: {file_exists}")

            if not file_exists:
                logger.debug(f"{self.filename} doesn't exist. Creating new one...")
                header = True
                self.derive_df_filename()
            else:
                # logger.debug(f"OK... File exists. Header set to False.")
                header = False

        else:  # append with headers only if file doesn't exist yet.
            # logger.debug(f"No filename generated for {self.df_type} dataframe yet.")
            # logger.debug(f"Setting header to True and deriving new filename...")
            header = True
            self.derive_df_filename()

        if csv:

            if Path(f"{self.output_folder}/{self.filename}").suffix != '.csv':  # append extension if doesn't exist
                self.filename += ".csv"

            if update_filename_flag and file_exists:  # rename when update_filename_flag=True (should only trigger at end)
                # logger.debug(f"update_filename_flag flag set to {update_filename_flag}. Running file rename steps...")
                self.update_filename(extension='.csv')

            self.df.to_csv(rf"{self.output_folder}/{self.filename}", index=False, mode='a', header=header)
            logger.info(f"Saved {self.df_type} dataframe into {self.filename}.")

    def update_filename(self, extension: str) -> None:
        prev_filename = self.filename
        self.derive_df_filename()
        self.filename += extension
        os.rename(f"{self.output_folder}/{prev_filename}", f"{self.output_folder}/{self.filename}")
        logger.info(f"Renamed file from {prev_filename} to {self.filename}...")
        # assert Path(f"data/{self.filename}").is_file()

    def derive_df_filename(self) -> None:

        def kwarg_it(__template: str) -> list:
            __template = re.sub(r"{", "{kwargs['", re.sub(r"}", "']}", __template))
            __list = re.split("_(?={)|(?<=})_", __template)
            return __list

        def __evaluate_filename_args(**kwargs) -> str:
            """
            Evaluate kwargs into a single filename string.
            Kwargs must include a 'template' kwarg.
            All elements within the f-string that must be evaluated should also be included within kwargs.
            Template elements can only be evaluated two levels deep.
            """

            template = kwargs["template"]
            eval_arg = []
            for arg in kwarg_it(template):

                try:
                    (arg,) = eval(arg)
                except NameError:
                    pass
                finally:
                    eval_arg.append(str(arg))

                if any(x in arg for x in {'{', '}'}):  # if brackets still exist, evaluate one level deeper
                    eval_sub_arg = []
                    for sub_arg in kwarg_it(arg):

                        try:
                            (sub_arg,) = eval(sub_arg)
                        except NameError:
                            pass
                        finally:
                            eval_sub_arg.append(str(sub_arg))

                    arg = '_'.join(eval_sub_arg)
                    eval_arg[-1] = arg

            return '_'.join(eval_arg)

        self.filename = __evaluate_filename_args(**self.filename_args)
        # logger.debug(f"filename derived: {self.filename}")

    @property
    def is_empty(self) -> bool:
        if self.df.empty:
            return True
        else:
            return False

    @property
    def rows(self):
        return self.total_items


class MatchDataFrame(WorkerDataFrame):
    def __init__(self, *args, **kwargs):
        super(MatchDataFrame, self).__init__(df_type="matches", *args, **kwargs)
        self.exchange = kwargs.get("exchange", None)
        self.market = kwargs.get("market", None)
        self.timestamp = kwargs.get("timestamp", datetime.now().strftime("%Y%m%d-%H%M%S"))
        self.columns = (
            "type", "time", "product_id", "side", "size", "price", "trade_id", "maker_order_id", "taker_order_id"
        )
        self.df = pd.DataFrame(columns=self.columns)
        self.filename = None

    def process_item(self, item, display_match=True, store_in_df=False) -> None:
        if display_match:
            self.display_match(item)
        if store_in_df:
            item = self.convert_to_df(item)
            self.concat(item)

    def convert_to_df(self, item) -> pd.DataFrame():
        """Converts passed item from dict to DataFrame"""
        item = {key: value for key, value in item.items() if key in self.columns}
        item["time"] = datetime.strptime(item.get('time'), "%Y-%m-%dT%H:%M:%S.%fZ")
        df = pd.DataFrame(columns=item.keys(), data=[item])
        return df

    def derive_df_filename(self) -> None:
        try:
            self.filename_args = {
                "template": "{exchange}_{filename_body}_{all_symbols}_USD_{timestamp}",
                "exchange": self.exchange,
                "filename_body": "{count}_order_matches",
                "count": self.rows,
                "all_symbols": self.short_str,
                "timestamp": self.timestamp
            }
        except Exception as e:
            logger.critical(e)
            raise e
        else:
            super().derive_df_filename()

    @property
    def short_str(self):
        if self.market is None:
            short_str = ','.join([x[:x.find("-")] for x in list(self.df.loc[:, "product_id"].unique())])
        else:
            short_str = self.market[:self.market.find("-")]
        return short_str

    @staticmethod
    def display_match(item) -> None:

        # build text
        line_output_1 = f"{item['time']} --- "
        line_output_2 = f"{item['side']} "
        line_output_3 = f"{item['size']} {item['product_id']} at ${float(item['price']):,}"

        # calc volume
        usd_volume = float(item['size']) * float(item['price'])
        line_output_right = f"${usd_volume:,.2f}"

        # handle colors
        if item["side"] == "buy":
            line_output_2 = colored(line_output_2, "green")
            line_output_right = colored(line_output_right, "green")
        elif item["side"] == "sell":
            line_output_2 = colored(line_output_2, "red")
            line_output_right = colored(line_output_right, "red")

        # alignment
        line_output_left = line_output_1 + line_output_2 + line_output_3
        line_output = f"{line_output_left:<80}{line_output_right:>25}"

        s_print(line_output)


class CandleDataFrame(WorkerDataFrame):
    def __init__(self, *args, **kwargs):
        super(CandleDataFrame, self).__init__(df_type="candles", *args, **kwargs)
        self.exchange = kwargs.get("exchange", None)
        self.market = kwargs.get("market", None)
        self.frequency = kwargs.get("frequency", None)
        self.timestamp = kwargs.get("timestamp", datetime.now().strftime("%Y%m%d-%H%M%S"))
        self.columns = (
            "type", "candle", "product_id", "frequency", "open", "high", "low", "close", "volume"
        )
        self.df = pd.DataFrame(columns=self.columns)
        # temp variables to help with building current candle
        self.last_candle = None
        self.last_open = None
        self.last_high = None
        self.last_low = None
        self.last_close = None
        self.last_volume = None

    def convert_to_df(self, item) -> pd.DataFrame():
        item["time"] = datetime.strptime(item.get('time'), "%Y-%m-%dT%H:%M:%S.%fZ")
        df = pd.DataFrame(columns=item.keys(), data=[item])
        return df

    def process_item(self, item) -> None:
        item = self.convert_to_df(item)  # convert from dict into df to leverage pandas dt.floor method
        __candle = item["time"].dt.floor(freq=self.frequency)[0]  # floor time at chosen frequency
        __product_id = item["product_id"][0]
        __size = float(item["size"][0])
        __price = float(item["price"][0])

        if self.last_candle is None:  # first candle
            self.last_open = __price
            self.last_high = __price
            self.last_low = __price
            self.last_close = __price
            self.last_volume = __size
        elif __candle != self.last_candle:
            # if new candle, append candle vars to df and reset vars for new candle
            __tuple = (
                "candles", self.last_candle, __product_id, self.frequency, self.last_open,
                self.last_high, self.last_low, self.last_close, round(self.last_volume, 6)
            )
            # logger.debug(f"appending tuple to candles df: {__tuple}")
            self.append_tuple(__tuple)
            self.last_open = __price
            self.last_high = __price
            self.last_low = __price
            self.last_close = __price
            self.last_volume = __size
        else:  # if same candle, continue building it up
            self.last_high = max(self.last_high, __price)
            self.last_low = min(self.last_low, __price)
            self.last_close = __price
            self.last_volume += __size

        self.last_candle = __candle

    def derive_df_filename(self) -> None:
        try:
            self.filename_args = {
                "template": "{exchange}_{filename_body}_{all_symbols}_USD_{timestamp}",
                "exchange": self.exchange,
                "filename_body": "{count}_{freq}_OHLC_candles",
                "count": self.rows,
                "freq": self.frequency,
                "all_symbols": self.short_str,
                "timestamp": self.timestamp
            }
        except Exception as e:
            logger.critical(e)
            raise e
        else:
            super().derive_df_filename()

    @property
    def short_str(self):
        if self.market is None:
            short_str = ','.join([x[:x.find("-")] for x in list(self.df.loc[:, "product_id"].unique())])
        else:
            short_str = self.market[:self.market.find("-")]
        return short_str
