#!/bin/python3

# Built-In Imports
from collections import OrderedDict
from datetime import datetime, timedelta
from os import system as command
from argparse import ArgumentParser
from sqlite3 import connect
from time import sleep
from threading import Thread

# Imported Libraries
import telepot
import pandas
import plotly.graph_objects as graphing

# Spreadsheet library methods.
from pyexcel_ods import get_data as read_spreadsheet
from pyexcel_ods import save_data as write_spreadsheet

# Alpaca.Markets brokerage account api.
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame

# Plotly graphing library.
import plotly.graph_objects as go

# In-house packages.
from Lab93_Cryptogram.CredentialManagement import *
from Lab93_TradeClient.AccountData import AccountEnumeration

credabase="/server/administrator/database/credentials.db"


class PortfolioReporter:

    def __init__( self,
                  credentialDatabase="./credentials.db",
                  priceIndex="./price-index.db"):
        """
        """


        """ Inline Functions """

        # Convert a string to a float founded to a certain percentage.
        self._round = lambda string, precision: round(float(string), precision)
        self._to_timestamp = lambda date_time: datetime.timestamp(date_time)
        self._from_timestamp = lambda time_stamp: datetime.fromtimestamp(time_stamp)


        # For writing changes to the price quote database.
        self.PriceIndex = priceIndex


        """ Alpaca API """

        # Exchange account API credentials.
        self.AlpacaAPI = MultiKeyAPICredentials(
            platform="alpaca",
            credabase=credentialDatabase,
            keyfile="/server/administrator/database/key"
        )


        """ Telegram API """

        # Hot-line to The Administrators personal desk.
        self.TelegramID = SingleKeyAPICredentials(
            platform="telegram_admin",
            credabase=credentialDatabase,
            keyfile="/server/administrator/database/key"
        )


        # Ignition key for the server golem.
        self.TelegramAPI = SingleKeyAPICredentials(
            platform="telegram",
            credabase=credentialDatabase,
            keyfile="/server/administrator/database/key"
        )


        # Snapshot index of current account position.
        self.AccountReport = AccountEnumeration(self.AlpacaAPI["key"], self.AlpacaAPI["secret"])

        self.AdministratorHotline = telepot.Bot(self.TelegramAPI)


    def DailyReport(self):
        """
        A daily report consists of a fresh sub-directory populated with a spreadsheet defining
        a snapshot of the account position at the time of writing the report and a candle chart
        detailing the performance of all currently held assets throughout the past market day.

        In order to generate the candle chart for the previous market period, the system will query
        the broker api's historic market data service for the days minutes and write them to the
        price-index database.

        At the end of the generation of each report the administrator is alerted to the fact
        through telegram with a link to view the report in its entirety at lab-93.guyyatsu.me.
        """


        """ initialize all constants required by the report """

        # the datetime object representing the current timestamp
        report_date = datetime.now()

        # previous dates datetime object
        report_start = report_date - timedelta(days=1)
        
        # directory for archiving reports
        report_directory="/server/resources/reports/daily"

        # spreadsheet for detailing account positional snapshot
        spreadsheet = f"{report_directory}/{report_date.strftime('%Y-%m-%d')}.spreadsheet.ods"

        # japanese bar charts tracking movements of a position
        candlegraph = f"{report_directory}/{report_date.strftime('%Y-%m-%d')}.candlegraph.png"

        # bitcoin historic data collection api
        data_client = CryptoHistoricalDataClient()

        # data packet for writing to spreadsheet
        spreadsheet_data = OrderedDict()


        """ record previous day performance data to price-index """
        def CollectMinutes():
            """
            """
            connection = connect(self.PriceIndex)
            cursor = connection.cursor()
            execute = cursor.execute

            # Solicit the previous market days performance
            # bars by the minute up to the current date.
            previous_market_minutes = data_client.get_crypto_bars(
                CryptoBarsRequest(
                    symbol_or_symbols=["BTC/USD"],
                    timeframe=TimeFrame.Minute,
                    start=report_start,
                    end=report_date,
                )
            )

            # Read the minute data response line by line
            # and write each to the price-index database.
            for line_minute in previous_market_minutes["BTC/USD"]:

                time = self._to_timestamp(line_minute.timestamp)
                execute(
                    "INSERT OR IGNORE INTO btcusd(high, low, open, close, time) VALUES(?,?,?,?,?)",
                    (line_minute.high, line_minute.low, line_minute.open, line_minute.close, time)
                ); connection.commit(); sleep(0.5)


        """ write spreadsheet detailing current account position """
        def RecordPosition():
            """
            """


            # Summarize account holdings overall
            # and by position.
            positions = self.AccountReport\
                            .GetAllPositions(cmdline=False)

            account = self.AccountReport\
                          .GetAccount(cmdline=False)


            # Write out a sheet detailing current
            # major points in the account.

            spreadsheet_data.update(
                {"Investments": [
                    ["Equity", self._round(account["equity"], 2)],
                    ["Cash", self._round(account["cash"], 2)],
                    ["Bitcoin P/L", self._round(positions["BTCUSD"]["unrealized profit/loss"], 2)]
                ]}
            )


            # Write a summary for each position held
            # by the account for a more detailed analysis
            # of the asset.

            for position in positions:
                spreadsheet_data.update({
                    str(positions[position]["symbol"]): [

                        # Average price paid for an asset
                        ["Entry Price", self._round(positions[position]["average entry price"],2)],

                        # Market-value of the asset at time of reporting.
                        ["Current Price", self._round(positions[position]["current price"],2)],
    
                        # Fractional share of the asset being held.
                        ["Quantity Held", self._round(positions[position]["quantity"],3)],
    
                        # Current going-price for our share of assets.
                        ["Market Value", self._round(positions[position]["market value"],2)],
    
                        # How much _we_ paid for our position.
                        ["Cost Basis", self._round(positions[position]["cost basis"],2)],
    
                        # Total difference between entryprice.marketvalue and maket.value.
                        ["Profit / Loss", self._round(positions[position]["unrealized profit/loss"],2)]
                    ]
                })

            # Record writings to spreadsheet file and save.
            return write_spreadsheet(spreadsheet, spreadsheet_data)


        """ generate candlestick graph for report """
        def WriteCandles():

            market_open = []
            market_close = []
            market_high = []
            market_low = []
            market_time = []

            # Request hourly bars for an asset to make
            # it easier to draw a graph for it.
            previous_market_hours = data_client.get_crypto_bars(
                CryptoBarsRequest(
                    symbol_or_symbols=["BTC/USD"],
                    timeframe=TimeFrame.Hour,
                    start=report_start,
                    end=report_date,
                )
            )

            # Read the results in an array for use with pandas.
            for line_hour in previous_market_hours["BTC/USD"]:
                time = self._to_timestamp(line_hour.timestamp)
                market_open.append(line_hour.open)
                market_close.append(line_hour.close)
                market_high.append(line_hour.high)
                market_low.append(line_hour.low)
                market_time.append(time)

            CandlestickData= [
                go.Candlestick(
                    x=market_time,
                    open=market_open,
                    high=market_high,
                    low=market_low,
                    close=market.close
                )
            ]

            go.Figure(CandlestickData).write_image(candlegraph)


        """ system runtime definition and execution """

        def RuntimeExecution():

            threads = [ Thread(target=CollectMinutes, daemon=True),
                        Thread(target=RecordPosition, daemon=True),
                        Thread(target=WriteCandles, daemon=True)    ]

            for thread in threads: thread.start(); thread.join()

            # Alert the administrator of his new report.
            return self.AdministratorHotline.sendMessage(
                self.TelegramID,
                "Your daily report is ready."
            )


        RuntimeExecution()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-P", "--price-index", required=True)
    parser.add_argument("-C", "--credentials", required=True)
    parser.add_argument("-d", "--daily", action="store_true")
    parser.add_argument("-w", "--weekly", action="store_true")
    arguments = parser.parse_args()

    """
    if arguments.weekly:
        spreadsheet = PortfolioReporter(reportDirectory=arguments.reports_directory)
        spreadsheet.WeeklyReport(database=arguments.price_index)
    """ 

    if arguments.daily:

        spreadsheet = PortfolioReporter(
            credentialDatabase = arguments.credentials,
            priceIndex=arguments.price_index)

        spreadsheet.DailyReport()
