# %% Libs
import json
import pandas as pd
import signal
from bs4 import BeautifulSoup
from numpy import nan
# from random import sample
from retrying import retry
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen


# Set timeout objects for retrying stalled notes
class TimeoutException(Exception):
    pass


class Timeout:

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutException(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


# %% Read in the examples
class BmoScraper:
    # Pass in note URLs & lookup for PDW
    def __init__(self, bmo_urls):
        try:
            self.notes_dict = {}
            self.errors_dict = {}
            self.timeout_sec = 600
            for note in bmo_urls:
                try:
                    try:
                        try:
                            self.notes_dict[note.rsplit(
                                '/', 1)[-1]] = pd.read_html(note)
                            sleep(3)
                        except HTTPError:
                            print(
                                'HTTP Error: Waiting 10 seconds and trying again'  # noqa
                                ' for the current item')
                            sleep(10)
                            self.notes_dict[note.rsplit(
                                '/', 1)[-1]] = pd.read_html(note)
                            sleep(3)
                    except HTTPError:
                        print('HTTP Error: Waiting 30 seconds and trying again'
                              ' for the current item')
                        sleep(30)
                        self.notes_dict[note.rsplit(
                            '/', 1)[-1]] = pd.read_html(note)
                        sleep(3)
                except HTTPError:
                    message = (f'Note {note} failed to read after 3 attemps.  '
                               'Logging for investigation.')
                    self.errors_dict[(note, '__init__')] = message

            self.bmo_example_fields = pd.read_excel('BMO Examples.xlsx')
            self.pdw_df = self.bmo_example_fields[['PDW Fields']].copy()
            self.skip_cols = pd.Series(
                ['Payment Schedule', 'Portfolio Summary', 'Rates Schedule'])
        except Exception as e:
            # This should never be triggered, but may be given a non-URL
            template = ("An exception of type {0} occurred. "
                        "Arguments:\n{1!r}")
            message = template.format(type(e).__name__, e.args)
            self.errors_dict[('__init__')] = message

    # Get all scraping results as a single row table
    @retry(stop_max_attempt_number=3)
    def transpose_set_header(self):

        # Additional fxn to recycle code
        def reassign_cols_truncate(self, key, i):
            self.notes_dict[key][i].columns = self.notes_dict[key][i].iloc[0]
            self.notes_dict[key][i] = self.notes_dict[key][i][1:]

        # This checks if the scrape results are a true table or not
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    for i in range(len(val)):
                        if ~self.skip_cols.isin(
                                self.notes_dict[key][i].columns).any():
                            self.notes_dict[key][i] = self.notes_dict[key][i].T
                            reassign_cols_truncate(self, key, i)
                        else:
                            table_name = self.notes_dict[key][i].columns[0]
                            reassign_cols_truncate(self, key, i)
                            self.notes_dict[key][i].columns.name = table_name
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, 'transpose_set_header')] = (message,
                                                                       val)

    # Convert all to dict to navigate indices
    @retry(stop_max_attempt_number=3)
    def label_note_tables(self):
        with Timeout(seconds=self.timeout_sec):
            try:
                self.notes_dict = {
                    key:
                    {val[i].columns.name: val[i]
                     for i in range(len(val))}
                    for key, val in self.notes_dict.items()
                }
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict['label_note_tables'] = message

    # Set PDW index
    @retry(stop_max_attempt_number=3)
    def set_pdw_index(self):
        with Timeout(seconds=self.timeout_sec):
            try:
                self.pdw_df.set_index('PDW Fields', inplace=True)
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict['set_pdw_index'] = message

    # Rule: PDW Name
    @retry(stop_max_attempt_number=3)
    def _PDW_Name(self):
        with Timeout(seconds=self.timeout_sec):
            for key in self.notes_dict.keys():
                try:
                    self.pdw_df[key] = None
                    self.pdw_df.at[
                        'PDW Name',
                        key] = 'https://www.bmonotes.com/Note/' + key
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, 'transpose_set_header')] = message

    # Rule: callBarrierLevelFinal
    @retry(stop_max_attempt_number=3)
    def _callBarrierLevelFinal(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                # Check if right type of note
                try:
                    if 'Payment Schedule' in val.keys():
                        if 'Autocall Level' in self.notes_dict[key][
                                'Payment Schedule'].columns:
                            # Replace '-' with NaN
                            self.notes_dict[key]['Payment Schedule'].replace(
                                '-', None, inplace=True, regex=False)

                            # Set filter to first non-null value
                            mask = ~self.notes_dict[key]['Payment Schedule'][
                                'Autocall Level'].isnull()

                            # Set value in the PDW table
                            self.pdw_df.at[
                                'productCall.callBarrierLevelFinal',
                                key] = float(
                                    self.notes_dict[key]['Payment Schedule']
                                    ['Autocall Level'].loc[mask].iloc[0].strip(
                                        '%').replace(" ", "")) / 100
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key,
                                      '_callBarrierLevelFinal')] = (message,
                                                                    val)

    # Rule: callObservationDateList
    @retry(stop_max_attempt_number=3)
    def _callObservationDateList(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                # Check if right type of note
                try:
                    if 'Payment Schedule' in val.keys():
                        if 'Observation Date' in val[
                                'Payment Schedule'].columns:
                            # Add the entire observation date column as a list
                            self.pdw_df.at[
                                'productCall.callObservationDateList',
                                key] = pd.to_datetime(
                                    self.notes_dict[key]['Payment Schedule']
                                    ['Observation Date']).dt.strftime(
                                        r'%Y-%m-%d').to_list()
                            self.pdw_df.at[
                                'productCall.callObservationDateList',
                                key] = [{
                                    'callObservationDate': val
                                } for val in self.pdw_df.at[
                                    'productCall.callObservationDateList',
                                    key]]

                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key,
                                      '_callObservationDateList')] = (message,
                                                                      val)

    # Rule: callObservationFrequency
    @retry(stop_max_attempt_number=3)
    def _callObservationFrequency(self):

        def check_call_freq(self, dt_days):
            if 2 <= dt_days <= 5:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Bi-Weekly'
            elif 6 <= dt_days <= 7:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Weekly'
            elif 28 <= dt_days <= 31:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Monthly'
            elif 14 <= dt_days <= 16:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Bi-Monthly'
            elif dt_days == 1:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Daily'
            elif 364 <= dt_days <= 366:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Annually'
            elif 182 <= dt_days <= 184:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Semi-Annually'
            elif 89 <= dt_days <= 92:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Quarterly'
            else:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'Custom'

        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    # Check if right type of note
                    if 'Payment Schedule' in val.keys():
                        if 'Observation Date' in val[
                                'Payment Schedule'].columns:
                            # Add the entire observation date column as a list
                            dt_diff = pd.to_datetime(
                                self.notes_dict[key]['Payment Schedule']
                                ['Observation Date'])
                            dt_diff = dt_diff - dt_diff.shift()
                            dt_days = dt_diff.mean().days
                            check_call_freq(self, dt_days)
                    elif 'Product Details' in val.keys():
                        if 'Extension Frequency' in val[
                                'Product Details'].columns:
                            value = self.notes_dict[key]['Product Details'][
                                'Extension Frequency'][0]
                            value_dict = {
                                'Semi-Annual': 'Semi-Annually',
                                'Annual': 'Annually',
                                'Quarter': 'Quarterly',
                                'Month': 'Monthly',
                                'Bi-Month': 'Bi-Monthly',
                                'Week': 'Weekly',
                                'Day': 'Daily',
                            }
                            self.pdw_df.at[
                                'productCall.callObservationFrequency',
                                key] = value_dict[value]
                            self.pdw_df.at['productCall.callType',
                                           key] = 'Issuer'
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key,
                                      '_callObservationFrequency')] = (message,
                                                                       val)

    # Rule: callType
    @retry(stop_max_attempt_number=3)
    def _callType(self):
        # Check if right type of note
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Payment Schedule' in val.keys():
                        if 'Autocall Level' in val['Payment Schedule'].columns:
                            # Check if all values are the same
                            autocall_series = self.notes_dict[key][
                                'Payment Schedule']['Autocall Level'].dropna(
                                ).reset_index(drop=True)
                            if (autocall_series == autocall_series.values[0]
                                ).all():  # noqa
                                self.pdw_df.at['productCall.callType',
                                               key] = 'Auto'
                            else:
                                self.pdw_df.at['productCall.callType',
                                               key] = 'Autocall Step'
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_callType')] = (message, val)

    # Rule: numberNoCallPeriods
    @retry(stop_max_attempt_number=3)
    def _numberNoCallPeriods(self):
        # Check if right type of note
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Payment Schedule' in val.keys():
                        if 'Autocall Level' in val['Payment Schedule'].columns:
                            # Filter to NaN + 1
                            self.pdw_df.at[
                                'productCall.numberNoCallPeriods', key] = len(
                                    self.notes_dict[key]['Payment Schedule'].
                                    loc[self.notes_dict[key]
                                        ['Payment Schedule']
                                        ['Autocall Level'].isna()]) + 1
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_numberNoCallPeriods')] = (message,
                                                                       val)

    # Rule: currency
    @retry(stop_max_attempt_number=3)
    def _currency(self):
        # Check if right type of note
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Currency' in val['Product Details'].columns:
                            # Get currency column
                            self.pdw_df.at[
                                'productGeneral.currency',
                                key] = self.notes_dict[key]['Product Details'][
                                    'Currency'][0]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_currency')] = (message, val)

    # Rule: cusip
    @retry(stop_max_attempt_number=3)
    def _cusip(self):

        def get_isin_from_cusip(cusip_str):
            country_code = 'CA'
            isin_to_digest = country_code + cusip_str.upper()

            get_numerical_code = lambda c: str(ord(c) - 55)  # noqa
            encode_letters = lambda c: c if c.isdigit(  # noqa
            ) else get_numerical_code(c)
            to_digest = ''.join(map(encode_letters, isin_to_digest))

            ints = [int(s) for s in to_digest[::-1]]
            every_second_doubled = [x * 2 for x in ints[::2]] + ints[1::2]

            sum_digits = lambda i: sum(divmod(i, 10))  # noqa
            digit_sum = sum([sum_digits(i) for i in every_second_doubled])

            check_digit = (10 - digit_sum % 10) % 10
            return isin_to_digest + str(check_digit)

        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    # Get JHN column and correct length
                    if 'Product Details' in val.keys():
                        if 'Cusip' in val['Product Details'].columns:
                            self.pdw_df.at['productGeneral.cusip',
                                           key] = self.notes_dict[key][
                                               'Product Details']['Cusip'][0]
                            self.pdw_df.at['productGeneral.isin',
                                           key] = get_isin_from_cusip(
                                               self.pdw_df.at[
                                                   'productGeneral.cusip',
                                                   key])
                        elif 'JHN Code' in val['Product Details'].columns:
                            jhn = self.notes_dict[key]['Product Details'][
                                'JHN Code'][0]
                            if len(jhn) == 7:
                                self.pdw_df.at['productGeneral.cusip',
                                               key] = 'CA' + jhn
                            elif len(jhn) == 8:
                                self.pdw_df.at['productGeneral.cusip',
                                               key] = 'C' + jhn
                            else:
                                self.pdw_df.at['productGeneral.cusip',
                                               key] = jhn
                                self.pdw_df.at['productGeneral.isin',
                                               key] = get_isin_from_cusip(
                                                   self.pdw_df.at[
                                                       'productGeneral.cusip',
                                                       key])

                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_cusip')] = (message, val)

    # Rule: issueDate
    @retry(stop_max_attempt_number=3)
    def _issueDate(self):
        # Check if right type of note
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    # Get date in right format
                    if 'Product Details' in val.keys():
                        if 'Issue Date' in val['Product Details'].columns:
                            self.pdw_df.at[
                                'productGeneral.issueDate',
                                key] = pd.to_datetime(
                                    self.notes_dict[key]['Product Details']
                                    ['Issue Date']).dt.strftime(r'%Y-%m-%d')[0]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_issueDate')] = (message, val)

    # Rule: issuer
    @retry(stop_max_attempt_number=3)
    def _issuer(self):
        with Timeout(seconds=self.timeout_sec):
            for key in self.notes_dict.keys():
                # Hardcode for now
                try:
                    self.pdw_df.at['productGeneral.issuer',
                                   key] = 'Bank of Montreal'
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_issuer')] = message

    # Rule: maturityDate
    @retry(stop_max_attempt_number=3)
    def _maturityDate(self):
        # Check if right type of note
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    # Get date in right format
                    if 'Product Details' in val.keys():
                        if 'Maturity Date' in val['Product Details'].columns:
                            self.pdw_df.at[
                                'productGeneral.maturityDate',
                                key] = pd.to_datetime(
                                    self.notes_dict[key]['Product Details']
                                    ['Maturity Date']).dt.strftime(
                                        r'%Y-%m-%d')[0]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_maturityDate')] = (message, val)

    # Rule: productName
    @retry(stop_max_attempt_number=3)
    def _productName(self):
        # Get title of webpages
        with Timeout(seconds=self.timeout_sec):
            try:
                for key, val in self.notes_dict.items():
                    soup = BeautifulSoup(
                        urlopen('https://www.bmonotes.com/Note/' + key),
                        features="lxml")
                    page_title = str(soup.find_all('h1')[1]).replace(
                        r'<h1>', '').replace(r'</h1>', '').strip()
                    self.pdw_df.at['productGeneral.productName',
                                   key] = page_title
                    if 'Principal Protected' in page_title:
                        self.pdw_df.at['productGeneral.registrationType',
                                       key] = 'PPN'
                    elif 'Description' in val.keys():
                        if 'principal protection' in val[
                                'Description'].columns[0].lower():
                            self.pdw_df.at['productGeneral.registrationType',
                                           key] = 'PPN'
                    else:
                        self.pdw_df.at['productGeneral.registrationType',
                                       key] = 'PAR'
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_productName')] = message

    # Rule: stage
    @retry(stop_max_attempt_number=3)
    def _stage(self):
        # Simple hardcode
        with Timeout(seconds=self.timeout_sec):
            for key in self.notes_dict.keys():
                try:
                    self.pdw_df.at['productGeneral.stage', key] = 'Ops Review'
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_stage')] = message

    # Rule: status
    @retry(stop_max_attempt_number=3)
    def _status(self):
        # Simple hardcode
        with Timeout(seconds=self.timeout_sec):
            for key in self.notes_dict.keys():
                try:
                    self.pdw_df.at['productGeneral.status',
                                   key] = 'Update Product Details'
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_status')] = message

    # Rule: tenorFinal
    @retry(stop_max_attempt_number=3)
    def _tenorFinal(self):
        # Get Term value
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Term' in val['Product Details'].columns:
                            self.pdw_df.at['productGeneral.tenorFinal',
                                           key] = float(self.notes_dict[key]
                                                        ['Product Details']
                                                        ['Term'][0].split()[0])
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_tenorFinal')] = (message, val)

    # Rule: tenorUnit
    @retry(stop_max_attempt_number=3)
    def _tenorUnit(self):
        # Get Term unit
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Term' in val['Product Details'].columns:
                            self.pdw_df.at[
                                'productGeneral.tenorUnit',
                                key] = self.notes_dict[key]['Product Details'][
                                    'Term'][0].split()[1].title()
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_tenorUnit')] = (message, val)

    # Rule: underlierList
    @retry(stop_max_attempt_number=3)
    def _underlierList(self):
        with Timeout(seconds=self.timeout_sec):
            try:
                # Get value from table
                for key, val in self.notes_dict.items():
                    if 'Product Details' in val.keys():
                        if 'Linked To' in val['Product Details'].columns:
                            if ',' in self.notes_dict[key]['Product Details'][
                                    'Linked To'][0]:
                                self.pdw_df.at['productGeneral.underlierList',
                                               key] = self.notes_dict[key][
                                                   'Product Details'][
                                                       'Linked To'][0].replace(
                                                           ' ', '').split(',')

                            elif ',' not in self.notes_dict[key][
                                    'Product Details']['Linked To'][0]:
                                self.pdw_df.at[
                                    'productGeneral.underlierList', key] = [{
                                        'underlierSymbol':
                                        self.notes_dict[key]['Product Details']
                                        ['Linked To'][0],
                                        'underlierWeight':
                                        1.0,
                                        'underlierSource':
                                        'Bloomberg',
                                    }]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_underlierList')] = (message, val)

    # Rule: underlierWeight
    @retry(stop_max_attempt_number=3)
    def _underlierWeight(self):
        # Check for portfolio summary section and get weights
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Portfolio Summary' in val.keys():
                        if 'Share Weight' in val['Portfolio Summary'].columns:
                            underlier_weight = val['Portfolio Summary'].loc[
                                val['Portfolio Summary']['Share Weight'].str.
                                contains('%', na=False),
                                'Share Weight'].str.replace(
                                    '%', '').astype(float) / 100
                            self.pdw_df.at[
                                'productGeneral.underlierList.underlierWeight',
                                key] = underlier_weight.to_list()
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_underlierWeight')] = (message,
                                                                   val)

    # Rule: upsideParticipationRateFinal
    @retry(stop_max_attempt_number=3)
    def _upsideParticipationRateFinal(self):
        # Get value from table & convert to float
        with Timeout(seconds=self.timeout_sec):
            try:
                for key, val in self.notes_dict.items():
                    if 'Product Details' in val.keys():
                        if 'Upside Participation' in val[
                                'Product Details'].columns:
                            self.pdw_df.at[
                                'productGrowth.upsideParticipationRateFinal',
                                key] = float(
                                    self.notes_dict[key]['Product Details']
                                    ['Upside Participation'][0].replace(
                                        '-', '').replace('%', '')) / 100
                        elif 'Excess Participation' in val[
                                'Product Details'].columns:
                            self.pdw_df.at[
                                'productGrowth.upsideParticipationRateFinal',
                                key] = float(
                                    self.notes_dict[key]['Product Details']
                                    ['Excess Participation'][0].replace(
                                        '-', '').replace('%', '')) / 100
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key,
                                  '_upsideParticipationRateFinal')] = (message,
                                                                       val)

    # Rule: principalBarrierLevelFinal
    @retry(stop_max_attempt_number=3)
    def _principalBarrierLevelFinal(self):
        # Get value from table & convert to float
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    # These can be negative!
                    if 'Product Details' in val.keys():
                        if 'Barrier Protection' in val[
                                'Product Details'].columns:
                            barrier_val = float(
                                self.notes_dict[key]['Product Details']
                                ['Barrier Protection'][0].replace(
                                    '%', '').replace(" ", "")) / 100
                            self.pdw_df.at[
                                'productProtection.principalBarrierLevelFinal',
                                key] = barrier_val + 1
                            self.pdw_df.at['productProtection.protectionLevel',
                                           key] = barrier_val * -1
                            self.pdw_df.at['productProtection.downsideType',
                                           key] = 'Barrier'
                            self.pdw_df.at[
                                'productProtection.putLeverageFinal', key] = 1
                            self.pdw_df.at['productProtection.putStrikeFinal',
                                           key] = barrier_val + 1
                        elif 'Buffer Protection' in val[
                                'Product Details'].columns:
                            buffer_val = float(
                                self.notes_dict[key]['Product Details']
                                ['Buffer Protection'][0].replace(
                                    '%', '').replace(" ", "")) / 100
                            self.pdw_df.at[
                                'productProtection.principalBufferLevelFinal',
                                key] = buffer_val * -1
                            self.pdw_df.at['productProtection.putStrikeFinal',
                                           key] = buffer_val + 1
                            self.pdw_df.at['productProtection.protectionLevel',
                                           key] = buffer_val * -1
                            self.pdw_df.at['productProtection.downsideType',
                                           key] = 'Buffer'
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_principalBarrierLevelFinal')] = (
                        message, val)

    # Rule: paymentBarrierFinal
    @retry(stop_max_attempt_number=3)
    def _paymentBarrierFinal(self):
        # Grab field from table & convert to float
        with Timeout(seconds=self.timeout_sec):
            try:
                for key, val in self.notes_dict.items():
                    if 'Product Details ' in val.keys():
                        if 'Coupon Knock-Out Level' in val[
                                'Product Details '].columns:
                            coupon_val = float(
                                self.notes_dict[key]['Indicative Return']
                                ['Coupon Knock-Out Level'][0].replace(
                                    '-', '').replace('%', '').replace(
                                        " ", "")) / 100
                            self.pdw_df.at['productYield.paymentBarrierFinal',
                                           key] = coupon_val
                        elif 'Coupon Knock-In Level' in val[
                                'Product Details '].columns:
                            coupon_val = float(
                                self.notes_dict[key]['Indicative Return']
                                ['Coupon Knock-In Level'][0].replace(
                                    '%', '').replace(" ", "")) / 100
                            self.pdw_df.at['productYield.paymentBarrierFinal',
                                           key] = coupon_val + 1
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_paymentBarrierFinal')] = (message,
                                                                   val)

    # Rule: paymentDateList
    @retry(stop_max_attempt_number=3)
    def _paymentDateList(self):
        # Add column as list
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Payment Schedule' in val.keys():
                        if 'Coupon Payment Date' in val[
                                'Payment Schedule'].columns:
                            self.pdw_df.at[
                                'productYield.paymentDateList',
                                key] = pd.to_datetime(
                                    self.notes_dict[key]['Payment Schedule']
                                    ['Coupon Payment Date']).dt.strftime(
                                        r'%Y-%m-%d').to_list()
                            self.pdw_df.at['productYield.paymentDateList',
                                           key] = [{
                                               'paymentDate': val
                                           } for val in self.pdw_df.at[
                                               'productYield.paymentDateList',
                                               key]]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_paymentDateList')] = (message,
                                                                   val)

    # Rule: paymentEvaluationFrequencyFinal
    @retry(stop_max_attempt_number=3)
    def _paymentEvaluationFrequencyFinal(self):
        # If exists, static
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Pay Frequency' in val['Product Details'].columns:
                            if isinstance(
                                    val['Product Details']['Pay Frequency'][0],
                                    str):
                                self.pdw_df.at[
                                    'productYield.paymentEvaluationFrequencyFinal',  # noqa
                                    key] = self.notes_dict[key][
                                        'Product Details']['Pay Frequency'][
                                            0].title()
                                self.pdw_df.at[
                                    'productYield.paymentFrequency',
                                    key] = self.notes_dict[key][
                                        'Product Details']['Pay Frequency'][
                                            0].title()
                        elif 'Coupon Frequency' in val[
                                'Product Details'].columns:
                            if isinstance(
                                    val['Product Details']['Coupon Frequency']
                                [0], str):  # noqa
                                value_dict = {
                                    'Semi-Annual': 'Semi-Annually',
                                    'Annual': 'Annually',
                                    'Quarter': 'Quarterly',
                                    'Month': 'Monthly',
                                    'Bi-Month': 'Bi-Monthly',
                                    'Week': 'Weekly',
                                    'Day': 'Daily',
                                }
                                value = self.notes_dict[key][
                                    'Product Details']['Coupon Frequency'][
                                        0].title()
                                self.pdw_df.at['productYield.paymentFrequency',
                                               key] = value_dict[value]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(
                        key, '_paymentEvaluationFrequencyFinal')] = (message,
                                                                     val)

    # Rule: paymentRatePerAnnumFinal
    @retry(stop_max_attempt_number=3)
    def _paymentRatePerAnnumFinal(self):
        # If exists, static & convert % to float
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Contingent Coupon' in val[
                                'Product Details'].columns:
                            self.pdw_df.at[
                                'productYield.paymentRatePerAnnumFinal',
                                key] = float(
                                    self.notes_dict[key]['Product Details']
                                    ['Contingent Coupon'][0].replace(
                                        '-', '').replace('%', '').replace(
                                            " ", "")) / 100
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key,
                                      '_paymentRatePerAnnumFinal')] = (message,
                                                                       val)

    # Rule: paymentRatePerPeriodFinal
    @retry(stop_max_attempt_number=3)
    def _paymentRatePerPeriodFinal(self):
        # Take previous rule results to calculate this if exists
        with Timeout(seconds=self.timeout_sec):
            for col in self.pdw_df.columns:
                try:
                    cond1 = self.pdw_df.at[
                        'productYield.paymentRatePerAnnumFinal',
                        col] is not None
                    cond2 = self.pdw_df.at['productYield.paymentFrequency',
                                           col] is not None
                    cond3 = {
                        'Annualy': 1,
                        'Bi-Monthly': 24,
                        'Bi-Weekly': 104,
                        'Daily': 365,
                        'Monthly': 12,
                        'Quarterly': 4,
                        'Semi-Annually': 2,
                        'Weekly': 52,
                    }
                    if cond1 and cond2:
                        self.pdw_df.at[
                            'productYield.paymentRatePerPeriodFinal',
                            col] = self.pdw_df.at[
                                'productYield.paymentRatePerAnnumFinal',
                                col] / cond3[self.pdw_df.at[
                                    'productYield.paymentFrequency', col]]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(col,
                                      'paymentRatePerPeriodFinal')] = message

    # Rule: fundservID
    @retry(stop_max_attempt_number=3)
    def _fundservID(self):
        # If exists, static
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'JHN Code' in val[
                                'Product Details'].columns and key.startswith(
                                    'JHN'):
                            self.pdw_df.at[
                                'productGeneral.fundservID',
                                key] = self.notes_dict[key]['Product Details'][
                                    'JHN Code'][0]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_fundservID')] = (message, val)

    # Rule: Mark to Market Price
    @retry(stop_max_attempt_number=3)
    def _mark_to_market_price(self):
        # If exists, static & replace $, convert to float
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Current Status' in val.keys():
                        if 'Current Bid Price' in val[
                                'Current Status'].columns:
                            if val['Current Status']['Current Bid Price'][
                                    0] != '-':  # noqa
                                self.pdw_df.at[
                                    'Mark to Market Price', key] = float(
                                        self.notes_dict[key]['Current Status']
                                        ['Current Bid Price'][0].replace(
                                            '$', ''))
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key,
                                      '_mark_to_market_price')] = (message,
                                                                   val)

    # Rule: minimumReturnFinal
    @retry(stop_max_attempt_number=3)
    def _minimumReturnFinal(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Minimum Payment' in val['Product Details'].columns:
                            self.pdw_df.at[
                                'productGrowth.minimumReturnFinal',
                                key] = float(
                                    self.notes_dict[key]['Product Details']
                                    ['Minimum Payment'][0].replace('$', ''))
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_minimumReturnFinal')] = (message,
                                                                      val)

    # Rule: minimumReturnFinal
    @retry(stop_max_attempt_number=3)
    def _tradeDate(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Available Until' in val['Product Details'].columns:
                            self.pdw_df.at[
                                'productGeneral.tradeDate',
                                key] = pd.to_datetime(
                                    self.notes_dict[key]['Product Details']
                                    ['Available Until']).dt.strftime(
                                        r'%Y-%m-%d')[0]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_tradeDate')] = (message, val)

    # Rule: callPremiumFinal
    @retry(stop_max_attempt_number=3)
    def _callPremiumFinal(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'AutoCall Coupon (Next Call Date)' in val[
                                'Product Details'].columns:
                            self.pdw_df.at[
                                'productCall.callPremiumFinal', key] = float(
                                    self.notes_dict[key]['Product Details']
                                    ['AutoCall Coupon (Next Call Date)']
                                    [0].replace('-', '').replace(
                                        '%', '').replace(" ", "")) / 100
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_callPremiumFinal')] = (message,
                                                                    val)

    # Rule: putLeverageFinal
    @retry(stop_max_attempt_number=3)
    def _putLeverageFinal(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys():
                        if 'Downside Participation' in val[
                                'Product Details'].columns:
                            value = float(
                                self.notes_dict[key]['Product Details']
                                ['Downside Participation'][0].replace(
                                    '-', '').replace('%', '').replace(
                                        " ", "")) / 100
                            self.pdw_df.at[
                                'productProtection.putLeverageFinal',
                                key] = value
                            if value > 1:
                                self.pdw_df.at[
                                    'productProtection.downsideType',
                                    key] = 'Geared Buffer'

                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_putLeverageFinal')] = (message,
                                                                    val)

    # Rule: extendibleNote
    @retry(stop_max_attempt_number=3)
    def _extendibleNote(self):
        with Timeout(seconds=self.timeout_sec):
            for key, val in self.notes_dict.items():
                try:
                    if 'Product Details' in val.keys(
                    ) and 'Rates Schedule' in val.keys():
                        if 'Product SubType' in val['Product Details'].columns:
                            if val['Product Details']['Product SubType'][
                                    0] != 'Extendible Step-up Note':
                                if (val['Rates Schedule']['Rate/Coupon'] ==
                                        val['Rates Schedule']
                                    ['Rate/Coupon'].values[0]).all():  # noqa
                                    rate_coupon_val = self.notes_dict[key][
                                        'Rates Schedule'][
                                            'Rate/Coupon'].values[0].replace(
                                                '%', '').replace(" ", "")
                                    rate_coupon_val = float(
                                        rate_coupon_val) / 100
                                    self.pdw_df.at[
                                        'productYield.paymentRatePerPeriodFinal',  # noqa
                                        key] = rate_coupon_val
                                    dt_diff = pd.to_datetime(
                                        self.notes_dict[key]['Rates Schedule']
                                        ['From (including)'])
                                    dt_diff = dt_diff - dt_diff.shift()
                                    dt_days = dt_diff.mean().days
                                    self.pdw_df.at[
                                        'productYield.paymentRatePerAnnumFinal',  # noqa
                                        key] = rate_coupon_val / dt_days
                                    self.pdw_df.at[
                                        'productYield.paymentDateList',
                                        key] = pd.to_datetime(
                                            self.notes_dict[key]
                                            ['Rates Schedule']
                                            ['From (including)']).dt.strftime(
                                                r'%Y-%m-%d').to_list()
                                    self.pdw_df.at[
                                        'productYield.paymentDateList',
                                        key] = [{
                                            'paymentDate': val
                                        } for val in self.pdw_df.at[
                                            'productYield.paymentDateList',
                                            key]]
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(key, '_extendibleNote')] = (message, val)

    # Run all rules
    def run_all_rules(self):
        try:
            self.transpose_set_header()
        except TimeoutException:
            print("Timeout transpose_set_header!  Trying again.")

        try:
            self.label_note_tables()
        except TimeoutException:
            print("Timeout label_note_tables!  Trying again.")

        try:
            self.set_pdw_index()
        except TimeoutException:
            print("Timeout set_pdw_index!  Trying again.")

        try:
            self._PDW_Name()
        except TimeoutException:
            print("Timeout _PDW_Name!  Trying again.")

        try:
            self._callBarrierLevelFinal()
        except TimeoutException:
            print("Timeout _callBarrierLevelFinal!  Trying again.")

        try:
            self._callObservationDateList()
        except TimeoutException:
            print("Timeout _callObservationDateList!  Trying again.")

        try:
            self._callObservationFrequency()
        except TimeoutException:
            print("Timeout _callObservationFrequency!  Trying again.")

        try:
            self._callType()
        except TimeoutException:
            print("Timeout _callType!  Trying again.")

        try:
            self._numberNoCallPeriods()
        except TimeoutException:
            print("Timeout _numberNoCallPeriods!  Trying again.")

        try:
            self._currency()
        except TimeoutException:
            print("Timeout _currency!  Trying again.")

        try:
            self._cusip()
        except TimeoutException:
            print("Timeout _cusip!  Trying again.")

        try:
            self._issueDate()
        except TimeoutException:
            print("Timeout _issueDate!  Trying again.")

        try:
            self._issuer()
        except TimeoutException:
            print("Timeout _issuer!  Trying again.")

        try:
            self._maturityDate()
        except TimeoutException:
            print("Timeout _maturityDate!  Trying again.")

        try:
            self._productName()
        except TimeoutException:
            print("Timeout _productName!  Trying again.")

        try:
            self._stage()
        except TimeoutException:
            print("Timeout _stage!  Trying again.")

        try:
            self._status()
        except TimeoutException:
            print("Timeout _status!  Trying again.")

        try:
            self._tenorFinal()
        except TimeoutException:
            print("Timeout _tenorFinal!  Trying again.")

        try:
            self._tenorUnit()
        except TimeoutException:
            print("Timeout _tenorUnit!  Trying again.")

        try:
            self._underlierList()
        except TimeoutException:
            print("Timeout _underlierList!  Trying again.")

        try:
            self._underlierWeight()
        except TimeoutException:
            print("Timeout _underlierWeight!  Trying again.")

        try:
            self._upsideParticipationRateFinal()
        except TimeoutException:
            print("Timeout _upsideParticipationRateFinal!  Trying again.")

        try:
            self._principalBarrierLevelFinal()
        except TimeoutException:
            print("Timeout _principalBarrierLevelFinal!  Trying again.")

        try:
            self._paymentBarrierFinal()
        except TimeoutException:
            print("Timeout _paymentBarrierFinal!  Trying again.")

        try:
            self._paymentDateList()
        except TimeoutException:
            print("Timeout _paymentDateList!  Trying again.")

        try:
            self._paymentEvaluationFrequencyFinal()
        except TimeoutException:
            print("Timeout _paymentEvaluationFrequencyFinal!  Trying again.")

        try:
            self._paymentRatePerAnnumFinal()
        except TimeoutException:
            print("Timeout _paymentRatePerAnnumFinal!  Trying again.")

        try:
            self._paymentRatePerPeriodFinal()
        except TimeoutException:
            print("Timeout _paymentRatePerPeriodFinal!  Trying again.")

        try:
            self._fundservID()
        except TimeoutException:
            print("Timeout _fundservID!  Trying again.")

        try:
            self._mark_to_market_price()
        except TimeoutException:
            print("Timeout _mark_to_market_price!  Trying again.")

        try:
            self._minimumReturnFinal()
        except TimeoutException:
            print("Timeout _minimumReturnFinal!  Trying again.")

        try:
            self._tradeDate()
        except TimeoutException:
            print("Timeout _tradeDate!  Trying again.")

        try:
            self._callPremiumFinal()
        except TimeoutException:
            print("Timeout _callPremiumFinal!  Trying again.")

        try:
            self._putLeverageFinal()
        except TimeoutException:
            print("Timeout _putLeverageFinal!  Trying again.")

        try:
            self._extendibleNote()
        except TimeoutException:
            print("Timeout _extendibleNote!  Trying again.")

    @retry(stop_max_attempt_number=3)
    def reset_pdw_indices(self):
        # Reset indices to prepare to JSON
        with Timeout(seconds=self.timeout_sec):
            try:
                self.pdw_insert_df = self.pdw_df.copy()
                self.pdw_insert_df.drop(['PDW Name', 'Mark to Market Price'],
                                        inplace=True)
                self.pdw_insert_df.dropna(subset=self.pdw_df.columns,
                                          how='all',
                                          inplace=True)
                self.pdw_insert_df.reset_index(inplace=True)
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(self.pdw_insert_df,
                                  'reset_pdw_indices')] = message

    @retry(stop_max_attempt_number=3)
    def process_pdw_dicts(self):
        # Process for JSON
        self.pdw_df_dict = {}
        with Timeout(seconds=self.timeout_sec):
            for col in self.pdw_df.columns:
                try:
                    self.pdw_df_dict[col] = self.pdw_insert_df[[
                        'PDW Fields', col
                    ]].dropna()
                    self.pdw_df_dict[col] = pd.concat(
                        [
                            self.pdw_df_dict[col]['PDW Fields'].str.split(
                                '.', expand=True), self.pdw_df_dict[col]
                        ],
                        axis=1,
                    )
                    self.pdw_df_dict[col].drop(columns='PDW Fields',
                                               inplace=True)
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(col, 'process_pdw_dicts')] = message

    @retry(stop_max_attempt_number=3)
    def gen_pdw_json(self):
        # Convert to JSON & set up cxn
        self.result = {}
        with Timeout(seconds=self.timeout_sec):
            for col in self.pdw_df_dict.keys():
                try:
                    len_cols = list(
                        range(len(self.pdw_df_dict[col].columns) - 1))
                    pdw_pre_insert = self.pdw_df_dict[col].set_index(
                        len_cols).groupby(level=0).apply(
                            lambda x: x.xs(x.name)[col].to_dict()).to_dict()

                    # Prepare underlier list
                    if ('underlierList', nan
                        ) in pdw_pre_insert['productGeneral'].keys() and (
                            'underlierList', 'underlierWeight'
                        ) in pdw_pre_insert['productGeneral'].keys():
                        pdw_pre_insert['productGeneral']['underlierList'] = []
                        for sym, weight in zip(
                                pdw_pre_insert['productGeneral'][(
                                    'underlierList', nan)],
                                pdw_pre_insert['productGeneral'][(
                                    'underlierList', 'underlierWeight')]):
                            pdw_pre_insert['productGeneral'][
                                'underlierList'].append({
                                    'underlierSymbol':
                                    sym,
                                    'underlierWeight':
                                    weight,
                                    'underlierSource':
                                    'Bloomberg'
                                })
                        del pdw_pre_insert['productGeneral'][('underlierList',
                                                              nan)]
                        del pdw_pre_insert['productGeneral'][(
                            'underlierList', 'underlierWeight')]

                    # Prepare final JSON
                    pdw_insert = {}
                    for key, val in pdw_pre_insert.items():
                        pdw_insert[key] = {}
                        for key2, val2 in val.items():
                            if isinstance(key2, tuple):
                                if isinstance(key2[1], str):
                                    pdw_insert[key][key2[0]] = {key2[1]: val2}
                                else:
                                    pdw_insert[key][key2[0]] = val2
                            else:
                                pdw_insert[key][key2] = val2

                    # Final JSON assertions
                    required_fields = [
                        'productProtection',
                        'productCall',
                        'productYield',
                        'productGrowth',
                    ]
                    for field in required_fields:
                        if field not in pdw_insert.keys():
                            pdw_insert[field] = {}
                    if 'wrapperType' not in pdw_insert['productGeneral'].keys(
                    ):
                        pdw_insert['productGeneral']['wrapperType'] = 'Note'

                    # Export JSONS
                    self.result[col] = pdw_insert
                    self.result[col] = json.dumps(self.result[col])
                except Exception as e:
                    template = ("An exception of type {0} occurred. "
                                "Arguments:\n{1!r}")
                    message = template.format(type(e).__name__, e.args)
                    self.errors_dict[(col, 'insert_pdw_json_to_pdw')] = message

    def output_jsons(self):
        # The writing process as one method
        try:
            self.reset_pdw_indices()
        except TimeoutException:
            print("Timeout reset_pdw_indices!  Trying again.")

        try:
            self.process_pdw_dicts()
        except TimeoutException:
            print("Timeout process_pdw_dicts!  Trying again.")

        try:
            self.gen_pdw_json()
        except TimeoutException:
            print("Timeout gen_pdw_json!  Trying again.")


# %% Params
with open('urls_to_pdw.txt') as f:
    bmo_urls = f.read().splitlines()
# bmo_urls_sample = sample(bmo_urls, 50)
bmo_urls_sample = [
    'https://www.bmonotes.com/Note/JHN7482',
    'https://www.bmonotes.com/Note/JHN15954',
    'https://www.bmonotes.com/Note/JHN15093',
    'https://www.bmonotes.com/Note/JHN2058',
    'https://www.bmonotes.com/Note/JHN15992',
    'https://www.bmonotes.com/Note/06368DEW0',
    'https://www.bmonotes.com/Note/06368AV56',
    'https://www.bmonotes.com/note/06368D8L1',
]

# %% Add params to object
bmo = BmoScraper(bmo_urls_sample)

# %% Run all rules
bmo.run_all_rules()

# %% View Final results
pd.set_option('display.max_rows', 200)
bmo.pdw_df

# %% Write to PDW & view status
bmo.output_jsons()
bmo.result

# %% View errors
bmo.errors_dict

# %%
