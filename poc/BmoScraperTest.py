# %% Libs
from urllib.error import HTTPError
import pandas as pd
from bs4 import BeautifulSoup
from keyring import get_password
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from random import sample
from time import sleep
from urllib.request import urlopen


# %% Read in the examples
class BmoScraper:
    # Pass in note URLs & lookup for PDW
    def __init__(self, bmo_urls, user, password, host, port, options):
        cxn_string = f"mongodb://{user}:{password}@{host}:{port}/?{options}"
        self.client = MongoClient(cxn_string)
        self.notes_dict = {}
        self.errors_dict = {}
        for note in bmo_urls:
            try:
                try:
                    try:
                        self.notes_dict[note.rsplit(
                            '/', 1)[-1]] = pd.read_html(note)
                        sleep(3)
                    except HTTPError:
                        print('HTTP Error: Waiting 10 seconds and trying again'
                              ' for the current item')
                        sleep(10)
                        self.notes_dict[note.rsplit(
                            '/', 1)[-1]] = pd.read_html(note)
                        sleep(3)
                except HTTPError:
                    print('HTTP Error: Waiting 30 seconds and trying again'
                          ' for the current item')
                    sleep(30)
                    self.notes_dict[note.rsplit('/',
                                                1)[-1]] = pd.read_html(note)
                    sleep(3)
            except HTTPError:
                message = (f'Note {note} failed to read after 3 attemps.  '
                           'Logging for investigation.')
                self.errors_dict[(note, '__init__')] = message

        self.bmo_example_fields = pd.read_excel('BMO Examples.xlsx')
        self.pdw_df = self.bmo_example_fields[['PDW Fields']].copy()
        self.skip_cols = pd.Series(['Payment Schedule', 'Portfolio Summary'])

    # Get all scraping results as a single row table
    def transpose_set_header(self):

        # Additional fxn to recycle code
        def reassign_cols_truncate(self, key, i):
            self.notes_dict[key][i].columns = self.notes_dict[key][i].iloc[0]
            self.notes_dict[key][i] = self.notes_dict[key][i][1:]

        # This checks if the scrape results are a true table or not
        for key, val in self.notes_dict.items():
            for i in range(len(val)):
                if ~self.skip_cols.isin(self.notes_dict[key][i].columns).any():
                    self.notes_dict[key][i] = self.notes_dict[key][i].T
                    reassign_cols_truncate(self, key, i)
                else:
                    table_name = self.notes_dict[key][i].columns[0]
                    reassign_cols_truncate(self, key, i)
                    self.notes_dict[key][i].columns.name = table_name

    # Convert all to dict to navigate indices
    def label_note_tables(self):
        self.notes_dict = {
            key: {val[i].columns.name: val[i]
                  for i in range(len(val))}
            for key, val in self.notes_dict.items()
        }

    # Set PDW index
    def set_pdw_index(self):
        self.pdw_df.set_index('PDW Fields', inplace=True)

    # Rule: PDW Name
    def _PDW_Name(self):
        for key in self.notes_dict.keys():
            self.pdw_df[key] = None
            self.pdw_df.at['PDW Name',
                           key] = 'https://www.bmonotes.com/Note/' + key

    # Rule: callBarrierLevelFinal
    def _callBarrierLevelFinal(self):
        for key, val in self.notes_dict.items():
            # Check if right type of note
            try:
                if 'Payment Schedule' in val.keys():
                    # Replace '-' with NaN
                    self.notes_dict[key]['Payment Schedule'].replace(
                        '-', None, inplace=True, regex=False)

                    # Set filter to first non-null value
                    mask = ~self.notes_dict[key]['Payment Schedule'][
                        'Autocall Level'].isnull()

                    # Set value in the PDW table
                    self.pdw_df.at['productCall.callBarrierLevelFinal',
                                   key] = float(
                                       self.notes_dict[key]['Payment Schedule']
                                       ['Autocall Level'].loc[mask].iloc[0].
                                       strip('%')) / 100
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_callBarrierLevelFinal')] = (message,
                                                                     val)

    # Rule: callObservationDateList
    def _callObservationDateList(self):
        for key, val in self.notes_dict.items():
            # Check if right type of note
            try:
                if 'Payment Schedule' in val.keys():
                    # Add the entire observation date column as a list
                    self.pdw_df.at[
                        'productCall.callObservationDateList',
                        key] = self.notes_dict[key]['Payment Schedule'][
                            'Observation Date'].to_list()
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_callObservationDateList')] = (message,
                                                                       val)

    # Rule: callObservationFrequency
    def _callObservationFrequency(self):

        def check_call_freq(self, dt_days):
            if 2 <= dt_days <= 5:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'BI_WEEKLY'
            if 6 <= dt_days <= 7:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'WEEKLY'
            if 28 <= dt_days <= 31:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'MONTHLY'
            elif 14 <= dt_days <= 16:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'BI_MONTHLY'
            elif dt_days == 1:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'DAILY'
            elif 364 <= dt_days <= 366:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'ANNUALLY'
            elif 182 <= dt_days <= 184:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'SEMI_ANNUALLY'
            elif 89 <= dt_days <= 92:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'QUARTERLY'
            else:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'CUSTOM'

        try:
            for key, val in self.notes_dict.items():
                # Check if right type of note
                if 'Payment Schedule' in val.keys():
                    # Add the entire observation date column as a list
                    dt_diff = pd.to_datetime(
                        self.notes_dict[key]['Payment Schedule']
                        ['Observation Date'])
                    dt_diff = dt_diff - dt_diff.shift()
                    dt_days = dt_diff.mean().days
                    check_call_freq(self, dt_days)
        except Exception as e:
            template = ("An exception of type {0} occurred. "
                        "Arguments:\n{1!r}")
            message = template.format(type(e).__name__, e.args)
            self.errors_dict[(key, '_callObservationFrequency')] = (message,
                                                                    val)

    # Rule: callType
    def _callType(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            try:
                if 'Payment Schedule' in val.keys():
                    # Check if all values are the same
                    autocall_series = self.notes_dict[key]['Payment Schedule'][
                        'Autocall Level'].dropna().reset_index(drop=True)
                    if (autocall_series == autocall_series[0]).all():
                        # self.pdw_df.at['productCall.callType', key] = 'Autocall'
                        self.pdw_df.at['productCall.callType', key] = 'AUTO'
                    else:
                        # self.pdw_df.at['productCall.callType', key] = 'Auto Step'
                        self.pdw_df.at['productCall.callType',
                                       key] = 'AUTOCALL_STEP'
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_callType')] = (message, val)

    # Rule: numberNoCallPeriods
    def _numberNoCallPeriods(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            try:
                if 'Payment Schedule' in val.keys():
                    # Filter to NaN + 1
                    self.pdw_df.at[
                        'productCall.numberNoCallPeriods',
                        key] = len(self.notes_dict[key]['Payment Schedule'].
                                   loc[self.notes_dict[key]['Payment Schedule']
                                       ['Autocall Level'].isna()]) + 1
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_numberNoCallPeriods')] = (message,
                                                                   val)

    # Rule: currency
    def _currency(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Currency' in val.keys():
                        # Get currency column
                        self.pdw_df.at['productGeneral.currency',
                                       key] = self.notes_dict[key][
                                           'Product Details']['Currency'][0]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_currency')] = (message, val)

    # Rule: cusip
    def _cusip(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            try:
                # Get JHN column and correct length
                if 'Product Details' in val.keys():
                    if 'JHN Code' in val['Product Details'].columns:
                        jhn = self.notes_dict[key]['Product Details'][
                            'JHN Code'][0]
                        if len(jhn) == 7:
                            self.pdw_df.at['productGeneral.cusip',
                                           key] = 'CA' + jhn
                        elif len(jhn) == 8:
                            self.pdw_df.at['productGeneral.cusip',
                                           key] = 'C' + jhn
                        else:
                            self.pdw_df.at['productGeneral.cusip', key] = jhn
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_cusip')] = (message, val)

    # Rule: issueDate
    def _issueDate(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            try:
                # Get date in right format
                if 'Product Details' in val.keys():
                    if 'Issue Date' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productGeneral.issueDate', key] = pd.to_datetime(
                                self.notes_dict[key]['Product Details']
                                ['Issue Date']).dt.strftime(r'%m/%d/%Y')[0]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_issueDate')] = (message, val)

    # Rule: issuer
    def _issuer(self):
        for key in self.notes_dict.keys():
            # Hardcode for now
            self.pdw_df.at['productGeneral.issuer', key] = 'Bank of Montreal'

    # Rule: maturityDate
    def _maturityDate(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            try:
                # Get date in right format
                if 'Product Details' in val.keys():
                    if 'Maturity Date' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productGeneral.maturityDate',
                            key] = pd.to_datetime(
                                self.notes_dict[key]['Product Details']
                                ['Maturity Date']).dt.strftime(r'%m/%d/%Y')[0]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_maturityDate')] = (message, val)

    # Rule: productName
    def _productName(self):
        # Get title of webpages
        try:
            for key in self.notes_dict.keys():
                soup = BeautifulSoup(urlopen('https://www.bmonotes.com/Note/' +
                                             key),
                                     features="lxml")
                self.pdw_df.at['productGeneral.productName',
                               key] = str(soup.find_all('h1')[1]).replace(
                                   r'<h1>', '').replace(r'</h1>', '').strip()
        except Exception as e:
            template = ("An exception of type {0} occurred. "
                        "Arguments:\n{1!r}")
            message = template.format(type(e).__name__, e.args)
            self.errors_dict[(key, '_productName')] = message

    # Rule: stage
    def _stage(self):
        # Simple hardcode
        for key in self.notes_dict.keys():
            self.pdw_df.at['productGeneral.stage', key] = 'OPS_REVIEW'

    # Rule: status
    def _status(self):
        # Simple hardcode
        for key in self.notes_dict.keys():
            self.pdw_df.at['productGeneral.status',
                           key] = 'Update Product Details'

    # Rule: tenorFinal
    def _tenorFinal(self):
        # Get Term value
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Term' in val['Product Details'].columns:
                        self.pdw_df.at['productGeneral.tenorFinal',
                                       key] = float(self.notes_dict[key]
                                                    ['Product Details']['Term']
                                                    [0].split()[0])
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_tenorFinal')] = (message, val)

    # Rule: tenorUnit
    def _tenorUnit(self):
        # Get Term unit
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Term' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productGeneral.tenorUnit',
                            key] = self.notes_dict[key]['Product Details'][
                                'Term'][0].split()[1].upper()
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_tenorUnit')] = (message, val)

    # Rule: underlierList
    def _underlierList(self):
        try:
            # Get value from table
            for key, val in self.notes_dict.items():
                if 'Product Details' in val.keys():
                    if 'Linked To' in val['Product Details'].columns:
                        self.pdw_df.at['productGeneral.underlierList',
                                       key] = self.notes_dict[key][
                                           'Product Details']['Linked To'][0]
        except Exception as e:
            template = ("An exception of type {0} occurred. "
                        "Arguments:\n{1!r}")
            message = template.format(type(e).__name__, e.args)
            self.errors_dict[(key, '_underlierList')] = (message, val)

    # Rule: underlierweight
    def _underlierweight(self):
        # Check for portfolio summary section and get weights
        for key, val in self.notes_dict.items():
            try:
                if 'Portfolio Summary' in val.keys():
                    underlier_weight = self.notes_dict[key][
                        'Portfolio Summary'].iloc[:-1, :][
                            'Share Weight'].str.replace('%',
                                                        '').astype(float) / 100
                    self.pdw_df.at[
                        'productGeneral.underlierList.underlierweight',
                        key] = underlier_weight.to_list()
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_underlierweight')] = (message, val)

    # Rule: upsideParticipationRateFinal
    def _upsideParticipationRateFinal(self):
        # Get value from table & convert to float
        try:
            for key, val in self.notes_dict.items():
                if 'Product Details' in val.keys():
                    if 'Upside Participation' in val[
                            'Product Details'].columns:
                        self.pdw_df.at[
                            'productGrowth.upsideParticipationRateFinal',
                            key] = float(
                                self.notes_dict[key]['Product Details']
                                ['Upside Participation'][0].replace('%',
                                                                    '')) / 100
        except Exception as e:
            template = ("An exception of type {0} occurred. "
                        "Arguments:\n{1!r}")
            message = template.format(type(e).__name__, e.args)
            self.errors_dict[(key,
                              '_upsideParticipationRateFinal')] = (message,
                                                                   val)

    # Rule: downsideType
    def _downsideType(self):
        # If exists, static
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Barrier Protection' in val['Product Details'].columns:
                        self.pdw_df.at['productProtection.downsideType',
                                       key] = 'BARRIER'
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_downsideType')] = (message, val)

    # Rule: principalBarrierLevelFinal
    def _principalBarrierLevelFinal(self):
        # Get value from table & convert to float
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Barrier Protection' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productProtection.principalBarrierLevelFinal',
                            key] = float(
                                self.notes_dict[key]['Product Details']
                                ['Barrier Protection'][0].replace(
                                    '-', '').replace('%', '').strip()) / 100
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key,
                                  '_principalBarrierLevelFinal')] = (message,
                                                                     val)

    # Rule: countryDistribution
    def _countryDistribution(self):
        # Hardcode
        for key in self.notes_dict.keys():
            self.pdw_df.at['productRegulatory.countryDistribution',
                           key] = 'CANADA'

    # Rule: paymentBarrierFinal
    def _paymentBarrierFinal(self):
        # Grab field from table & convert to float
        try:
            for key, val in self.notes_dict.items():
                if 'Indicative Return' in val.keys():
                    if 'Coupon Knock-Out Level  (Basket Return)' in val[
                            'Indicative Return'].columns:
                        self.pdw_df.at[
                            'productYield.paymentBarrierFinal', key] = float(
                                self.notes_dict[key]['Indicative Return']
                                ['Coupon Knock-Out Level  (Basket Return)']
                                [0].replace('-', '').replace('%',
                                                             '').strip()) / 100
        except Exception as e:
            template = ("An exception of type {0} occurred. "
                        "Arguments:\n{1!r}")
            message = template.format(type(e).__name__, e.args)
            self.errors_dict[(key, '_paymentBarrierFinal')] = (message, val)

    # Rule: paymentDateList
    def _paymentDateList(self):
        # Add column as list
        for key, val in self.notes_dict.items():
            try:
                if 'Payment Schedule' in val.keys():
                    if 'Coupon Payment Date' in val[
                            'Payment Schedule'].columns:
                        self.pdw_df.at[
                            'productYield.paymentDateList',
                            key] = self.notes_dict[key]['Payment Schedule'][
                                'Coupon Payment Date'].to_list()
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_paymentDateList')] = (message, val)

    # Rule: paymentEvaluationFrequencyFinal
    def _paymentEvaluationFrequencyFinal(self):
        # If exists, static
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Pay Frequency' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productYield.paymentEvaluationFrequencyFinal',
                            key] = self.notes_dict[key]['Product Details'][
                                'Pay Frequency'][0].upper()
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_paymentEvaluationFrequencyFinal')] = (
                    message, val)

    # Rule: paymentRatePerAnnumFinal
    def _paymentRatePerAnnumFinal(self):
        # If exists, static & convert % to float
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Contingent Coupon' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productYield.paymentRatePerAnnumFinal',
                            key] = float(
                                self.notes_dict[key]['Product Details']
                                ['Contingent Coupon'][0].replace(
                                    '-', '').replace('%', '').strip()) / 100
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key,
                                  '_paymentRatePerAnnumFinal')] = (message,
                                                                   val)

    # Rule: fundservID
    def _fundservID(self):
        # If exists, static
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'JHN Code' in val['Product Details'].columns:
                        self.pdw_df.at['productGeneral.fundservID',
                                       key] = self.notes_dict[key][
                                           'Product Details']['JHN Code'][0]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_fundservID')] = (message, val)

    # Rule: Mark to Market Price
    def _mark_to_market_price(self):
        # If exists, static & replace $, convert to float
        for key, val in self.notes_dict.items():
            try:
                if 'Current Status' in val.keys():
                    if 'Current Bid Price' in val['Current Status'].columns:
                        if val['Current Status']['Current Bid Price'][0] != '-':
                            self.pdw_df.at[
                                'Mark to Market Price', key] = float(
                                    self.notes_dict[key]['Current Status']
                                    ['Current Bid Price'][0].replace('$', ''))
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_mark_to_market_price')] = (message,
                                                                    val)

    # Run all rules
    def run_all_rules(self):
        self.transpose_set_header()
        self.label_note_tables()
        self.set_pdw_index()
        self._PDW_Name()
        self._callBarrierLevelFinal()
        self._callObservationDateList()
        self._callObservationFrequency()
        self._callType()
        self._numberNoCallPeriods()
        self._currency()
        self._cusip()
        self._issueDate()
        self._issuer()
        self._maturityDate()
        self._productName()
        self._stage()
        self._status()
        self._tenorFinal()
        self._tenorUnit()
        self._underlierList()
        self._underlierweight()
        self._upsideParticipationRateFinal()
        self._downsideType()
        self._principalBarrierLevelFinal()
        self._countryDistribution()
        self._paymentBarrierFinal()
        self._paymentDateList()
        self._paymentEvaluationFrequencyFinal()
        self._paymentRatePerAnnumFinal()
        self._fundservID()
        self._mark_to_market_price()

    def reset_pdw_indices(self):
        # Reset indices to prepare to JSON
        self.pdw_insert_df = self.pdw_df.copy()
        self.pdw_insert_df.drop(['PDW Name', 'Mark to Market Price'],
                                inplace=True)
        self.pdw_insert_df.dropna(subset=self.pdw_df.columns,
                                  how='all',
                                  inplace=True)
        self.pdw_insert_df.reset_index(inplace=True)

    def process_pdw_dicts(self):
        # Process for JSON
        self.pdw_df_dict = {}
        for col in self.pdw_df.columns:
            self.pdw_df_dict[col] = self.pdw_insert_df[['PDW Fields',
                                                        col]].dropna()
            self.pdw_df_dict[col] = pd.concat(
                [
                    self.pdw_df_dict[col]['PDW Fields'].str.split(
                        '.', expand=True), self.pdw_df_dict[col]
                ],
                axis=1,
            )
            self.pdw_df_dict[col].drop(columns='PDW Fields', inplace=True)

    def insert_pdw_json_to_pdw(self):
        # Convert to JSON & set up cxn
        self.result = {}
        db = self.client['test-masking-dev']
        PdwProductCore = db.PdwProductCore
        for col in self.pdw_df_dict.keys():
            len_cols = list(range(len(self.pdw_df_dict[col].columns) - 1))
            pdw_pre_insert = self.pdw_df_dict[col].set_index(len_cols).groupby(
                level=0).apply(
                    lambda x: x.xs(x.name)[col].to_dict()).to_dict()

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

            # Insert into DB
            try:
                self.result[col] = (PdwProductCore.insert_one(pdw_insert),
                                    pdw_insert)
            except DuplicateKeyError:
                self.result[col] = ('Product exists', pdw_insert)

    def write_to_pdw(self):
        # The writing process as one method
        self.reset_pdw_indices()
        self.process_pdw_dicts()
        self.insert_pdw_json_to_pdw()


# %% Params
with open('urls_to_pdw.txt') as f:
    bmo_urls = f.read().splitlines()
bmo_urls_sample = sample(bmo_urls, 20)
user = "skimble"
password = get_password('docdb_preprod', user)
host = "dev-documentdb.cluster-cb6kajicuplh.us-east-1.docdb.amazonaws.com"
port = "27017"
options = ("tls=true&tlsAllowInvalidCertificates=true&replicaSet=rs0&"
           "readPreference=secondaryPreferred&retryWrites=false")

# %% Add params to object
bmo = BmoScraper(bmo_urls_sample, user, password, host, port, options)

# %% Run all rules
bmo.run_all_rules()

# %% View Final results
pd.set_option('display.max_rows', 200)
bmo.pdw_df

# %% Write to PDW & view status
bmo.write_to_pdw()
bmo.result

# %% View any errors
bmo.errors_dict
