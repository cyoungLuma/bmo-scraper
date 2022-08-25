# %% Libs
import json
import pandas as pd
from bs4 import BeautifulSoup
from numpy import nan
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen
from tqdm import tqdm

# %% Read in the examples


class BmoScraper:
    # Pass in note URLs & lookup for PDW
    def __init__(
        self,
        bmo_urls,
    ):
        self.notes_dict = {}
        self.errors_dict = {}
        for note in tqdm(bmo_urls):
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
                            'productCall.callBarrierLevelFinal', key] = float(
                                self.notes_dict[key]['Payment Schedule']
                                ['Autocall Level'].loc[mask].iloc[0].strip(
                                    '%').replace(" ", "")) / 100
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
                    if 'Observation Date' in val['Payment Schedule'].columns:
                        # Add the entire observation date column as a list
                        self.pdw_df.at[
                            'productCall.callObservationDateList',
                            key] = pd.to_datetime(
                                self.notes_dict[key]['Payment Schedule']
                                ['Observation Date']).dt.strftime(
                                    r'%Y-%m-%d').to_list()
                        self.pdw_df.at[
                            'productCall.callObservationDateList', key] = [{
                                'callObservationDate':
                                val
                            } for val in bmo.pdw_df.at[
                                'productCall.callObservationDateList', key]]

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

        try:
            for key, val in self.notes_dict.items():
                # Check if right type of note
                if 'Payment Schedule' in val.keys():
                    if 'Observation Date' in val['Payment Schedule'].columns:
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
                    if 'Autocall Level' in val['Payment Schedule'].columns:
                        # Check if all values are the same
                        autocall_series = self.notes_dict[
                            key]['Payment Schedule']['Autocall Level'].dropna(
                            ).reset_index(drop=True)
                        if (autocall_series == autocall_series[0]).all():
                            # self.pdw_df.at['productCall.callType', key] = 'Autocall'
                            self.pdw_df.at['productCall.callType',
                                           key] = 'Auto'
                        else:
                            # self.pdw_df.at['productCall.callType', key] = 'Auto Step'
                            self.pdw_df.at['productCall.callType',
                                           key] = 'Autocall Step'
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
                    if 'Autocall Level' in val['Payment Schedule'].columns:
                        # Filter to NaN + 1
                        self.pdw_df.at[
                            'productCall.numberNoCallPeriods', key] = len(
                                self.notes_dict[key]['Payment Schedule'].loc[
                                    self.notes_dict[key]['Payment Schedule']
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
                    if 'Currency' in val['Product Details'].columns:
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
                                ['Issue Date']).dt.strftime(r'%Y-%m-%d')[0]
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
                                ['Maturity Date']).dt.strftime(r'%Y-%m-%d')[0]
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
            self.pdw_df.at['productGeneral.stage', key] = 'Ops Review'

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
                                'Term'][0].split()[1].title()
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
                    if 'Linked To' in val[
                            'Product Details'].columns and ',' in self.notes_dict[
                                key]['Product Details']['Linked To'][0]:
                        self.pdw_df.at[
                            'productGeneral.underlierList',
                            key] = self.notes_dict[key]['Product Details'][
                                'Linked To'][0].replace(' ', '').split(',')
                    else:
                        self.pdw_df.at['productGeneral.underlierList',
                                       key] = [{
                                           'underlierSymbol':
                                           self.notes_dict[key]
                                           ['Product Details']['Linked To'][0],
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
    def _underlierWeight(self):
        # Check for portfolio summary section and get weights
        for key, val in self.notes_dict.items():
            try:
                if 'Portfolio Summary' in val.keys():
                    if 'Share Weight' in val['Portfolio Summary'].columns:
                        underlier_weight = val['Portfolio Summary'].loc[
                            val['Portfolio Summary']['Share Weight'].str.
                            contains('%', na=False),
                            'Share Weight'].str.replace('%',
                                                        '').astype(float) / 100
                        self.pdw_df.at[
                            'productGeneral.underlierList.underlierWeight',
                            key] = underlier_weight.to_list()
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_underlierWeight')] = (message, val)

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
    def _principalBarrierLevelFinal(self):
        # Get value from table & convert to float
        for key, val in self.notes_dict.items():
            try:
                # These can be negative!
                if 'Product Details' in val.keys():
                    if 'Barrier Protection' in val['Product Details'].columns:
                        barrier_val = float(
                            self.notes_dict[key]['Product Details']
                            ['Barrier Protection'][0].replace('%', '').replace(
                                " ", "")) / 100
                        self.pdw_df.at[
                            'productProtection.principalBarrierLevelFinal',
                            key] = barrier_val + 1
                        self.pdw_df.at['productProtection.protectionLevel',
                                       key] = barrier_val * -1
                        self.pdw_df.at['productProtection.downsideType',
                                       key] = 'Barrier'
                        self.pdw_df.at['productProtection.putLeverageFinal',
                                       key] = 1
                        self.pdw_df.at['productProtection.putStrikeFinal',
                                       key] = barrier_val + 1
                    elif 'Buffer Protection' in val['Product Details'].columns:
                        buffer_val = float(
                            self.notes_dict[key]['Product Details']
                            ['Buffer Protection'][0].replace('%', '').replace(
                                " ", "")) / 100
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
                self.errors_dict[(key,
                                  '_principalBarrierLevelFinal')] = (message,
                                                                     val)

    # # Rule: countryDistribution
    # def _countryDistribution(self):
    #     # Hardcode
    #     for key in self.notes_dict.keys():
    #         self.pdw_df.at['productRegulatory.countryDistribution',
    #                        key] = 'CANADA'

    # Rule: paymentBarrierFinal
    def _paymentBarrierFinal(self):
        # Grab field from table & convert to float
        try:
            for key, val in self.notes_dict.items():
                if 'Product Details ' in val.keys():
                    if 'Coupon Knock-Out Level' in val[
                            'Product Details '].columns:
                        coupon_val = float(
                            self.notes_dict[key]['Indicative Return']
                            ['Coupon Knock-Out Level'][0].replace(
                                '-', '').replace('%', '').replace(" ",
                                                                  "")) / 100
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
                            key] = pd.to_datetime(
                                self.notes_dict[key]['Payment Schedule']
                                ['Coupon Payment Date']).dt.strftime(
                                    r'%Y-%m-%d').to_list()
                        self.pdw_df.at['productYield.paymentDateList',
                                       key] = [{
                                           'paymentDate': val
                                       } for val in bmo.pdw_df.at[
                                           'productYield.paymentDateList',
                                           key]]
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
                                'Pay Frequency'][0].title()
                        self.pdw_df.at[
                            'productYield.paymentFrequency',
                            key] = self.notes_dict[key]['Product Details'][
                                'Pay Frequency'][0].title()
                    elif 'Coupon Frequency' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productYield.paymentFrequency',
                            key] = self.notes_dict[key]['Product Details'][
                                'Coupon Frequency'][0].title()
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
    def _paymentRatePerPeriodFinal(self):
        # Take previous rule results to calculate this if exists
        for col in self.pdw_df.columns:
            try:
                cond1 = self.pdw_df.at['productYield.paymentRatePerAnnumFinal',
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
                    self.pdw_df.at['productYield.paymentRatePerPeriodFinal',
                                   col] = self.pdw_df.at[
                                       'productYield.paymentRatePerAnnumFinal',
                                       col] / cond3[self.pdw_df.at[
                                           'productYield.paymentFrequency',
                                           col]]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(col, 'paymentRatePerPeriodFinal')] = message

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

    # Rule: minimumReturnFinal
    def _minimumReturnFinal(self):
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Minimum Payment' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productGrowth.minimumReturnFinal', key] = float(
                                self.notes_dict[key]['Product Details']
                                ['Minimum Payment'][0].replace('$', ''))
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_minimumReturnFinal')] = (message, val)

    # Rule: minimumReturnFinal
    def _tradeDate(self):
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Available Until' in val['Product Details'].columns:
                        self.pdw_df.at[
                            'productGeneral.tradeDate', key] = pd.to_datetime(
                                self.notes_dict[key]['Product Details']
                                ['Available Until']).dt.strftime(
                                    r'%Y-%m-%d')[0]
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_tradeDate')] = (message, val)

    # Rule: callPremiumFinal
    def _callPremiumFinal(self):
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'AutoCall Coupon (Next Call Date)' in val[
                            'Product Details'].columns:
                        self.pdw_df.at[
                            'productCall.callPremiumFinal', key] = float(
                                self.notes_dict[key]['Product Details']
                                ['AutoCall Coupon (Next Call Date)']
                                [0].replace('-', '').replace('%', '').replace(
                                    " ", "")) / 100
                        if self.pdw_df.at['productCall.callPremiumFinal',
                                          key] > 1:
                            self.pdw_df.at[
                                'productProtection.downsideType'] = 'Geared Buffer'
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_callPremiumFinal')] = (message, val)

    # Rule: putLeverageFinal
    def _putLeverageFinal(self):
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys():
                    if 'Downside Participation' in val[
                            'Product Details'].columns:
                        self.pdw_df.at[
                            'productProtection.putLeverageFinal', key] = float(
                                self.notes_dict[key]['Product Details']
                                ['Downside Participation'][0].replace(
                                    '-', '').replace('%', '').replace(
                                        " ", "")) / 100

            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_putLeverageFinal')] = (message, val)

    # Rule: extendibleNote
    def _extendibleNote(self):
        for key, val in self.notes_dict.items():
            try:
                if 'Product Details' in val.keys(
                ) and 'Rates Schedule' in val.keys():
                    if 'Product SubType' in val['Product Details'].columns:
                        if val['Product SubType'][
                                0] != 'Extendible Step-up Note':
                            if (val['Rate/Coupon'] == val['Rate/Coupon'][0]
                                ).all():
                                rate_coupon_val = float(
                                    self.notes_dict[key]['Rates Schedule']
                                    ['Rate/Coupon'][0].replace(
                                        '%', '').replace(" ", "")) / 100
                                self.pdw_df.at[
                                    'productYield.paymentRatePerPeriodFinal',
                                    key] = rate_coupon_val
                                dt_diff = pd.to_datetime(
                                    self.notes_dict[key]['Rates Schedule']
                                    ['From (including)'])
                                dt_diff = dt_diff - dt_diff.shift()
                                dt_days = dt_diff.mean().days
                                self.pdw_df.at[
                                    'productYield.paymentRatePerAnnumFinal'] = rate_coupon_val / dt_days
                                self.pdw_df.at[
                                    'productYield.paymentDateList'] = pd.to_datetime(
                                        self.notes_dict[key]['Rates Schedule']
                                        ['From (including)']).dt.strftime(
                                            r'%Y-%m-%d').to_list()
                                self.pdw_df.at[
                                    'productYield.paymentDateList', key] = [{
                                        'paymentDate':
                                        val
                                    } for val in bmo.pdw_df.at[
                                        'productYield.paymentDateList', key]]

            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(key, '_putLeverageFinal')] = (message, val)

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
        self._underlierWeight()
        self._upsideParticipationRateFinal()
        self._principalBarrierLevelFinal()
        # self._countryDistribution()
        self._paymentBarrierFinal()
        self._paymentDateList()
        self._paymentEvaluationFrequencyFinal()
        self._paymentRatePerAnnumFinal()
        self._paymentRatePerPeriodFinal()
        self._fundservID()
        self._mark_to_market_price()
        self._minimumReturnFinal()
        self._tradeDate()
        self._callPremiumFinal()
        self._putLeverageFinal()
        self._extendibleNote()

    def reset_pdw_indices(self):
        # Reset indices to prepare to JSON
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

    def process_pdw_dicts(self):
        # Process for JSON
        self.pdw_df_dict = {}
        for col in self.pdw_df.columns:
            try:
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
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(col, 'process_pdw_dicts')] = message

    def gen_pdw_json(self):
        # Convert to JSON & set up cxn
        self.result = {}
        # db = self.client['test-masking-dev']
        # PdwProductCore = db.PdwProductCore
        for col in self.pdw_df_dict.keys():
            try:
                len_cols = list(range(len(self.pdw_df_dict[col].columns) - 1))
                pdw_pre_insert = self.pdw_df_dict[col].set_index(
                    len_cols).groupby(level=0).apply(
                        lambda x: x.xs(x.name)[col].to_dict()).to_dict()

                # Prepare underlier list
                if ('underlierList',
                        nan) in pdw_pre_insert['productGeneral'].keys() and (
                            'underlierList', 'underlierWeight'
                        ) in pdw_pre_insert['productGeneral'].keys():
                    pdw_pre_insert['productGeneral']['underlierList'] = []
                    for sym, weight in zip(
                            pdw_pre_insert['productGeneral'][('underlierList',
                                                              nan)],
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
                    del pdw_pre_insert['productGeneral'][('underlierList',
                                                          'underlierWeight')]

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
                    # 'productGeneral.wrapperType',
                ]
                for field in required_fields:
                    if field not in pdw_insert.keys():
                        pdw_insert[field] = {}
                if 'wrapperType' not in pdw_insert['productGeneral'].keys():
                    pdw_insert['productGeneral']['wrapperType'] = 'Note'

                # Insert into DB
                # try:
                # self.result[col] = (PdwProductCore.insert_one(pdw_insert),
                #                     pdw_insert)
                #     self.result[col] = pdw_insert
                # except DuplicateKeyError:
                #     self.result[col] = ('Product exists', pdw_insert)
                self.result[col] = pdw_insert
                self.result[col] = json.dumps(self.result[col])
            except Exception as e:
                template = ("An exception of type {0} occurred. "
                            "Arguments:\n{1!r}")
                message = template.format(type(e).__name__, e.args)
                self.errors_dict[(col, 'insert_pdw_json_to_pdw')] = message

    def output_jsons(self):
        # The writing process as one method
        self.reset_pdw_indices()
        self.process_pdw_dicts()
        self.gen_pdw_json()


# # %% Params
# bmo_urls_sample = [
#     'https://www.bmonotes.com/Note/JHN7482',
#     'https://www.bmonotes.com/Note/JHN15954',
#     'https://www.bmonotes.com/Note/JHN15093',
#     'https://www.bmonotes.com/Note/JHN2058',
#     'https://www.bmonotes.com/Note/JHN15992',
# ]

# bmo = BmoScraper(bmo_urls_sample)

# # %% Run all rules
# bmo.run_all_rules()

# # %% View Final results
# pd.set_option('display.max_rows', 200)
# bmo.pdw_df

# # %% Write to PDW & view status
# bmo.output_jsons()


# # %% View any errors that were caught
# bmo.errors_dict

