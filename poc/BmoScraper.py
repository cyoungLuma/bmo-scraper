# %% Libs
import pandas as pd

pd.set_option('display.max_rows', 200)


# %% Read in the examples
class BmoScraper:
    # Pass in note URLs & lookup for PDW
    def __init__(self, bmo_urls):
        self.notes_dict = {
            note.rsplit('/', 1)[-1]: pd.read_html(note)
            for note in bmo_urls
        }
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
                if ~bmo.skip_cols.isin(bmo.notes_dict[key][i].columns).any():
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
            if 'Payment Schedule' in val.keys():
                # Replace '-' with NaN
                self.notes_dict[key]['Payment Schedule'].replace('-',
                                                                 None,
                                                                 inplace=True,
                                                                 regex=False)

                # Set filter to first non-null value
                mask = ~self.notes_dict[key]['Payment Schedule'][
                    'Autocall Level'].isnull()

                # Set value in the PDW table
                self.pdw_df.at[
                    'productCall.callBarrierLevelFinal', key] = float(
                        self.notes_dict[key]['Payment Schedule']
                        ['Autocall Level'].loc[mask].iloc[0].strip('%')) / 100

    # Rule: callObservationDateList
    def _callObservationDateList(self):
        for key, val in self.notes_dict.items():
            # Check if right type of note
            if 'Payment Schedule' in val.keys():
                # Add the entire observation date column as a list
                self.pdw_df.at['productCall.callObservationDateList',
                               key] = self.notes_dict[key]['Payment Schedule'][
                                   'Observation Date'].to_list()

    # Rule: callObservationFrequency
    def _callObservationFrequency(self):

        def check_call_freq(self, dt_days):
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
                               key] = 'ANUALLY'
            elif 89 <= dt_days <= 92:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'QUARTERLY'
            else:
                self.pdw_df.at['productCall.callObservationFrequency',
                               key] = 'CUSTOM'

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

    # Rule: callType
    def _callType(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            if 'Payment Schedule' in val.keys():
                # Check if all values are the same
                autocall_series = self.notes_dict[key]['Payment Schedule'][
                    'Autocall Level'].dropna().reset_index(drop=True)
                if (autocall_series == autocall_series[0]).all():
                    self.pdw_df.at['productCall.callType', key] = 'Autocall'
                else:
                    self.pdw_df.at['productCall.callType', key] = 'Auto Step'

    def _numberNoCallPeriods(self):
        # Check if right type of note
        for key, val in self.notes_dict.items():
            if 'Payment Schedule' in val.keys():
                # Filter to NaN + 1
                self.pdw_df.at['productCall.numberNoCallPeriods', key] = len(
                    self.notes_dict[key]['Payment Schedule'].loc[
                        self.notes_dict[key]['Payment Schedule']
                        ['Autocall Level'].isna()]) + 1


# %% Set of URLs
bmo_urls = [
    'https://www.bmonotes.com/Note/JHN7482',
    'https://www.bmonotes.com/Note/JHN15954'
]

# %% Add to object
bmo = BmoScraper(bmo_urls)
bmo.transpose_set_header()
bmo.label_note_tables()
bmo.set_pdw_index()
bmo._PDW_Name()
bmo._callBarrierLevelFinal()
bmo._callObservationDateList()
bmo._callObservationFrequency()
bmo._callType()
bmo._numberNoCallPeriods()

# %% Final results
bmo.pdw_df
