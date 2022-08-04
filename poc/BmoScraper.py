# %% Libs
import pandas as pd


# %% Read in the examples
class BmoScraper:
    # Pass in note URLs & lookup for PDW
    def __init__(self, bmo_urls):
        self.notes_dict = {
            note.rsplit('/', 1)[-1]: pd.read_html(note)
            for note in bmo_urls
        }
        self.bmo_example_fields = pd.read_excel('BMO Examples.xlsx')

    # Get all scraping results as a single row table
    def transpose_set_header(self):
        for key, val in self.notes_dict.items():
            for i in range(len(val)):
                self.notes_dict[key][i] = self.notes_dict[key][i].T
                self.notes_dict[key][i].columns = self.notes_dict[key][i].iloc[
                    0]
                self.notes_dict[key][i] = self.notes_dict[key][i][1:]

    # Convert all to dict to navigate indices
    def label_note_tables(self):
        self.notes_dict = {
            key: {val[i].columns.name: val[i]
                  for i in range(len(val))}
            for key, val in self.notes_dict.items()
        }


# %% Set of URLs
bmo_urls = {
    'https://www.bmonotes.com/Note/JHN7482',
    'https://www.bmonotes.com/Note/JHN15954'
}

# %% Add to object
bmo = BmoScraper(bmo_urls)
bmo.transpose_set_header()
bmo.label_note_tables()

# %% Get rules for each
pdw_df = {
    'JHN7482':
    bmo.bmo_example_fields.loc[~bmo.bmo_example_fields['JHN7482'].isna(),
                               ['PDW Fields', 'JHN7482']],
    'JHN15954':
    bmo.bmo_example_fields.loc[~bmo.bmo_example_fields['JHN15954'].isna(),
                               ['PDW Fields', 'JHN15954']],
}
