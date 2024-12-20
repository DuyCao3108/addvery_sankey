from SankeyMakerClass import SankeyMaker
import os


# MANDATORY
INPUT_DATA_PATH = os.path.join(os.getcwd(), "transformed_data.csv")
STAGE_COLS_MAP = {
    "CLIENT_ADDVERY_SEGMENT_JUN": "JUN",
    "CLIENT_ADDVERY_SEGMENT_JUL": "JUL",
    "CLIENT_ADDVERY_SEGMENT_AUG": "AUG",
    "CLIENT_ADDVERY_SEGMENT_SEP": "SEP",
    "CLIENT_ADDVERY_SEGMENT_OCT": "OCT",
    "CLIENT_ADDVERY_SEGMENT_NOV": "NOV"
}
VALUE_COLUMN = "CNT"
AGG_FOR_VALUE_COLUMN = "sum" 


# OPTIONAL
CUSTOM_SETTINGS = {
    "node_order": {
        "1-Active":0,
        "2-Onboarding":1,
        "3-Retention":2,
        "4-Winback":3,
    },
    "hide_stage_label": 5,
    "unit_divide": 1_000,
    "color_theme_name": "THEME_1", # watch out CASE
    # "color_overwrite": {
    #     "NC": "#3C3D37"
    # },
}

# CUSTOM_SETTINGS = None


if __name__ == "__main__":
    sankey = SankeyMaker()
    
    # prepare input
    sankey.prepare_sankey(
        input_data_path=INPUT_DATA_PATH,
        stage_cols_map=STAGE_COLS_MAP,
        val_col=VALUE_COLUMN,
        val_agg=AGG_FOR_VALUE_COLUMN,
        custom_settings=CUSTOM_SETTINGS
    )
    
    sankey.make_sankey()