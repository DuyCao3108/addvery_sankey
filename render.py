from SankeyMakerClass import SankeyMaker
import os


# MANDATORY
INPUT_DATA_PATH = os.path.join(os.getcwd(), "transformed_data.csv")
STAGE_COLS_MAP = {
    "2024_06": "2024_06",
    "2024_07": "2024_07",
    "2024_08": "2024_08",
    "2024_09": "2024_09",
    "2024_10": "2024_10",
    '2024_11': '2024_11'
}
VALUE_COLUMN = "CNT_SKP_CLIENT"
AGG_FOR_VALUE_COLUMN = "sum" 


# OPTIONAL
CUSTOM_SETTINGS = {
    "node_order": {
        "1-Active Customer":0,
        "2-Onboarding":1,
        "3-Retention":2,
        "4-Winback":3,
        "Unidentified":4
    },
    "color_theme_name": "THEME_1", # watch out CASE
    # "color_overwrite": {
    #     "NC": "#3C3D37"
    # },
    # "node_tohide_sr_tar": "NC"
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