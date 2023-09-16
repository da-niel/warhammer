import re, base64

import streamlit as st
import pandas as pd

from snowflake import snowpark

st.set_page_config(
    layout='wide'
)

# DICE_ROLL to PROBABILITY mapping
    # if you need a 2 or higher, that corresponds to a 5 (out of 6) chance
PROB_MAP = {
    1:6, 
    2:5,
    3:4,
    4:3,
    5:2,
    6:1
}

@st.cache_resource
def build_session(params):
    return snowpark.Session.builder.configs(params).create()

@st.cache_data
def get_data(query):
    return session.sql(query).to_pandas()

def get_url(stage, unit, page, df):
    path = df[df.RELATIVE_PATH.str.contains(f"{unit} \({page}\)")].RELATIVE_PATH.values[0]
    url_df = get_data(f"select BUILD_STAGE_FILE_URL( @{stage} , '{path}') as url")
    return url_df.iloc[0,0]

def base64_to_image(encoded_string):
    return base64.b64decode(encoded_string)

def calculateHits(series: pd.Series):

    hits = dict()
    for p, q in PROB_MAP.items():
        nHits = (
            series.A 
            * (PROB_MAP[series.BS] / 6)
            * (q / 6)
            * (min(PROB_MAP[save_roll_ass] / 6, 1))
        )
        hits[p] = nHits
    
    return hits

remove_int = lambda s: re.sub(r'\d', '', s)

def process_keyword(kw):
    kw = remove_int(kw).strip()
    if "anti" in kw.lower():
        kw = "Anti"
    elif kw.lower().endswith(("x", "+")):
        kw = " ".join(kw.split(" ")[:-1])
    
    return kw

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

st.write("# Warhammer")

if not st.session_state["authenticated"]:
    welcome_screen = st.empty()

    with welcome_screen:
        c, _ = st.columns((1,3))
        with c:
            with st.form("Credentials"):
                username = st.text_input("Username")
                password = st.text_input("Password", type='password')

                login = st.form_submit_button("Login")

    if login:
        params = {
            "account": st.secrets.snowflake_warhammer["ACCOUNT"],
            "user": username, # ask for input
            "password": password, # ask for input
            "database": st.secrets.snowflake_warhammer["DATABASE"],
            "schema": st.secrets.snowflake_warhammer["SCHEMA"] # ask for input
        }  

        # Session and data
        session = build_session(params)
        welcome_screen.empty()
        st.session_state['authenticated'] = True


if not st.session_state.authenticated:
    st.stop()

units = get_data("select * from dim_units")
weapons = get_data("select * from dim_weapons")
keywords = get_data("select * from dim_keywords")
images = get_data("select * from dim_images")

units.set_index("NAME", inplace=True)
weapons.set_index("WEAPONS", inplace=True)
keywords.set_index("ABILITY", inplace=True)
images.set_index("NAME", inplace=True)

left, _, main, right = st.columns([0.3, 0.1, 0.7, 0.4])

# Sidebar
with st.sidebar:
    st.write ("### Options")
    faction = st.selectbox("Select Faction", options = units.RACE.unique())
    show_image = st.checkbox("Show datasheet as image", value = True)
    show_table = st.checkbox("Show datasheet as text", value = False)
    show_page_two = st.checkbox("Show second page", value = False)
    show_damage_calc = st.checkbox("Show Damage Calc", value = False)
    if show_damage_calc:
        save_roll_ass = st.number_input("Assumed Save Roll", min_value = 2, max_value = 6, value = 3, step=1)
    
    kwd = st.multiselect('Keyword Definions', options = keywords.index)

    if kwd:
        st.write(keywords.loc[kwd])

units = units.query(f"RACE == '{faction}'")

with left:
    f'# {faction.capitalize()}'

    selected_units = st.multiselect(label = 'Units', options = units.index.unique())

    skw_list = []
    for kws in weapons.loc[weapons.UNITS.isin(selected_units)].KEYWORDS:
        skw_list.extend(kws.split(","))

    skw_set = {process_keyword(kw) for kw in skw_list}

    # show keywords
    st.write("### Keywords")
    for kw in skw_set:
        if kw.lower() != 'none':
            try:
                st.write(f"**{kw}**")
                st.write(keywords.loc[kw].values[0])
            except:
                pass

# Show Stats
with main:
    # for spacing
    st.write("# ")
    st.write("# ")

    for selected_unit in selected_units:
        su_df = units[units.index == selected_unit] # selected_unit df
        sw_df = weapons[weapons.UNITS == selected_unit] # selected_weapon df
        with st.expander(selected_unit, expanded= True):
            if show_image:
                try:
                    page_1 = base64_to_image(images.loc[f"{selected_unit} (1)"].IMAGE_ENCODED)
                    st.image(page_1)
                    if show_page_two:
                        page_2 = base64_to_image(images.loc[f"{selected_unit} (2)"].IMAGE_ENCODED)
                        st.image(page_2)
                except Exception as e:
                    print(e)
            
            if show_table:
                st.write(su_df.select_dtypes(include='number'))
                for col in su_df.select_dtypes(exclude='number'):
                    st.markdown(f"**{col}**")
                    st.write(su_df[col].values[0])

            
            if show_damage_calc:
                unit_weapons = sw_df[sw_df.UNITS == selected_unit]
                for weapon in unit_weapons.index:
                    st.write(calculateHits(unit_weapons.loc[weapon]))