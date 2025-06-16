import streamlit as st
from streamlit import session_state as ss
import datetime
import time

# Hota
from modules.cache import hcache
from modules.helper import *
import os
from modules.auth import authenticate, set_current_user, get_current_user, authenticate_with_token, refresh_access_token
from modules.cache import Cache
from models.engine import Base, engine

# Create database tables (Account, etc.) if they do not exist
Base.metadata.create_all(bind=engine)

# Page setup
st.set_page_config(page_title='Project 3', page_icon=':mag:', layout='wide')

# Initialize auth and cache in session
if "user" not in ss:
    ss.user = None
if "hcache" not in ss:
    ss.hcache = None
if "token_expiry" not in ss:
    ss.token_expiry = None
if "last_token_refresh" not in ss:
    ss.last_token_refresh = time.time()

# First-run: if no users exist, prompt to create initial admin account
import modules.auth as auth
users = auth.load_users()
if not users:
    st.title("Setup Admin Account")
    setup_username = st.text_input("Admin Username", key="setup_username")
    setup_password = st.text_input("Password", type="password", key="setup_password")
    setup_confirm = st.text_input("Confirm Password", type="password", key="setup_confirm")
    if st.button("Create Admin Account", key="create_admin"):
        if not setup_username or not setup_password:
            st.error("Please enter username and password.")
        elif setup_password != setup_confirm:
            st.error("Passwords do not match.")
        else:
            try:
                auth.create_user(setup_username, setup_password, is_admin=True)
                st.success("Admin account created. Please login.")
                st.rerun()
            except Exception as e:
                st.error(f"Error creating admin account: {e}")
    st.stop()

# Check if token refresh is needed (every 30 minutes)
current_time = time.time()
if (ss.user is not None and 
    "refresh_token" in st.session_state and 
    (current_time - ss.last_token_refresh) > 1800):  # 30 minutes
    
    # Try to refresh the token
    new_access_token = refresh_access_token(st.session_state.refresh_token)
    if new_access_token:
        st.session_state.access_token = new_access_token
        ss.last_token_refresh = current_time
    
# Check for token in session and try to authenticate
if ss.user is None and "access_token" in st.session_state:
    user = authenticate_with_token(st.session_state.access_token)
    if user:
        ss.user = user
        set_current_user(user)
        if "hcache" not in ss or ss.hcache is None:
            ss.hcache = Cache(f".cache_{user['username']}")
            # Monkey-patch global cache for modules
            import modules.cache as cache_module
            cache_module.hcache = ss.hcache
            # Update HDFS paths per user
            from config import cfg
            for key, path in cfg['hdfs_subpath'].items():
                if path.startswith(cfg['hdfs_base_path']):
                    suffix = path[len(cfg['hdfs_base_path']):]
                else:
                    suffix = path
                cfg['hdfs'][key] = f"{cfg['hdfs_base_path']}/users/{user['id']}{suffix}"

# Logout option
if ss.user:
    if st.sidebar.button("Logout"):
        set_current_user(None)
        ss.user = None
        ss.hcache = None
        # Clear tokens
        if "access_token" in st.session_state:
            del st.session_state.access_token
        if "refresh_token" in st.session_state:
            del st.session_state.refresh_token
        if "token_expiry" in st.session_state:
            del st.session_state.token_expiry
        if "last_token_refresh" in st.session_state:
            del st.session_state.last_token_refresh
        st.rerun()

# Login page
if ss.user is None:
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    remember_me = st.checkbox("Remember me", value=True)
    if st.button("Login"):
        user_data = authenticate(username, password)
        if user_data:
            # Store tokens in session state
            if "access_token" in user_data:
                st.session_state.access_token = user_data["access_token"]
                ss.last_token_refresh = time.time()
            if "refresh_token" in user_data and remember_me:
                st.session_state.refresh_token = user_data["refresh_token"]
            
            # Remove tokens from user object before setting
            user = {k: v for k, v in user_data.items() if k not in ["access_token", "refresh_token"]}
            set_current_user(user)
            ss.user = user
            ss.hcache = Cache(f".cache_{user['username']}")
            # Monkey-patch global cache for modules
            import modules.cache as cache_module
            cache_module.hcache = ss.hcache
            # Update HDFS paths per user
            from config import cfg
            for key, path in cfg['hdfs_subpath'].items():
                if path.startswith(cfg['hdfs_base_path']):
                    suffix = path[len(cfg['hdfs_base_path']):]
                else:
                    suffix = path
                cfg['hdfs'][key] = f"{cfg['hdfs_base_path']}/users/{user['id']}{suffix}"
            st.rerun()
        else:
            st.error("Invalid username or password")
    st.stop()

# Ensure cache instance is available in modules
import modules.cache as cache_module
cache_module.hcache = ss.hcache

st.title('Big Data Collection and Processing using LLM')

# Init session_state
if "stage_process" not in ss:
    ss.stage_process = 0
if "list_query_engine_search" not in ss:
    ss.list_query_engine_search = []
if "list_query_engine_search_gen" not in ss:
    ss.list_query_engine_search_gen = []

# Dynamic navigation menu with admin option
menu_options = ["Home", "HDFS", "Spark", "Config"]
if ss.user.get("is_admin"):
    menu_options.append("Admin Dashboard")
menu = st.sidebar.radio("Navigation", menu_options)

# Stage selector in sidebar (only show on Home page)
if menu == "Home":
    st.sidebar.markdown("---")
    list_stage_process = [
        'Stage 1: Generate queries',
        'Stage 2: Get url',
        'Stage 3: Get content',
        'Stage 4: Gen columns info',
        'Stage 5: Process data',
        'Stage 6: Final process',
    ]
    stage_process = st.sidebar.selectbox(
        label='Select Processing Stage',
        options=list_stage_process, 
        index=ss["stage_process"],
        help="Choose which stage of the data processing pipeline to work on"
    )
    # Update session state with current stage index
    ss["stage_process"] = list_stage_process.index(stage_process)

if menu == "Home":
    # Global variables
    if hcache.exists('list_query_engine_search'):
        ss.list_query_engine_search = hcache.get('list_query_engine_search', default=[])

    # Stage process
    with st.container():
        # Get stage list (defined in sidebar section above)
        list_stage_process = [
            'Stage 1: Generate queries',
            'Stage 2: Get url',
            'Stage 3: Get content',
            'Stage 4: Gen columns info',
            'Stage 5: Process data',
            'Stage 6: Final process',
        ]

        # Stage 1
        if stage_process == list_stage_process[0]: # Stage 1: Generate queries for search engine api using one LLM
            from compoments.stage1 import render
            render()

        elif stage_process == list_stage_process[1]: # Stage 2: Using search engine api to search url web
            from compoments.stage2 import render
            render()
            
        elif stage_process == list_stage_process[2]: # Stage 3: From url send request to get content by multi spark node then each spark node send to hadoop to save
            from compoments.stage3 import render
            render()
            
        elif stage_process == list_stage_process[3]:  # Stage 4 Gen columns info for multi node for unified data processing
            from compoments.stage4 import render
            render()

        elif stage_process == list_stage_process[4]:  # Stage 5 Process data to get final data for analysis using spark and hadoop with multi node
            from compoments.stage5 import render
            render()

        elif stage_process == list_stage_process[5]:  # Stage 6: Final process with python code generate from LLM to process data and get result for analysis using spark and hadoop with multi node
            from compoments.stage6 import render
            render()

        else:
            st.write('Stage not exited')

elif menu == "HDFS":
    from compoments.hdfs_explorser import render
    render()

elif menu == "Spark":
    from compoments.spark_explorser import render
    render()

elif menu == "Config":
    from compoments.config import render
    render()

elif menu == "Admin Dashboard":
    from compoments.admin_dashboard import render as render_admin
    render_admin()