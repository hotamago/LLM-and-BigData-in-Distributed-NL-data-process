import streamlit as st
import yaml
from config import cfg, save_config
from streamlit_ace import st_ace

def render():
    st.header("Settings")
    
    with st.expander("About Settings", expanded=True):
        st.info("""
        This page allows you to configure various settings for the application.
        Use the simple form below to update settings as needed.
        """)
    
    # Create tabs for different configuration sections
    tab1, tab2, tab3 = st.tabs(["Search Settings", "System Settings", "Advanced"])
    
    with tab1:
        st.subheader("Search Configuration")
        
        # Serper API key in a password field
        serper_api_key = st.text_input(
            "Search API Key", 
            value=cfg.get('serper', {}).get('api_key', ''),
            type="password",
            help="Enter your Serper API key for web searches"
        )
        
        # Other search settings
        search_country = st.selectbox(
            "Default Search Country",
            options=["us", "uk", "ca", "au", "in", "de", "fr", "es", "it", "jp"],
            index=0,
            help="Select your preferred search region"
        )
        
        search_locale = st.selectbox(
            "Default Search Language",
            options=["en", "fr", "de", "es", "it", "ja"],
            index=0,
            help="Select your preferred search language"
        )
    
    with tab2:
        st.subheader("System Configuration")
        
        # Spark settings
        spark_master = st.text_input(
            "Spark Master URL",
            value=cfg.get('spark', {}).get('master', 'local[*]'),
            help="The Spark master connection URL"
        )
        
        spark_app_name = st.text_input(
            "Application Name",
            value=cfg.get('spark', {}).get('app_name', 'WebContentCollector'),
            help="The name of this application in Spark"
        )
        
        # HDFS settings
        hdfs_url_contents = st.text_input(
            "Content Storage Location",
            value=cfg.get('hdfs', {}).get('url_contents', ''),
            help="Where to store collected content"
        )
    
    with tab3:
        st.subheader("Advanced Configuration")
        st.warning("Only modify these settings if you understand their purpose.")
        
        # Expert mode - use YAML editor
        use_expert_mode = st.checkbox("Use Expert Mode (YAML Editor)")
        
        if use_expert_mode:
            config_yaml = yaml.dump(cfg, sort_keys=False)
            edited_config = st_ace(
                value=config_yaml,
                language='yaml',
                theme='monokai',
                keybinding='vscode',
                height=400
            )
            
            try:
                new_cfg = yaml.load(edited_config, Loader=yaml.FullLoader)
            except yaml.YAMLError as e:
                st.error(f"Error in YAML syntax: {e}")
                new_cfg = None
        else:
            # Create a new config from the form values
            new_cfg = {
                'serper': {
                    'api_key': serper_api_key
                },
                'spark': {
                    'master': spark_master,
                    'app_name': spark_app_name
                },
                'hdfs': {
                    'url_contents': hdfs_url_contents
                }
            }
    
    # Save button
    if st.button("Save Settings"):
        if new_cfg:
            try:
                save_config(new_cfg)
                st.success("Settings saved successfully!")
            except Exception as e:
                st.error(f"Error saving settings: {str(e)}")
        else:
            st.error("Unable to save settings. Please fix any errors first.")