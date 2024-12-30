import streamlit as st
import yaml
from config import cfg, save_config
from streamlit_ace import st_ace

def render():
    st.header("Edit Configuration")
    
    with st.form("config_form"):
        config_yaml = yaml.dump(cfg, sort_keys=False)
        edited_config = st_ace(
            value=config_yaml,
            language='yaml',
            theme='monokai',
            keybinding='vscode',
            height=400
        )
        
        submit = st.form_submit_button("Save Configuration")
        
        if submit:
            try:
                new_cfg = yaml.load(edited_config, Loader=yaml.FullLoader)
                save_config(new_cfg)
                st.success("Configuration saved successfully.")
            except yaml.YAMLError as e:
                st.error(f"Error parsing YAML: {e}")