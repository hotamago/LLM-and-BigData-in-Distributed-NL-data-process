import streamlit as st
from streamlit import session_state as ss
import pandas as pd
from streamlit_ace import st_ace
import json

# Hota
# from modules.cache import ss.hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *


def render():
    st.header("Step 4: Define Data Processing Requirements")

    with st.expander("How this works", expanded=True):
        st.info("""
        1. This step helps you specify what information to extract from the collected content
        2. Enter your requirements in plain language (what data you want to analyze)
        3. Click 'Generate Extraction Fields' to create a plan
        4. Review and customize the fields that will be extracted
        5. Continue when you're satisfied with the extraction plan
        """)

    # Example requirements to help users understand what to enter
    with st.expander("Example requirements"):
        st.markdown("""
        **Example 1:** Extract the main topic, author name, publication date, and key points from each article.
        
        **Example 2:** Identify product names, prices, features, and customer ratings from product descriptions.
        
        **Example 3:** Extract location names, event dates, participating organizations, and contact information.
        """)

    # User input for data processing requirements with better guidance
    user_input_stage_4 = st.text_area(
        "What information would you like to extract from the content?",
        value=ss.hcache.get("user_input_stage_4", default=""),
        height=120,
        placeholder="Describe what information you want to extract from the collected content...",
        help="Be specific about what data points you want to extract from the collected web content",
    )

    # Cache user input
    ss.hcache.set("user_input_stage_4", user_input_stage_4)

    # Generate columns button with better label
    if st.button("Generate Extraction Fields", key="gen_columns_info"):
        with st.spinner("Analyzing your requirements..."):
            # Show progress
            progress = st.progress(0)
            status = st.empty()

            status.write("Identifying data fields to extract...")
            progress.progress(0.3)

            columns_info = gen_columns_info(
                json.dumps(
                    {
                        "requirements": user_input_stage_4,
                        "exited_columns": "url, title, snippet, content",
                    }
                )
            )

            progress.progress(0.8)
            status.write("Finalizing extraction fields...")

            ss.hcache.set("columns_info", columns_info)

            progress.progress(1.0)
            status.success("Extraction fields generated successfully!")

    # Display and edit generated columns in a more user-friendly way
    if ss.hcache.exists("columns_info"):
        st.subheader("Data Extraction Fields")

        columns_info = list(ss.hcache.get("columns_info", default=[]))

        # Display as a clean table instead of raw JSON
        columns_df = pd.DataFrame(columns_info)
        if not columns_df.empty:
            columns_df.columns = ["Field Name", "Description"]
            st.table(columns_df)

        # Editing interface with tabs
        tab1, tab2 = st.tabs(["Add New Field", "Edit Existing Fields"])

        with tab1:
            col1, col2 = st.columns(2)
            with col1:
                new_field_name = st.text_input(
                    "New Field Name",
                    key="new_field_name",
                    placeholder="e.g., publication_date",
                )
            with col2:
                new_field_desc = st.text_input(
                    "Field Description",
                    key="new_field_desc",
                    placeholder="e.g., Date when article was published",
                )

            if new_field_name.strip().startswith("sentiment"):
                st.warning(
                    "Sentiment analysis is not supported in this version. Please remove the field."
                )
                st.stop()

            if st.button("Add Field", key="add_field"):
                if new_field_name and new_field_desc:
                    columns_info.append(
                        {"name": new_field_name, "description": new_field_desc}
                    )
                    ss.hcache.set("columns_info", columns_info)
                    st.success(f"Added new field: {new_field_name}")
                    st.rerun()
                else:
                    st.warning("Please provide both a field name and description")

        with tab2:
            # Only show if there are fields to edit
            if columns_info:
                # Choose field to edit
                field_to_edit = st.selectbox(
                    "Select field to edit or remove",
                    options=range(len(columns_info)),
                    format_func=lambda i: columns_info[i]["name"],
                )

                # Edit selected field
                edited_name = st.text_input(
                    "Field Name",
                    value=columns_info[field_to_edit]["name"],
                    key=f"edit_name_{field_to_edit}",
                )

                edited_desc = st.text_input(
                    "Field Description",
                    value=columns_info[field_to_edit]["description"],
                    key=f"edit_desc_{field_to_edit}",
                )

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Update Field", key=f"update_field_{field_to_edit}"):
                        columns_info[field_to_edit] = {
                            "name": edited_name,
                            "description": edited_desc,
                        }
                        ss.hcache.set("columns_info", columns_info)
                        st.success("Field updated")
                        st.rerun()

                with col2:
                    if st.button("Remove Field", key=f"remove_field_{field_to_edit}"):
                        removed_name = columns_info[field_to_edit]["name"]
                        columns_info.pop(field_to_edit)
                        ss.hcache.set("columns_info", columns_info)
                        st.success(f"Removed field: {removed_name}")
                        st.rerun()
            else:
                st.info("No fields available to edit")

        # For advanced users who prefer JSON editing
        with st.expander("Advanced: Edit as JSON", expanded=False):
            st.warning("This option is for advanced users only.")
            edited_columns_info = st_ace(
                value=json.dumps(columns_info, indent=2),
                placeholder="Edit columns info JSON",
                height=200,
                language="json",
                key="edited_columns_info",
            )
            if st.button("Update from JSON", key="update_columns_info"):
                try:
                    updated_info = json.loads(edited_columns_info)
                    ss.hcache.set("columns_info", updated_info)
                    st.success("Fields updated from JSON")
                    st.rerun()
                except json.JSONDecodeError:
                    st.error("Invalid JSON format. Please check your input.")

        # Continue button
        st.markdown("---")
        st.write(
            "When you're satisfied with the extraction fields, continue to the next step:"
        )
        if st.button("Continue to Next Step", key="confirm_columns_info"):
            ss.stage_process = 4
            st.rerun()
