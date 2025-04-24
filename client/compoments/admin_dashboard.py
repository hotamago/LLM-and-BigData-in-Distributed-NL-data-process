import streamlit as st
from streamlit import session_state as ss
import modules.auth as auth


def render():
    """Render the Admin Dashboard UI."""
    st.title("Admin Dashboard")

    # Load and display existing users
    users = auth.load_users()
    st.subheader("Existing Users")
    st.table(users)

    # Create new user
    st.subheader("Create New User")
    new_username = st.text_input("Username", key="new_username")
    new_password = st.text_input("Password", type="password", key="new_password")
    new_is_admin = st.checkbox("Is Admin", key="new_is_admin")
    if st.button("Create User"):
        try:
            auth.create_user(new_username, new_password, new_is_admin)
            st.success("User created successfully.")
            st.rerun()
        except Exception as e:
            st.error(f"Error creating user: {e}")

    # Edit existing user
    if users:
        st.subheader("Edit User")
        edit_user = st.selectbox("Select User to Edit", [u['username'] for u in users], key="edit_user")
        edit_password = st.text_input("New Password", type="password", key="edit_password")
        edit_is_admin = st.checkbox("Is Admin", value=next(u for u in users if u['username'] == edit_user)['is_admin'], key="edit_is_admin")
        if st.button("Update User"):
            try:
                auth.update_user(edit_user, password=edit_password or None, is_admin=edit_is_admin)
                st.success("User updated successfully.")
                st.rerun()
            except Exception as e:
                st.error(f"Error updating user: {e}")

        # Delete user
        st.subheader("Delete User")
        del_user = st.selectbox("Select User to Delete", [u['username'] for u in users], key="del_user")
        if st.button("Delete User"):
            if del_user == ss.user['username']:
                st.error("Cannot delete the currently logged-in user.")
            else:
                try:
                    auth.delete_user(del_user)
                    st.success("User deleted successfully.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error deleting user: {e}")