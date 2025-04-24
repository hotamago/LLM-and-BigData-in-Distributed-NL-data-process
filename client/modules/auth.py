from models.engine import SessionLocal
from models.account import Account, RoleEnum

current_user = None

def load_users():
    """Load and return list of user dicts from database."""
    with SessionLocal() as db:
        accounts = db.query(Account).all()
        return [
            {'username': a.username, 'is_admin': a.role == RoleEnum.admin}
            for a in accounts
        ]


def authenticate(username, password):
    """Return user dict if credentials match, else None."""
    with SessionLocal() as db:
        account = db.query(Account).filter(Account.username == username).first()
        if account and account.check_password(password):
            return {'username': account.username, 'is_admin': account.role == RoleEnum.admin}
        return None


def create_user(username, password, is_admin=False):
    """Add a new user to the database."""
    with SessionLocal() as db:
        if db.query(Account).filter(Account.username == username).first():
            raise ValueError('User already exists')
        account = Account(username=username)
        account.set_password(password)
        account.role = RoleEnum.admin if is_admin else RoleEnum.user
        db.add(account)
        db.commit()


def update_user(username, password=None, is_admin=None):
    """Update existing user credentials or admin flag in database."""
    with SessionLocal() as db:
        account = db.query(Account).filter(Account.username == username).first()
        if not account:
            raise ValueError('User not found')
        if password:
            account.set_password(password)
        if is_admin is not None:
            account.role = RoleEnum.admin if is_admin else RoleEnum.user
        db.commit()


def delete_user(username):
    """Remove a user from the database."""
    with SessionLocal() as db:
        account = db.query(Account).filter(Account.username == username).first()
        if account:
            db.delete(account)
            db.commit()


def set_current_user(user):
    """Set the current authenticated user (dict)."""
    global current_user
    current_user = user


def get_current_user():
    """Get the current authenticated user dict."""
    return current_user
