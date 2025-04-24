"""add account table

Revision ID: 9942869ca769
Revises: 
Create Date: 2025-04-24 16:17:06.511230

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '9942869ca769'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # create RoleEnum type
    roleenum = postgresql.ENUM('user', 'admin', name='roleenum')
    roleenum.create(op.get_bind(), checkfirst=True)
    # create account table
    op.create_table(
        'account',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('username', sa.String(length=255), nullable=False, unique=True),
        sa.Column('password_hash', sa.String(), nullable=False),
        sa.Column('role', postgresql.ENUM('user', 'admin', name='roleenum', create_type=False), nullable=False),
    )

def downgrade():
    # drop account table and enum type
    op.drop_table('account')
    postgresql.ENUM('user', 'admin', name='roleenum').drop(op.get_bind(), checkfirst=True)
