"""Staff configuration for KadeejaClinic bot - pulled from database.

This module now serves as a compatibility layer. Staff configuration is stored
in the Supabase 'staff_config' table and accessed via DatabaseManager methods.

Database schema for staff_config:
- telegram_id: BIGINT PRIMARY KEY
- staff_name: TEXT NOT NULL
- role: TEXT DEFAULT 'staff' CHECK (role IN ('admin', 'staff'))
- is_active: BOOLEAN DEFAULT TRUE

To add staff members, insert records directly into the staff_config table:
INSERT INTO staff_config (telegram_id, staff_name, role, is_active)
VALUES (123456789, 'Dr. Fareeda', 'admin', TRUE);
INSERT INTO staff_config (telegram_id, staff_name, role, is_active)
VALUES (987654321, 'Nimisha', 'staff', TRUE);
"""

from typing import Optional, Dict, Any

# Import DatabaseManager to fetch staff from database
# This is imported here to avoid circular imports
_database_manager = None


def _get_db_manager():
    """Lazy import of DatabaseManager to avoid circular imports."""
    global _database_manager
    if _database_manager is None:
        from database import DatabaseManager
        _database_manager = DatabaseManager
    return _database_manager


def get_user_role(telegram_id: int) -> str:
    """Get user role based on Telegram ID from database."""
    db = _get_db_manager()
    return db.get_staff_role(telegram_id)


def get_user_name(telegram_id: int) -> str:
    """Get user name based on Telegram ID from database."""
    db = _get_db_manager()
    return db.get_staff_name(telegram_id)


def is_admin(telegram_id: int) -> bool:
    """Check if user is an admin."""
    db = _get_db_manager()
    return db.is_admin(telegram_id)


def is_staff(telegram_id: int) -> bool:
    """Check if user is a staff member."""
    db = _get_db_manager()
    return db.is_staff(telegram_id)


def is_authorized(telegram_id: int) -> bool:
    """Check if user is authorized to use the bot."""
    return is_staff(telegram_id)


# Legacy compatibility - kept for any external references
# These are no longer used internally but provided for backward compatibility
STAFF_CONFIG = {
    "admin_chat_ids": {},
    "staff_chat_ids": {},
    "staff_roles": {},
}


def refresh_staff_config() -> None:
    """
    Refresh the staff_config dictionary from database.
    This updates the legacy STAFF_CONFIG dict for backward compatibility.
    """
    db = _get_db_manager()
    staff_list = db.get_all_active_staff()

    STAFF_CONFIG["admin_chat_ids"] = {}
    STAFF_CONFIG["staff_chat_ids"] = {}
    STAFF_CONFIG["staff_roles"] = {}

    for staff in staff_list:
        telegram_id = str(staff.get('telegram_id', ''))
        staff_name = staff.get('staff_name', '')
        role = staff.get('role', 'staff')

        if role == 'admin':
            STAFF_CONFIG["admin_chat_ids"][telegram_id] = staff_name
        else:
            STAFF_CONFIG["staff_chat_ids"][telegram_id] = staff_name

        STAFF_CONFIG["staff_roles"][telegram_id] = role