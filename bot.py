"""KadeejaClinic Telegram Bot - Professional Clinic Management System.
Optimized for 512 MB RAM deployment with lazy imports and session cleanup.
"""
import os
import io
import gc
import threading
import socketserver
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, time, timezone, timedelta
from typing import Optional, List, Dict, Any
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# Import FastAPI for web server (for Render deployment)
try:
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from database import DatabaseManager, supabase, PK_MAPPING
from staff_config import get_user_role, get_user_name, is_admin, is_staff, is_authorized

# Lazy import for visualizer - only load when generating graphs
def _load_visualizers():
    """Lazy import visualizer modules to save memory at startup."""
    from utils.visualizer import (
        generate_edc_annual_graph,
        generate_edc_horizontal_graph,
        generate_comparative_attrition_plot,
        generate_new_pregnancy_inflow_graph,
        generate_delivery_trend_graph,
        generate_attrition_trend_graph,
        generate_visit_trend_graph
    )
    return {
        'edc_annual': generate_edc_annual_graph,
        'edc_horizontal': generate_edc_horizontal_graph,
        'attrition': generate_comparative_attrition_plot,
        'pregnancy_inflow': generate_new_pregnancy_inflow_graph,
        'delivery_trend': generate_delivery_trend_graph,
        'attrition_trend': generate_attrition_trend_graph,
        'visit_trend': generate_visit_trend_graph
    }

# Session timeout handler
async def cleanup_session_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle conversation timeout by clearing session data and notifying user.
    This helps free memory when conversations are abandoned.
    """
    if update.effective_chat:
        chat_id = update.effective_chat.id
        if hasattr(context, 'user_data') and context.user_data:
            # Clear the conversation data
            context.user_data.clear()
            # Force garbage collection
            gc.collect()
            await update.message.reply_text(
                "⏱️ Your session has timed out due to inactivity.\n"
                "Please start over by selecting an option from the menu."
            )
    return ConversationHandler.END

# ========================================
# CONFIGURATION
# ========================================
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found in environment variables")

# ========================================
# CONVERSATION STATES FOR /visit
# ========================================
VISIT_DATE = 1
NAME = 2
PHONE = 3
PATIENT_ID = 4
IS_PREGNANCY = 5
GRAVIDA = 6
EDC_CHECK = 7
EDC_INPUT = 8
EDC_CONFIRM = 14
NOTES = 9
NEXT_VISIT = 15

# Conversation states for Admin status sync
ADMIN_STATUS_SELECT = 20
ADMIN_STATUS_CONFIRM = 21

# Conversation states for Staff task completion
STAFF_TASK_COMPLETION = 30
STAFF_BOOKING_VERIFY = 31
STAFF_BOOKING_CONFIRM = 32

# Conversation states for Manual Booking
BOOKING_NAME = 33
BOOKING_PHONE = 34
BOOKING_DATE = 35
BOOKING_CONFIRM_CHANGE = 36
BOOKING_SUNDAY_CONFIRM = 37

# Conversation states for Sunday handling
NEXT_VISIT_SUNDAY_CONFIRM = 38

# Conversation states for Search
SEARCH_NAME = 60
SEARCH_PHONE = 61

# Conversation states for EDC Annual View (EDC_VIEW_CONV)
EDC_VIEW_SELECT_YEAR = 50
EDC_VIEW_GENERATE = 51

# Conversation states for Comparative Attrition (ATTRITION_CONV)
ATTRITION_VIEW_GRAPH = 60

# Conversation states for Task Cleanup (CLEANUP_CONV)
CLEANUP_SELECT_YEAR = 70
CLEANUP_SELECT_MONTH = 71
CLEANUP_CONFIRM_DELETE = 72

# Conversation states for Direct Task Delegation (ASSIGN_TASK_CONV)
ASSIGN_CHOOSE_STAFF = 80
ASSIGN_INPUT_MESSAGE = 81
ASSIGN_INPUT_DUE_DATE = 82
ASSIGN_CONFIRM_SEND = 83

# Conversation states for Trends Analytics (TRENDS_CONV)
TRENDS_SELECT_YEAR = 90
ATTRITION_TREND_SELECT_YEAR = 91
VISIT_TREND_SELECT_YEAR = 92

# Conversation states for View Bookings (VIEW_BOOKINGS_CONV)
VIEW_BOOKINGS_DATE_SELECT = 95
VIEW_BOOKINGS_CUSTOM_DATE = 96

# ========================================
# HELPER FUNCTIONS
# ========================================
def format_date(date_str: str) -> str:
    """Format date string for display."""
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%d %B %Y')
    except:
        return date_str

def validate_date(date_str: str) -> bool:
    """Validate date format YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except:
        return False

def validate_date_not_future(date_str: str) -> bool:
    """Validate that date is not greater than today."""
    try:
        date_dt = datetime.strptime(date_str, '%Y-%m-%d')
        today = datetime.now()
        return date_dt <= today
    except:
        return False

def validate_phone(phone: str) -> bool:
    """Validate phone number - must be numeric and 10 digits."""
    return phone.isdigit() and len(phone) == 10

def is_edc_valid(edc: str, visit_date: str) -> bool:
    """Check if EDC is after visit date."""
    try:
        edc_dt = datetime.strptime(edc, '%Y-%m-%d')
        visit_dt = datetime.strptime(visit_date, '%Y-%m-%d')
        return edc_dt > visit_dt
    except:
        return False

# ========================================
# KEYBOARDS
# ========================================
def get_yes_no_keyboard() -> ReplyKeyboardMarkup:
    """Get Yes/No keyboard."""
    return ReplyKeyboardMarkup(
        [['Yes', 'No'], ['Cancel']],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def get_edc_check_keyboard(old_edc: str) -> InlineKeyboardMarkup:
    """Get EDC confirmation keyboard with Keep/Edit/Cancel buttons."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Keep ({format_date(old_edc)})", callback_data='edc_keep')],
        [InlineKeyboardButton("Edit", callback_data='edc_edit')],
        [InlineKeyboardButton("Cancel", callback_data='cancel')]
    ])

def get_today_keyboard() -> ReplyKeyboardMarkup:
    """Get Today button keyboard with actual dates."""
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    return ReplyKeyboardMarkup(
        [[today], [yesterday], ['Cancel']],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def get_admin_menu_keyboard() -> ReplyKeyboardMarkup:
    """Get Admin menu keyboard."""
    return ReplyKeyboardMarkup(
        [
            ['📝 Add Visit', '📅 Add Booking'],
            ['🔍 Search Patient', '📅 View Bookings'],
            ['🤰 Pregnancy Registry', '📣 Assign Task'],
            ['📊 Trends'],
        ],
        resize_keyboard=True
    )

def get_staff_menu_keyboard() -> ReplyKeyboardMarkup:
    """Get Staff menu keyboard."""
    return ReplyKeyboardMarkup(
        [
            ['📝 Add Visit', '📅 Add Booking'],
            ['🔍 Search Patient', '📅 View Bookings'],
            ['📞 Planned Patient Calls', '🧹 Unplanned Tasks'],
        ],
        resize_keyboard=True
    )

def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    """Get keyboard with Cancel button."""
    return ReplyKeyboardMarkup(
        [['Cancel']],
        resize_keyboard=True,
        one_time_keyboard=False  # Keep keyboard visible
    )


def get_booking_date_picker_keyboard() -> InlineKeyboardMarkup:
    """Get inline keyboard for date selection in View Bookings (Admin only)."""
    # Calculate dates
    ist_offset = timedelta(hours=5, minutes=30)
    ist_timezone = timezone(ist_offset)
    today_utc = datetime.now(timezone.utc)
    today_ist = today_utc.astimezone(ist_timezone)

    yesterday = today_ist - timedelta(days=1)
    tomorrow = today_ist + timedelta(days=1)

    # Format dates
    today_str = today_ist.strftime('%Y-%m-%d')
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    tomorrow_str = tomorrow.strftime('%Y-%m-%d')

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Today", callback_data=f'view_date_{today_str}')],
        [InlineKeyboardButton("⬅️ Yesterday", callback_data=f'view_date_{yesterday_str}')],
        [InlineKeyboardButton("➡️ Tomorrow", callback_data=f'view_date_{tomorrow_str}')],
        [InlineKeyboardButton("✏️ Custom Date", callback_data='view_date_custom')]
    ])

# ========================================
# ACCESS CONTROL DECORATOR
# ========================================
def authorized_only(handler_func):
    """Decorator to check if user is authorized."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_chat:
            chat_id = update.effective_chat.id
            if not is_authorized(chat_id):
                await update.message.reply_text(
                    "❌ You are not authorized to use this bot.\n\n"
                    "Please contact the clinic administrator for access."
                )
                return
        return await handler_func(update, context, *args, **kwargs)
    return wrapper

# ========================================
# WELCOME & START COMMAND
# ========================================
@authorized_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command with role-based welcome message."""
    chat_id = update.effective_chat.id
    role = get_user_role(chat_id)
    user_name = get_user_name(chat_id)

    if role == 'admin':
        welcome_msg = (
            f"👋 Welcome {user_name}!\n\n"
            "🏥 *KadeejaClinic - Admin Dashboard*\n\n"
            "Managing clinic performance and data."
        )
        keyboard = get_admin_menu_keyboard()
    elif role == 'staff':
        welcome_msg = (
            f"👋 Welcome {user_name}!\n\n"
            "🏥 *KadeejaClinic - Staff Workbench*\n\n"
            "Log visits and complete your assigned tasks."
        )
        keyboard = get_staff_menu_keyboard()
    else:
        welcome_msg = (
            "🏥 *KadeejaClinic*\n\n"
            "Please contact the administrator to set up your access."
        )
        keyboard = None

    if keyboard:
        await update.message.reply_text(welcome_msg, reply_markup=keyboard, parse_mode='Markdown')
    else:
        await update.message.reply_text(welcome_msg, parse_mode='Markdown')

# ========================================
# /forcesync COMMAND HANDLER
# ========================================
@authorized_only
async def force_sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /forcesync command - manually triggers daily_sync_job."""
    chat_id = update.effective_chat.id
    user_name = get_user_name(chat_id)

    await update.message.reply_text("🔄 Starting daily sync automation...")

    # Run debug first to show current state
    debug_info = DatabaseManager.debug_sync_state()
    debug_msg = (
        f"📊 Sync Debug Information\n\n"
        f"Today's bookings (booked_by='Auto'): {len(debug_info.get('bookings_today_auto', []))}\n"
        f"Today's bookings (other): {len(debug_info.get('bookings_today_other', []))}\n"
        f"Missed bookings: {len(debug_info.get('bookings_missed', []))}\n"
        f"Pending No Visit tasks: {len(debug_info.get('pending_no_visit_tasks', []))}\n"
        f"Pending 0-Day Reminder tasks: {len(debug_info.get('pending_today_reminders', []))}\n\n"
        f"Running sync now..."
    )
    await update.message.reply_text(debug_msg)

    await daily_sync_job(context)

    await update.message.reply_text("✅ Daily sync completed!")

# ========================================
# /visit CONVERSATION HANDLER (10 STATES)
# ========================================
@authorized_only
async def visit_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the /visit conversation - Step 1: Get visit date."""
    print(f"[DEBUG] visit_start called - User: {update.effective_user.id if update.effective_user else 'unknown'}")
    context.user_data.clear()
    context.user_data['conversation'] = 'visit'
    keyboard = get_today_keyboard()
    await update.message.reply_text(
        "📅 *New Visit Registration*\n\n"
        "Please enter the visit date (YYYY-MM-DD format) or select from the options below:",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    print(f"[DEBUG] visit_start returning VISIT_DATE state")
    return VISIT_DATE

async def visit_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle visit date input - Step 2: Get patient name."""
    print(f"[DEBUG] visit_date_handler called - User: {update.effective_user.id if update.effective_user else 'unknown'}")
    try:
        text = update.message.text.strip()
        print(f"[DEBUG] Received text: '{text}'")

        if text == 'Cancel':
            print(f"[DEBUG] User cancelled")
            return await cancel_conversation(update, context)

        # Validate date input (keyboard shows actual dates)
        if validate_date(text):
            # Validate that date is not in the future
            if not validate_date_not_future(text):
                print(f"[DEBUG] Date is in the future: '{text}'")
                await update.message.reply_text(
                    "❌ Visit date cannot be in the future. Please use today's date or a past date:",
                    reply_markup=get_today_keyboard()
                )
                return VISIT_DATE
            context.user_data['visit_date'] = text
            print(f"[DEBUG] Valid date provided: {context.user_data['visit_date']}")
        else:
            print(f"[DEBUG] Invalid date format: '{text}'")
            await update.message.reply_text(
                "❌ Invalid date format. Please use YYYY-MM-DD format:",
                reply_markup=get_today_keyboard()
            )
            return VISIT_DATE

        visit_date_display = format_date(context.user_data['visit_date'])
        print(f"[DEBUG] Sending name prompt, returning NAME state")
        await update.message.reply_text(
            f"📅 Visit Date: {visit_date_display}\n\n"
            "👤 Please enter the patient's name:",
            reply_markup=get_cancel_keyboard()
        )
        return NAME
    except Exception as e:
        print(f"Error in visit_date_handler: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(
            "❌ An error occurred. Please try again or type /start to restart.",
            reply_markup=get_today_keyboard()
        )
        return VISIT_DATE

async def name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle patient name input - Step 3: Get phone number."""
    print(f"[DEBUG] name_handler called - User: {update.effective_user.id if update.effective_user else 'unknown'}")
    try:
        text = update.message.text.strip()
        print(f"[DEBUG] Received name: '{text}'")

        if text == 'Cancel':
            print(f"[DEBUG] User cancelled")
            return await cancel_conversation(update, context)

        if not text:
            print(f"[DEBUG] Empty name provided")
            await update.message.reply_text("❌ Name cannot be empty. Please enter the patient's name:",
                                          reply_markup=get_cancel_keyboard())
            return NAME

        context.user_data['name'] = text
        print(f"[DEBUG] Sending phone prompt, returning PHONE state")
        await update.message.reply_text(
            f"👤 Patient Name: {text}\n\n"
            "📱 Please enter the phone number:",
            reply_markup=get_cancel_keyboard()
        )
        return PHONE
    except Exception as e:
        print(f"Error in name_handler: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(
            "❌ An error occurred. Please try again or type /start to restart.",
            reply_markup=get_cancel_keyboard()
        )
        return NAME

async def phone_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle phone number input - Step 4: Get patient ID."""
    try:
        text = update.message.text.strip()

        if text == 'Cancel':
            return await cancel_conversation(update, context)

        if not text:
            await update.message.reply_text("❌ Phone number cannot be empty. Please enter the phone number:",
                                          reply_markup=get_cancel_keyboard())
            return PHONE

        # Validate phone number - must be numeric and 10 digits
        if not validate_phone(text):
            await update.message.reply_text(
                "❌ Invalid phone number. Please enter a 10-digit numeric phone number:",
                reply_markup=get_cancel_keyboard()
            )
            return PHONE

        context.user_data['phone'] = text

        # Check if patient exists to show existing ID
        existing_patient = DatabaseManager.fetch_patient(context.user_data['name'], text)
        if existing_patient and existing_patient.get('patient_id'):
            existing_id = existing_patient['patient_id']
            await update.message.reply_text(
                f"📱 Phone: {text}\n"
                f"🏥 Existing Patient ID: {existing_id}\n\n"
                "Press Enter to keep the same ID or enter a new Patient ID:",
                reply_markup=get_cancel_keyboard()
            )
        else:
            await update.message.reply_text(
                f"📱 Phone: {text}\n\n"
                "Please enter the Patient ID:",
                reply_markup=get_cancel_keyboard()
            )

        return PATIENT_ID
    except Exception as e:
        print(f"Error in phone_handler: {e}")
        await update.message.reply_text(
            "❌ An error occurred. Please try again or type /start to restart.",
            reply_markup=get_cancel_keyboard()
        )
        return PHONE

async def patient_id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle patient ID input - Step 5: Check if pregnancy."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    # If user pressed Enter (empty input), try to keep existing ID
    if not text:
        try:
            existing_patient = DatabaseManager.fetch_patient(context.user_data['name'], context.user_data['phone'])
            if existing_patient and existing_patient.get('patient_id'):
                context.user_data['patient_id'] = existing_patient['patient_id']
            else:
                await update.message.reply_text("❌ Patient ID cannot be empty. Please enter the Patient ID:",
                                              reply_markup=get_cancel_keyboard())
                return PATIENT_ID
        except Exception as e:
            print(f"Database error: {e}")
            await update.message.reply_text(
                "❌ Error fetching patient ID. Please try again or contact support.",
                reply_markup=get_cancel_keyboard()
            )
            return PATIENT_ID
    else:
        context.user_data['patient_id'] = text

    keyboard = get_yes_no_keyboard()
    await update.message.reply_text(
        f"🏥 Patient ID: {context.user_data['patient_id']}\n\n"
        "🤰 Is this a pregnancy-related visit?",
        reply_markup=keyboard
    )
    return IS_PREGNANCY

async def is_pregnancy_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle pregnancy check - Step 6: Ask for Gravida status."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if text.lower() == 'yes':
        context.user_data['is_pregnancy'] = True

        # Ask for Gravida status instead of hardcoding G1
        await update.message.reply_text(
            "🤰 Please enter the Gravida status (e.g., G1, G2, G3):",
            reply_markup=get_cancel_keyboard()
        )
        return GRAVIDA
    else:
        context.user_data['is_pregnancy'] = False
        context.user_data['edc'] = None
        context.user_data['gravida'] = None
        await update.message.reply_text(
            "🤰 Pregnancy: No\n\n"
            "📝 Please enter any clinical notes for this visit:",
            reply_markup=get_cancel_keyboard()
        )
        return NOTES


async def gravida_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle gravida status input - Step 7: Check pregnancy registry."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    # Validate and store gravida status
    if not text or not text.upper().startswith('G'):
        await update.message.reply_text(
            "❌ Invalid Gravida status. Please enter a valid value (e.g., G1, G2, G3):",
            reply_markup=get_cancel_keyboard()
        )
        return GRAVIDA

    context.user_data['gravida'] = text.upper()

    # Check pregnancy_registry for existing EDC with this specific gravida
    try:
        existing_pregnancy = DatabaseManager.fetch_pregnancy_registry(
            context.user_data['name'],
            context.user_data['phone'],
            context.user_data['gravida']
        )

        if existing_pregnancy:
            old_edc = existing_pregnancy['edc_date']
            context.user_data['existing_edc'] = old_edc

            # Show existing EDC with Keep/Edit options
            keyboard = get_edc_check_keyboard(old_edc)
            await update.message.reply_text(
                f"🤰 Pregnancy Registry Found for {text.upper()}!\n"
                f"Current EDC: {format_date(old_edc)}\n\n"
                "Do you want to keep this EDC or edit it?",
                reply_markup=keyboard
            )
            return EDC_CHECK
        else:
            # No existing EDC, ask for input
            await update.message.reply_text(
                f"🤰 No existing EDC found for {text.upper()} in registry.\n\n"
                "Please enter the EDC (Expected Date of Confinement) in YYYY-MM-DD format:",
                reply_markup=get_cancel_keyboard()
            )
            return EDC_INPUT

    except Exception as e:
        print(f"Database error: {e}")
        await update.message.reply_text(
            "❌ Error checking pregnancy registry. Please try again or contact support.",
            reply_markup=get_cancel_keyboard()
        )
        return GRAVIDA


async def edc_check_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle EDC confirmation button clicks."""
    query = update.callback_query
    await query.answer()

    if query.data == 'cancel':
        return await cancel_conversation(update, context)

    if query.data == 'edc_keep':
        # Keep existing EDC
        old_edc = context.user_data['existing_edc']
        context.user_data['edc'] = old_edc

        await query.edit_message_text(
            f"✅ EDC kept: {format_date(old_edc)}\n\n"
            "📝 Please enter any clinical notes for this visit:"
        )
        return NOTES
    elif query.data == 'edc_edit':
        # Edit existing EDC - ask for new input
        await query.edit_message_text(
            "Please enter the new EDC (Expected Date of Confinement) in YYYY-MM-DD format:"
        )
        return EDC_INPUT

    return EDC_CHECK

async def edc_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle EDC date input."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if not validate_date(text):
        await update.message.reply_text(
            "❌ Invalid date format. Please use YYYY-MM-DD format:",
            reply_markup=get_cancel_keyboard()
        )
        return EDC_INPUT

    # Validate EDC is after visit date
    if not is_edc_valid(text, context.user_data['visit_date']):
        await update.message.reply_text(
            f"❌ EDC must be after visit date ({format_date(context.user_data['visit_date'])}). "
            "Please enter a valid EDC in YYYY-MM-DD format:",
            reply_markup=get_cancel_keyboard()
        )
        return EDC_INPUT

    context.user_data['edc'] = text

    # Show confirmation
    keyboard = ReplyKeyboardMarkup([['Yes', 'No'], ['Cancel']], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(
        f"🤰 EDC: {format_date(text)}\n\n"
        "Is this EDC correct?",
        reply_markup=keyboard
    )
    return EDC_CONFIRM

async def edc_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle EDC confirmation."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if text.lower() == 'yes':
        await update.message.reply_text(
            f"✅ EDC confirmed: {format_date(context.user_data['edc'])}\n\n"
            "📝 Please enter any clinical notes for this visit:",
            reply_markup=get_cancel_keyboard()
        )
        return NOTES
    else:
        await update.message.reply_text(
            "Please enter the EDC (Expected Date of Confinement) in YYYY-MM-DD format:",
            reply_markup=get_cancel_keyboard()
        )
        return EDC_INPUT

async def notes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle clinical notes - Step 9: Ask for next visit date."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    context.user_data['notes'] = text

    keyboard = ReplyKeyboardMarkup([['Today'], ['Cancel']], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(
        "📅 Please enter the next visit date (YYYY-MM-DD) or type 'none' if not scheduled:",
        reply_markup=keyboard
    )
    return NEXT_VISIT

async def next_visit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle next visit date and save everything."""
    try:
        text = update.message.text.strip()

        if text == 'Cancel':
            return await cancel_conversation(update, context)

        # Parse next visit date
        next_visit_date = None
        if text.lower() == 'none' or text.lower() == 'no':
            next_visit_date = None
        elif text == 'Today':
            next_visit_date = datetime.now().strftime('%Y-%m-%d')
        elif validate_date(text):
            next_visit_date = text
        else:
            keyboard = ReplyKeyboardMarkup([['Today'], ['Cancel']], resize_keyboard=True, one_time_keyboard=True)
            await update.message.reply_text(
                "❌ Invalid date format. Please use YYYY-MM-DD format or type 'none':",
                reply_markup=keyboard
            )
            return NEXT_VISIT

        # Check if the selected date is a Sunday (only if a date was provided)
        if next_visit_date:
            try:
                visit_dt = datetime.strptime(next_visit_date, '%Y-%m-%d')
                if visit_dt.weekday() == 6:  # Sunday (weekday 6)
                    # Calculate next Monday (add 1 day to Sunday)
                    monday_date = visit_dt + timedelta(days=1)
                    monday_str = monday_date.strftime('%Y-%m-%d')

                    # Store the original date in case user wants to choose different one
                    context.user_data['sunday_next_visit_date'] = next_visit_date

                    # Show Sunday warning with inline keyboard
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(f"Move to Monday ({format_date(monday_str)})", callback_data=f"visit_monday|{monday_str}"),
                            InlineKeyboardButton("Enter a different date", callback_data="visit_newdate")
                        ]
                    ])

                    await update.message.reply_text(
                        f"⚠️ The selected date {format_date(next_visit_date)} falls on a Sunday.",
                        reply_markup=keyboard
                    )
                    return NEXT_VISIT_SUNDAY_CONFIRM
            except ValueError:
                pass

        context.user_data['next_visit_date'] = next_visit_date

        # ========================================
        # DATABASE ACTIONS (with error handling)
        # ========================================
        name = context.user_data['name']
        phone = context.user_data['phone']
        visit_date = context.user_data['visit_date']
        patient_id = context.user_data['patient_id']
        is_pregnancy = context.user_data['is_pregnancy']
        edc = context.user_data.get('edc')
        gravida = context.user_data.get('gravida')
        notes = context.user_data['notes']

        # Get the current user's name for task assignment
        chat_id = update.effective_chat.id
        current_user_name = get_user_name(chat_id)

        # Check if patient exists - if not, we'll create them
        existing_patient = DatabaseManager.fetch_patient(name, phone)
        is_new_patient = not existing_patient

        # 1. Patients: Create/update patient record with notes, patient_id, and last_visit_date
        # upsert_patient will create a new patient if they don't exist
        DatabaseManager.prepend_patient_notes(name, phone, notes, visit_date)
        DatabaseManager.upsert_patient(name, phone, patient_id=patient_id, last_visit=visit_date)

        # 2. Visits: Insert clinical record with gravida_status
        DatabaseManager.insert_visit(
            name, phone, visit_date, is_pregnancy,
            remarks=notes,
            next_visit_date=next_visit_date,
            gravida_status=gravida
        )

        # 2.5. Bookings: Mark current booking as visited
        DatabaseManager.mark_booking_visited(name, phone)

        # 3. Pregnancy Registry: UPSERT EDC date with gravida_status
        edc_display = "N/A"
        gravida_display = "N/A"
        if is_pregnancy and edc:
            DatabaseManager.upsert_pregnancy_registry(
                name, phone, edc, gravida_status=gravida, status='Active'
            )
            edc_display = format_date(edc)
            gravida_display = gravida

        # 4. Bookings: Update bookings table if next visit is provided
        if next_visit_date:
            DatabaseManager.upsert_booking(name, phone, next_visit_date)

        # 5. Tasks: Create '3-Day Feedback' task for the current user (dynamic assignee)
        # SUNDAY SHIFT: If VISIT_DATE + 3 is Sunday, set Monday
        three_day_due = DatabaseManager.calculate_due_date(visit_date, 3)
        DatabaseManager.create_patient_task(
            assignee=current_user_name,
            name=name,
            phone=phone,
            followup_type='3-Day Feedback',
            status='Pending',
            due_date=three_day_due
        )

        # Success Message
        success_message = (
            f"✅ *Visit Recorded Successfully!*\n\n"
            f"👤 Patient: {name}\n"
            f"📱 Phone: {phone}\n"
            f"🏥 Patient ID: {patient_id}\n"
            f"📅 Visit Date: {format_date(visit_date)}\n"
            f"🤰 Pregnancy: {'Yes' if is_pregnancy else 'No'}\n"
        )
        if is_pregnancy and edc:
            success_message += f"👶 Gravida: {gravida_display}\n"
            success_message += f"📅 EDC: {edc_display}\n"
        success_message += f"📝 Notes: {notes}\n"
        if next_visit_date:
            success_message += f"📅 Next Visit: {format_date(next_visit_date)}\n"
        if is_new_patient:
            success_message += f"\n🆕 *New patient added to the patient table*\n"
        success_message += f"\n📋 Task Created for {current_user_name} (3-Day Feedback)\n"
        success_message += f"📅 Task Due: {format_date(three_day_due)}\n"
        if next_visit_date:
            success_message += f"📅 Booking record created for next visit\n"
        success_message += f"\n📍 Use /start to return to main menu"

        # Show appropriate menu based on user role
        chat_id = update.effective_chat.id
        role = get_user_role(chat_id)
        keyboard = get_admin_menu_keyboard() if role == 'admin' else get_staff_menu_keyboard()

        await update.message.reply_text(success_message, parse_mode='Markdown', reply_markup=keyboard)

    except Exception as e:
        print(f"Database error in next_visit_handler: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(
            f"❌ Error saving to database: {e}\n\n"
            "Please try again or contact support.",
            reply_markup=get_cancel_keyboard()
        )
        # Keep the conversation alive so user can retry
        return NEXT_VISIT

    # Clear user data
    context.user_data.clear()

    return ConversationHandler.END

# ========================================
# /search COMMAND
# ========================================
@authorized_only
async def search_patients(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the /search command - starts the search conversation."""
    return await search_start(update, context)

# ========================================
# SEARCH CONVERSATION FLOW
# ========================================
async def search_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the /search conversation - Step 1: Get patient name."""
    context.user_data.clear()
    context.user_data['conversation'] = 'search'
    await update.message.reply_text(
        "🔍 *Search Patients*\n\n"
        "👤 Please enter the patient's name (or type 'none' to skip):",
        reply_markup=get_cancel_keyboard(),
        parse_mode='Markdown'
    )
    return SEARCH_NAME

async def search_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle patient name input - Step 2: Get phone number."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if not text:
        await update.message.reply_text("❌ Input cannot be empty. Please enter the patient's name or 'none':",
                                      reply_markup=get_cancel_keyboard())
        return SEARCH_NAME

    # Store name (convert to lowercase 'none' for comparison)
    context.user_data['search_name'] = text if text.lower() != 'none' else None
    name_display = text if text.lower() != 'none' else 'None'

    await update.message.reply_text(
        "🔍 *Search Patients*\n\n"
        f"👤 Patient Name: {name_display}\n\n"
        "📱 Please enter the phone number (or type 'none' to skip):",
        reply_markup=get_cancel_keyboard(),
        parse_mode='Markdown'
    )
    return SEARCH_PHONE

async def search_phone_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle phone number input - Step 3: Perform search and show results."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if not text:
        await update.message.reply_text("❌ Input cannot be empty. Please enter the phone number or 'none':",
                                      reply_markup=get_cancel_keyboard())
        return SEARCH_PHONE

    # Store phone (convert to lowercase 'none' for comparison)
    search_name = context.user_data.get('search_name')
    search_phone = text if text.lower() != 'none' else None

    # Validate that at least one parameter is provided
    if not search_name and not search_phone:
        await update.message.reply_text(
            "❌ Both name and phone cannot be 'none'. Please provide at least one search parameter.",
            reply_markup=get_cancel_keyboard()
        )
        return SEARCH_PHONE

    # Perform search with appropriate query
    try:
        if search_name and search_phone:
            # Optimize: search with both parameters
            results = DatabaseManager.search_patients_by_name_and_phone(search_name, search_phone)
        elif search_name:
            # Search by name only
            results = DatabaseManager.search_patients_by_name(search_name)
        else:
            # Search by phone only
            results = DatabaseManager.search_patients_by_phone(search_phone)

        if not results:
            await update.message.reply_text(
                "🔍 No patients found matching your criteria."
            )
        else:
            # Build results message
            message = f"🔍 *Search Results*\n\n"
            if search_name and search_phone:
                message += f"👤 Name: {search_name}\n📱 Phone: {search_phone}\n"
            elif search_name:
                message += f"👤 Name: {search_name}\n"
            else:
                message += f"📱 Phone: {search_phone}\n"
            message += f"\nFound {len(results)} patient(s)\n\n"

            for idx, patient in enumerate(results[:10], 1):
                name = patient.get('patient_name', 'N/A')
                phone = patient.get('phone_number', 'N/A')
                patient_id = patient.get('patient_id', 'N/A')
                last_visit = patient.get('last_visit_date', 'N/A')

                message += f"*{idx}. {name}*\n"
                message += f"   📱 Phone: {phone}\n"
                message += f"   🏥 Patient ID: {patient_id}\n"
                message += f"   📅 Last Visit: {format_date(last_visit) if last_visit != 'N/A' else 'N/A'}\n\n"

                # Show last 3 visit notes
                visit_history = DatabaseManager.get_patient_visit_history(name, phone, limit=3)
                if visit_history:
                    message += f"   📝 *Recent Visits:*\n"
                    for visit in visit_history:
                        v_date = visit.get('visit_date', 'N/A')
                        v_remarks = visit.get('remarks', 'N/A')
                        is_preg = visit.get('is_pregnancy', False)
                        preg_status = "🤰" if is_preg else ""
                        message += f"   • {format_date(v_date)} {preg_status}: {v_remarks}\n"
                    message += "\n"

            if len(results) > 10:
                message += f"... and {len(results) - 10} more result(s).\n"

            await update.message.reply_text(message, parse_mode='Markdown')

            # Show Admin status buttons if user is Admin
            chat_id = update.effective_chat.id
            role = get_user_role(chat_id)
            if role == 'admin':
                for idx, patient in enumerate(results[:5], 1):
                    name = patient.get('patient_name', 'N/A')
                    phone = patient.get('phone_number', 'N/A')
                    name_encoded = name.replace(' ', '_')  # Encode spaces

                    status_keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("👶 Delivered", callback_data=f"status_delivered_{idx}_{name_encoded}_{phone}"),
                            InlineKeyboardButton("🚫 Unreachable", callback_data=f"status_unreachable_{idx}_{name_encoded}_{phone}"),
                            InlineKeyboardButton("🛑 Discontinued", callback_data=f"status_discontinued_{idx}_{name_encoded}_{phone}")
                        ]
                    ])

                    status_message = f"📊 *Status Update for {name}*\n📱 {phone}"
                    await update.message.reply_text(status_message, reply_markup=status_keyboard, parse_mode='Markdown')

    except Exception as e:
        print(f"Database error in search_phone_handler: {e}")
        await update.message.reply_text(
            "❌ Error searching database. Please try again or contact support."
        )

    # Clear user data and end conversation
    context.user_data.clear()
    return ConversationHandler.END

# ========================================
# TODAY'S BOOKINGS VIEW
# ========================================
@authorized_only
async def show_todays_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the Today's Bookings view with role-based date filtering."""
    try:
        chat_id = update.effective_chat.id
        role = get_user_role(chat_id)

        # Staff Role: Show only today's bookings (restricted view)
        if role == 'staff':
            # Get current date in IST (UTC+5:30)
            ist_offset = timedelta(hours=5, minutes=30)
            ist_timezone = timezone(ist_offset)
            today_utc = datetime.now(timezone.utc)
            today_ist = today_utc.astimezone(ist_timezone)
            today_str = today_ist.strftime('%Y-%m-%d')
            today_display = today_ist.strftime('%d %B %Y')

            # Fetch today's bookings
            bookings = DatabaseManager.fetch_bookings_by_date(today_str)

            if not bookings:
                message = (
                    f"📅 *Today's Bookings ({today_display})*\n\n"
                    f"📭 No bookings scheduled for today.\n\n"
                    f"Use /start to return to the main menu."
                )
                await update.message.reply_text(message, parse_mode='Markdown')
                return

            # Display bookings
            message = f"📅 *Today's Bookings ({today_display})*\n\n"
            message += f"📋 Total Appointments: {len(bookings)}\n\n"

            for idx, booking in enumerate(bookings, 1):
                patient_name = booking.get('patient_name', 'N/A')
                phone_number = booking.get('phone_number', 'N/A')
                patient_id = booking.get('patient_id', 'N/A')
                planned_date = booking.get('planned_date', 'N/A')

                message += f"*{idx}. {patient_name}*\n"
                message += f"   📱 Phone: {phone_number}\n"
                message += f"   🏥 Patient ID: {patient_id if patient_id != 'N/A' else 'Not set'}\n"
                message += f"   📅 Date: {format_date(planned_date) if planned_date != 'N/A' else 'N/A'}\n\n"

            message += f"Use /start to return to the main menu."
            await update.message.reply_text(message, parse_mode='Markdown')

        # Admin Role: Show date picker for custom date selection
        elif role == 'admin':
            # Get current date in IST (UTC+5:30)
            ist_offset = timedelta(hours=5, minutes=30)
            ist_timezone = timezone(ist_offset)
            today_utc = datetime.now(timezone.utc)
            today_ist = today_utc.astimezone(ist_timezone)
            today_str = today_ist.strftime('%Y-%m-%d')
            today_display = today_ist.strftime('%d %B %Y')

            # Store today's date in context for default display
            context.user_data['selected_booking_date'] = today_str

            message = (
                f"📅 *View Bookings - Admin Dashboard*\n\n"
                f"Select a date to view bookings:\n\n"
                f"Default: {today_display} (Today)"
            )
            await update.message.reply_text(
                message,
                reply_markup=get_booking_date_picker_keyboard(),
                parse_mode='Markdown'
            )

    except Exception as e:
        print(f"Database error: {e}")
        await update.message.reply_text(
            "❌ Error fetching bookings. Please try again or contact support."
        )


async def view_bookings_date_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle date selection from the booking date picker inline keyboard."""
    query = update.callback_query
    await query.answer()

    callback_data = query.data

    if callback_data == 'view_date_custom':
        # Ask user to enter custom date
        await query.message.edit_text(
            "📅 *Enter Custom Date*\n\n"
            "Please enter the date in YYYY-MM-DD format:",
            parse_mode='Markdown'
        )
        # Store conversation state
        context.user_data['conversation'] = 'view_bookings_custom_date'
        return

    # Extract date from callback data (format: view_date_YYYY-MM-DD)
    if callback_data.startswith('view_date_'):
        selected_date = callback_data[len('view_date_'):]
        context.user_data['selected_booking_date'] = selected_date

        # Display bookings for selected date
        await display_bookings_for_date(update, context, selected_date)


async def display_bookings_for_date(update: Update, context: ContextTypes.DEFAULT_TYPE, date_str: str) -> None:
    """Display bookings for a specific date with navigation options."""
    try:
        # Format date for display
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        date_display = dt.strftime('%d %B %Y')

        # Fetch bookings for the selected date
        bookings = DatabaseManager.fetch_bookings_by_date(date_str)

        # Build message
        message = f"📅 *Bookings for {date_display}*\n\n"

        if not bookings:
            message += "📭 No bookings scheduled for this date.\n\n"
        else:
            message += f"📋 Total Appointments: {len(bookings)}\n\n"

            for idx, booking in enumerate(bookings, 1):
                patient_name = booking.get('patient_name', 'N/A')
                phone_number = booking.get('phone_number', 'N/A')
                patient_id = booking.get('patient_id', 'N/A')
                planned_date = booking.get('planned_date', 'N/A')
                status = booking.get('status', 'N/A')

                message += f"*{idx}. {patient_name}*\n"
                message += f"   📱 Phone: {phone_number}\n"
                message += f"   🏥 Patient ID: {patient_id if patient_id != 'N/A' else 'Not set'}\n"
                message += f"   📅 Date: {format_date(planned_date) if planned_date != 'N/A' else 'N/A'}\n"
                if status and status != 'N/A':
                    message += f"   ✅ Status: {status}\n"
                message += "\n"

        # Add navigation options
        message += "📝 *Navigation Options*\n\n"
        message += "Select another date or return to main menu."

        # Edit message with date picker
        if update.callback_query:
            await update.callback_query.message.edit_text(
                message,
                reply_markup=get_booking_date_picker_keyboard(),
                parse_mode='Markdown'
            )
        else:
            # For custom date input (text message)
            await update.message.reply_text(
                message,
                reply_markup=get_booking_date_picker_keyboard(),
                parse_mode='Markdown'
            )

    except Exception as e:
        print(f"Error displaying bookings: {e}")
        error_message = "❌ Error fetching bookings. Please try again."
        if update.callback_query:
            await update.callback_query.message.edit_text(error_message)
        else:
            await update.message.reply_text(error_message)

# ========================================
# MANUAL BOOKING CONVERSATION HANDLER
# ========================================
@authorized_only
async def manual_booking_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the manual booking conversation - Step 1: Get patient name."""
    context.user_data.clear()
    context.user_data['conversation'] = 'manual_booking'
    await update.message.reply_text(
        "📅 *Manual Booking*\n\n"
        "Please enter the patient's name:",
        reply_markup=get_cancel_keyboard(),
        parse_mode='Markdown'
    )
    return BOOKING_NAME


async def booking_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle patient name input - Step 2: Get phone number."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if not text:
        await update.message.reply_text(
            "❌ Name cannot be empty. Please enter the patient's name:",
            reply_markup=get_cancel_keyboard()
        )
        return BOOKING_NAME

    context.user_data['name'] = text
    await update.message.reply_text(
        f"👤 Patient Name: {text}\n\n"
        "📱 Please enter the phone number:",
        reply_markup=get_cancel_keyboard()
    )
    return BOOKING_PHONE


async def booking_phone_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle phone number input - Step 3: Check for existing booking."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if not text:
        await update.message.reply_text(
            "❌ Phone number cannot be empty. Please enter the phone number:",
            reply_markup=get_cancel_keyboard()
        )
        return BOOKING_PHONE

    # Validate phone number - must be numeric and 10 digits
    if not validate_phone(text):
        await update.message.reply_text(
            "❌ Invalid phone number. Please enter a 10-digit numeric phone number:",
            reply_markup=get_cancel_keyboard()
        )
        return BOOKING_PHONE

    context.user_data['phone'] = text
    name = context.user_data.get('name')

    # Check if booking already exists
    existing_booking = DatabaseManager.fetch_booking(name, text)

    if existing_booking:
        planned_date = existing_booking.get('planned_date', 'N/A')
        context.user_data['existing_booking'] = existing_booking

        await update.message.reply_text(
            f"⚠️ *Booking already exists!*\n\n"
            f"👤 Patient: {name}\n"
            f"📱 Phone: {text}\n"
            f"📅 Existing Date: {format_date(planned_date) if planned_date != 'N/A' else planned_date}\n\n"
            f"Do you want to change the booking date?",
            reply_markup=ReplyKeyboardMarkup([['Yes', 'No'], ['Cancel']], resize_keyboard=True, one_time_keyboard=True),
            parse_mode='Markdown'
        )
        return BOOKING_CONFIRM_CHANGE
    else:
        # No existing booking, proceed to ask for date
        await update.message.reply_text(
            f"👤 Patient Name: {name}\n"
            f"📱 Phone: {text}\n\n"
            f"📅 Please enter the booking date (YYYY-MM-DD format):",
            reply_markup=get_today_keyboard()
        )
        return BOOKING_DATE


async def booking_confirm_change_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle booking change confirmation - Step 4: Ask for new date if confirmed."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if text == 'No':
        await update.message.reply_text(
            "❌ Booking change cancelled.\n\n"
            "Use /start to return to the main menu."
        )
        context.user_data.clear()
        return ConversationHandler.END

    if text == 'Yes':
        name = context.user_data.get('name')
        phone = context.user_data.get('phone')

        await update.message.reply_text(
            f"👤 Patient Name: {name}\n"
            f"📱 Phone: {phone}\n\n"
            f"📅 Please enter the new booking date (YYYY-MM-DD format):",
            reply_markup=get_today_keyboard()
        )
        return BOOKING_DATE

    await update.message.reply_text(
        "❌ Please select 'Yes' to change the booking or 'No' to cancel.",
        reply_markup=ReplyKeyboardMarkup([['Yes', 'No'], ['Cancel']], resize_keyboard=True, one_time_keyboard=True)
    )
    return BOOKING_CONFIRM_CHANGE


async def booking_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle booking date input - Step 5: Save the booking."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    # Validate date input (keyboard shows actual dates)
    if validate_date(text):
        context.user_data['booking_date'] = text
    else:
        await update.message.reply_text(
            "❌ Invalid date format. Please use YYYY-MM-DD format:",
            reply_markup=get_today_keyboard()
        )
        return BOOKING_DATE

    # Check if the selected date is a Sunday
    booking_date = context.user_data.get('booking_date')
    try:
        booking_dt = datetime.strptime(booking_date, '%Y-%m-%d')
        if booking_dt.weekday() == 6:  # Sunday (weekday 6)
            # Calculate next Monday (add 1 day to Sunday)
            monday_date = booking_dt + timedelta(days=1)
            monday_str = monday_date.strftime('%Y-%m-%d')

            # Show Sunday warning with inline keyboard
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(f"Move to Monday ({format_date(monday_str)})", callback_data=f"booking_monday|{monday_str}"),
                    InlineKeyboardButton("Enter a different date", callback_data="booking_newdate")
                ]
            ])

            await update.message.reply_text(
                f"⚠️ The selected date {format_date(booking_date)} falls on a Sunday.",
                reply_markup=keyboard
            )
            return BOOKING_SUNDAY_CONFIRM
    except ValueError:
        pass

    # Get booking details
    name = context.user_data.get('name')
    phone = context.user_data.get('phone')

    # Get the staff member's name who is creating this booking
    chat_id = update.effective_chat.id
    booked_by = get_user_name(chat_id)

    try:
        # Upsert the booking
        booking = DatabaseManager.upsert_booking(name, phone, booking_date, booked_by)

        if booking:
            is_update = 'existing_booking' in context.user_data

            # If booking is for today, create a task for confirming availability
            today_str = datetime.now().strftime('%Y-%m-%d')
            if booking_date == today_str and not is_update:
                # Create "0-Day Reminder" task for Nimisha
                DatabaseManager.create_patient_task(
                    assignee='Nimisha',
                    name=name,
                    phone=phone,
                    followup_type='0-Day Reminder',
                    status='Pending',
                    due_date=today_str
                )

            if is_update:
                success_message = (
                    f"✅ *Booking Updated Successfully!*\n\n"
                    f"👤 Patient: {name}\n"
                    f"📱 Phone: {phone}\n"
                    f"📅 New Booking Date: {format_date(booking_date)}\n"
                    f"👤 Booked By: {booked_by}\n\n"
                    f"Use /start to return to the main menu."
                )
            else:
                task_created_note = ""
                if booking_date == today_str:
                    task_created_note = f"\n📋 Task Created: 0-Day Reminder\n"
                success_message = (
                    f"✅ *Booking Created Successfully!*\n\n"
                    f"👤 Patient: {name}\n"
                    f"📱 Phone: {phone}\n"
                    f"📅 Booking Date: {format_date(booking_date)}\n"
                    f"👤 Booked By: {booked_by}\n"
                    f"{task_created_note}"
                    f"\nUse /start to return to the main menu."
                )
            await update.message.reply_text(success_message, parse_mode='Markdown')
        else:
            await update.message.reply_text(
                "❌ Error saving booking. Please try again or contact support."
            )

    except Exception as e:
        print(f"Database error: {e}")
        await update.message.reply_text(
            "❌ Error saving to database. Please try again or contact support."
        )

    # Clear user data
    context.user_data.clear()

    return ConversationHandler.END

async def show_new_case_inflow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the New Case Inflow view (Admin only)."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        await update.message.reply_text("❌ This feature is only available to Admin.")
        return

    try:
        # Show loading message
        loading_msg = await update.message.reply_text(
            "🌱 *New Case Inflow*\n\n"
            "Generating multi-year pregnancy inflow graph...\n"
            "Please wait...",
            parse_mode='Markdown'
        )

        # Fetch pregnancy counts
        preg_data = DatabaseManager.get_new_pregnancy_counts()

        # Generate the graph using lazy loading
        visualizers = _load_visualizers()
        buf = visualizers['pregnancy_inflow'](preg_data)

        # Send the photo
        if preg_data:
            years = sorted(set(entry['year'] for entry in preg_data))
            total_pregnancies = sum(entry['count'] for entry in preg_data)
            caption = f"🌱 KadeejaClinic: Monthly New Pregnancy Registrations\nYears: {len(years)} ({min(years)}-{max(years)})\nTotal Pregnancies: {total_pregnancies}"
        else:
            caption = "🌱 KadeejaClinic: Monthly New Pregnancy Registrations\nNo pregnancy data available."

        # Delete loading message and send photo
        await loading_msg.delete()
        await update.message.reply_photo(photo=buf, caption=caption)

    except Exception as e:
        print(f"Error generating new case inflow graph: {e}")
        await update.message.reply_text(
            f"❌ Error generating new case inflow graph: {e}\n\n"
            "Please try again or contact support."
        )

# ========================================
# /tasks COMMAND
# ========================================
@authorized_only
async def show_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE, task_view: Optional[str] = None) -> None:
    """Handle the /tasks command with role-based views.

    Args:
        task_view: 'planned' to show only Planned Patient Calls,
                  'unplanned' to show only Unplanned Tasks,
                  None to show both
    """
    chat_id = update.effective_chat.id
    role = get_user_role(chat_id)
    user_name = get_user_name(chat_id)

    try:
        if role == 'staff':
            # Staff View: Filter 'Pending' tasks by their staff_name
            print(f"[DEBUG show_tasks] Fetching tasks for staff: {user_name}")
            pending_tasks = DatabaseManager.fetch_patient_tasks(assignee=user_name, status='Pending')
            admin_tasks = DatabaseManager.fetch_admin_tasks(assignee=user_name, status='Pending')

            print(f"[DEBUG show_tasks] Total pending_tasks: {len(pending_tasks)}")
            for task in pending_tasks:
                print(f"  - {task.get('followup_type')}: {task.get('patient_name')} (assignee: {task.get('assignee')})")

            message = f"📋 *Your Pending Tasks*\n\n"

            # Sort patient tasks by priority
            # Priority 1: 0-Day Reminder
            # Priority 2: 3-Day Reminder
            # Priority 3: 14-Day Reminder
            # Priority 4: 3-Day Feedback (Post-Visit)
            # Priority 5: No Visit
            priority_order = {
                "0-Day Reminder": 1,
                "3-Day Reminder": 2,
                "14-Day Reminder": 3,
                "3-Day Feedback": 4,
                "No Visit": 5
            }

            # Filter out unplanned tasks from pending_tasks
            planned_tasks = [t for t in pending_tasks if t.get('patient_name') != 'Unplanned Task']

            print(f"[DEBUG show_tasks] Planned tasks after filter: {len(planned_tasks)}")
            print(f"[DEBUG show_tasks] Unplanned tasks: {len(pending_tasks) - len(planned_tasks)}")
            for task in planned_tasks:
                print(f"  - {task.get('followup_type')}: {task.get('patient_name')}")

            # Sort tasks by priority, then by due date
            pending_tasks_sorted = sorted(
                planned_tasks,
                key=lambda t: (
                    priority_order.get(t.get('followup_type', ''), 99),
                    t.get('due_date', '9999-12-31')
                )
            )

            # Patient Tasks (Planned Calls) - Sorted by Priority with Inline Buttons
            # Only show if task_view is 'planned' or None (both)
            if task_view in ('planned', None):
                if pending_tasks_sorted:
                    message += "📞 *Planned Patient Calls:*\n\n"
                    for idx, task in enumerate(pending_tasks_sorted, 1):
                        patient = task.get('patient_name', 'N/A')
                        phone = task.get('phone_number', 'N/A')
                        task_type = task.get('followup_type', 'N/A')
                        due = task.get('due_date', 'N/A')
                        assignee = task.get('assignee', 'N/A')

                        # Get priority icon based on task type
                        priority_icons = {
                            "0-Day Reminder": "🔴",
                            "3-Day Reminder": "🟠",
                            "14-Day Reminder": "🟡",
                            "3-Day Feedback": "🟢",
                            "No Visit": "🟣"
                        }
                        priority_icon = priority_icons.get(task_type, "⚪")

                        # Create callback data for buttons (use pipe as delimiter to handle spaces)
                        call_callback = f"call|{idx}|{patient}|{phone}|{task_type}|{assignee}"
                        complete_callback = f"complete|{idx}|{patient}|{phone}|{task_type}|{assignee}"

                        # Calculate Next Visit Date based on task type
                        next_visit_date = None
                        if task_type == '0-Day Reminder' and due != 'N/A':
                            next_visit_date = format_date(due)
                        elif task_type == '3-Day Reminder' and due != 'N/A':
                            next_visit_dt = datetime.strptime(due, '%Y-%m-%d') + timedelta(days=3)
                            next_visit_date = format_date(next_visit_dt.strftime('%Y-%m-%d'))
                        elif task_type == '14-Day Reminder' and due != 'N/A':
                            next_visit_dt = datetime.strptime(due, '%Y-%m-%d') + timedelta(days=14)
                            next_visit_date = format_date(next_visit_dt.strftime('%Y-%m-%d'))

                        # Build task message with Next Visit Date if applicable
                        task_message = (
                            f"{priority_icon} *{idx}. {patient}*\n"
                            f"📱 {phone}\n"
                            f"📋 {task_type}\n"
                        )
                        if next_visit_date:
                            task_message += f"📅 Next Visit: {next_visit_date}\n"
                        task_message += f"📅 Due: {format_date(due) if due != 'N/A' else 'No due date'}"

                        keyboard = InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("📞 Call Now", callback_data=call_callback),
                                InlineKeyboardButton("✅ Completed", callback_data=complete_callback)
                            ]
                        ])

                        await update.message.reply_text(task_message, reply_markup=keyboard, parse_mode='Markdown')
                else:
                    message += "📞 *Planned Patient Calls:*\n   No pending tasks.\n\n"
                    await update.message.reply_text(message, parse_mode='Markdown')

            # Unplanned Tasks (from admin_tasks table - includes 8AM chores and admin-assigned tasks)
            # Only show if task_view is 'unplanned' or None (both)
            if task_view in ('unplanned', None):
                if admin_tasks:
                    message = "🧹 *Unplanned Tasks:*\n\n"
                    print(f"[DEBUG show_tasks] Admin tasks before sorting:")
                    for i, task in enumerate(admin_tasks):
                        print(f"  [{i}] {task.get('task_message')}, due={task.get('due_date')}, id={task.get('id')}")

                    # Sort admin tasks by due date
                    admin_tasks_sorted = sorted(admin_tasks, key=lambda t: t.get('due_date', '9999-12-31'))
                    print(f"[DEBUG show_tasks] Found {len(admin_tasks_sorted)} admin tasks after sorting")
                    for idx, task in enumerate(admin_tasks_sorted, 1):
                        msg = task.get('task_message', 'N/A')
                        due = task.get('due_date', 'N/A')
                        task_id = task.get('id', 'N/A')  # Get the UUID of the task

                        print(f"[DEBUG show_tasks] Admin task {idx}: msg={msg}, due={due}, task_id={task_id}")

                        # Create callback data for completion
                        # Use format: admin_complete_{task_id}
                        complete_callback = f"admin_complete_{task_id}"

                        # Remove Markdown formatting characters to avoid parsing errors
                        task_display = (
                            f"🧹 {idx}. {msg}\n"
                            f"📅 Due: {format_date(due) if due and due != 'N/A' else 'No due date'}"
                        )

                        print(f"[DEBUG show_tasks] Task display: {task_display}")

                        keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton("✅ Mark Done", callback_data=complete_callback)]
                        ])

                        await update.message.reply_text(task_display, reply_markup=keyboard)
                else:
                    message = "🧹 *Unplanned Tasks:*\n   No pending tasks.\n\n"
                    await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        print(f"[ERROR] Exception in show_tasks: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(
            "❌ Error fetching tasks. Please try again or contact support."
        )


# ========================================
# /pregnancy_registry COMMAND
# ========================================
@authorized_only
async def show_pregnancy_registry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the pregnancy registry command (Admin only)."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        await update.message.reply_text("❌ This command is only available to Admin.")
        return

    try:
        pregnancies = DatabaseManager.fetch_all_pregnancies()

        message = "🤰 *Pregnancy Registry*\n\n"

        if not pregnancies:
            message += "No active pregnancies registered.\n"
        else:
            message += f"Total Active Pregnancies: {len(pregnancies)}\n\n"

            for idx, preg in enumerate(pregnancies, 1):
                name = preg.get('patient_name', 'N/A')
                phone = preg.get('phone_number', 'N/A')
                edc = preg.get('edc_date', 'N/A')
                status = preg.get('status', 'N/A')

                message += f"{idx}. *{name}*\n"
                message += f"   📱 {phone}\n"
                message += f"   📅 EDC: {format_date(edc)}\n"
                message += f"   📊 Status: {status}\n\n"

        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        print(f"Database error: {e}")
        await update.message.reply_text(
            "❌ Error fetching pregnancy registry. Please try again or contact support."
        )

# ========================================
# EDC ANNUAL VIEW CONVERSATION (EDC_VIEW_CONV)
# ========================================
@authorized_only
async def edc_view_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the EDC Annual View - Step 1: Select year."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        if update.message:
            await update.message.reply_text("❌ This feature is only available to Admin.")
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("❌ This feature is only available to Admin.")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data['conversation'] = 'edc_view'

    # Get current year
    current_year = datetime.now().year
    next_year = current_year + 1

    # Create inline keyboard with year options
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(str(current_year), callback_data=f'edc_year_{current_year}'),
            InlineKeyboardButton(str(next_year), callback_data=f'edc_year_{next_year}')
        ],
        [
            InlineKeyboardButton("Cancel", callback_data='edc_cancel')
        ]
    ])

    message = (
        f"📊 *EDC Annual View*\n\n"
        f"Select the year to view EDCs:\n"
    )

    if update.message:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')
    elif update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return EDC_VIEW_SELECT_YEAR

async def edc_view_year_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle year selection - Step 2: Generate graph."""
    query = update.callback_query
    await query.answer()

    if query.data == 'edc_cancel':
        return await cancel_conversation(update, context)

    # Extract year from callback data
    year = int(query.data.replace('edc_year_', ''))

    # Show loading message
    await query.edit_message_text(
        f"📊 *EDC Annual View*\n\n"
        f"Generating EDC graph for {year}...\n"
        f"Please wait...",
        parse_mode='Markdown'
    )

    # Fetch EDC data
    edc_data = DatabaseManager.get_edcs_for_year(year)

    if not edc_data:
        error_message = (
            f"📊 *EDC Annual View*\n\n"
            f"❌ No EDCs found for {year}.\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')
        context.user_data.clear()
        return ConversationHandler.END

    # Generate the EDC graph
    try:
        visualizers = _load_visualizers()
        buf = visualizers['edc_annual'](edc_data, year)

        # Send the photo
        await query.message.reply_photo(
            photo=buf,
            caption=f"📊 EDC Planner for {year}\nTotal Patients: {len(edc_data)}"
        )

        # Update the original message
        success_message = (
            f"✅ EDC graph for {year} generated successfully!\n\n"
            f"Total Patients: {len(edc_data)}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(success_message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error generating EDC graph: {e}")
        error_message = (
            f"❌ Error generating EDC graph: {e}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')

    context.user_data.clear()
    return ConversationHandler.END

# ========================================
# TRENDS ANALYTICS CONVERSATION (TRENDS_CONV)
# ========================================
@authorized_only
async def trends_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the Trends Analytics - Step 1: Select feature type."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        if update.message:
            await update.message.reply_text("❌ This feature is only available to Admin.")
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("❌ This feature is only available to Admin.")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data['conversation'] = 'trends'

    # Create inline keyboard with trend options
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👶 Delivery Trend", callback_data='trends_delivery'),
            InlineKeyboardButton("📉 Attrition Trend", callback_data='trends_attrition'),
            InlineKeyboardButton("📈 Visit Trend", callback_data='trends_visit')
        ],
        [
            InlineKeyboardButton("Cancel", callback_data='trends_cancel')
        ]
    ])

    message = (
        f"📊 *Trends Analytics*\n\n"
        f"Select a trend to analyze:\n"
    )

    if update.message:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')
    elif update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return TRENDS_SELECT_YEAR


async def trends_delivery_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle delivery trend selection - Step 2: Select year."""
    query = update.callback_query
    await query.answer()

    if query.data == 'trends_cancel':
        return await cancel_conversation(update, context)

    # Show year selection
    current_year = datetime.now().year
    next_year = current_year + 1

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(str(current_year), callback_data=f'trends_year_{current_year}'),
            InlineKeyboardButton(str(next_year), callback_data=f'trends_year_{next_year}')
        ],
        [
            InlineKeyboardButton("Cancel", callback_data='trends_cancel')
        ]
    ])

    message = (
        f"👶 *Delivery Trend*\n\n"
        f"Select the year to view monthly delivery trends:\n"
    )

    await query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return TRENDS_SELECT_YEAR


async def trends_attrition_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle attrition trend selection - Step 2: Select year."""
    query = update.callback_query
    await query.answer()

    if query.data == 'trends_cancel':
        return await cancel_conversation(update, context)

    # Show year selection
    current_year = datetime.now().year
    next_year = current_year + 1

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(str(current_year), callback_data=f'attrition_year_{current_year}'),
            InlineKeyboardButton(str(next_year), callback_data=f'attrition_year_{next_year}')
        ],
        [
            InlineKeyboardButton("Cancel", callback_data='trends_cancel')
        ]
    ])

    message = (
        f"📉 *Attrition Trend*\n\n"
        f"Select the year to view monthly attrition trends:\n"
    )

    await query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return ATTRITION_TREND_SELECT_YEAR


async def trends_visit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle visit trend selection - Step 2: Select year."""
    query = update.callback_query
    await query.answer()

    if query.data == 'trends_cancel':
        return await cancel_conversation(update, context)

    # Show year selection
    current_year = datetime.now().year
    next_year = current_year + 1

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(str(current_year), callback_data=f'visit_year_{current_year}'),
            InlineKeyboardButton(str(next_year), callback_data=f'visit_year_{next_year}')
        ],
        [
            InlineKeyboardButton("Cancel", callback_data='trends_cancel')
        ]
    ])

    message = (
        f"📈 *Visit Trend*\n\n"
        f"Select the year to view monthly visit trends:\n"
    )

    await query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return VISIT_TREND_SELECT_YEAR


async def trends_year_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle year selection - Step 3: Generate delivery trend graph."""
    query = update.callback_query
    await query.answer()

    if query.data == 'trends_cancel':
        return await cancel_conversation(update, context)

    # Extract year from callback data
    year = int(query.data.replace('trends_year_', ''))

    # Show loading message
    await query.edit_message_text(
        f"👶 *Delivery Trend*\n\n"
        f"Generating delivery trend graph for {year}...\n"
        f"Please wait...",
        parse_mode='Markdown'
    )

    # Fetch delivery trend data
    trend_data = DatabaseManager.get_monthly_delivery_trends(year)

    if not trend_data:
        error_message = (
            f"👶 *Delivery Trend*\n\n"
            f"❌ No delivery data found for {year}.\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')
        context.user_data.clear()
        return ConversationHandler.END

    # Generate the delivery trend graph
    try:
        visualizers = _load_visualizers()
        buf = visualizers['delivery_trend'](trend_data, year)

        # Calculate total active pregnancies
        total_deliveries = sum(trend_data.values())

        # Send the photo
        await query.message.reply_photo(
            photo=buf,
            caption=f"👶 Monthly Delivery Trend for {year}\nTotal Expected Deliveries: {total_deliveries}"
        )

        # Update the original message
        success_message = (
            f"✅ Delivery trend graph for {year} generated successfully!\n\n"
            f"Total Expected Deliveries: {total_deliveries}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(success_message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error generating delivery trend graph: {e}")
        error_message = (
            f"❌ Error generating delivery trend graph: {e}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')

    # Clean up user_data and trigger garbage collection
    context.user_data.clear()
    gc.collect()

    return ConversationHandler.END


async def attrition_year_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle year selection for attrition trend - Step 3: Generate attrition trend graph."""
    query = update.callback_query
    await query.answer()

    if query.data == 'trends_cancel':
        return await cancel_conversation(update, context)

    # Extract year from callback data
    year = int(query.data.replace('attrition_year_', ''))

    # Show loading message
    await query.edit_message_text(
        f"📉 *Attrition Trend*\n\n"
        f"Generating attrition trend graph for {year}...\n"
        f"Please wait...",
        parse_mode='Markdown'
    )

    # Fetch attrition trend data
    attrition_data = DatabaseManager.get_monthly_attrition_trends(year)

    if not attrition_data or sum(attrition_data) == 0:
        error_message = (
            f"📉 *Attrition Trend*\n\n"
            f"❌ No attrition data found for {year}.\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')
        context.user_data.clear()
        return ConversationHandler.END

    # Generate the attrition trend graph
    try:
        visualizers = _load_visualizers()
        buf = visualizers['attrition_trend'](attrition_data, year)

        # Calculate total attrition cases
        total_attrition = sum(attrition_data)

        # Send the photo with specified caption format
        await query.message.reply_photo(
            photo=buf,
            caption=f"📉 Attrition Trend {year}: Total {total_attrition} cases."
        )

        # Update the original message
        success_message = (
            f"✅ Attrition trend graph for {year} generated successfully!\n\n"
            f"Total Attrition Cases: {total_attrition}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(success_message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error generating attrition trend graph: {e}")
        error_message = (
            f"❌ Error generating attrition trend graph: {e}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')

    # Clean up user_data and trigger garbage collection
    context.user_data.clear()
    gc.collect()

    return ConversationHandler.END


async def visit_trend_year_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle year selection for visit trend - Step 3: Generate visit trend graph."""
    query = update.callback_query
    await query.answer()

    if query.data == 'trends_cancel':
        return await cancel_conversation(update, context)

    # Extract year from callback data
    year = int(query.data.replace('visit_year_', ''))

    # Show loading message
    await query.edit_message_text(
        f"📈 *Visit Trend*\n\n"
        f"Generating visit trend graph for {year}...\n"
        f"Please wait...",
        parse_mode='Markdown'
    )

    # Fetch visit trend data
    visit_data = DatabaseManager.get_monthly_visit_trends(year)

    if not visit_data or sum(visit_data) == 0:
        error_message = (
            f"📈 *Visit Trend*\n\n"
            f"❌ No visit data found for {year}.\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')
        context.user_data.clear()
        return ConversationHandler.END

    # Generate the visit trend graph
    try:
        visualizers = _load_visualizers()
        buf = visualizers['visit_trend'](visit_data, year)

        # Calculate total visits
        total_visits = sum(visit_data)

        # Send the photo with specified caption format
        await query.message.reply_photo(
            photo=buf,
            caption=f"📈 Visit Trend {year}: Total {total_visits} visits recorded."
        )

        # Update the original message
        success_message = (
            f"✅ Visit trend graph for {year} generated successfully!\n\n"
            f"Total Visits: {total_visits}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(success_message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error generating visit trend graph: {e}")
        error_message = (
            f"❌ Error generating visit trend graph: {e}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')

    # Clean up user_data and trigger garbage collection
    context.user_data.clear()
    gc.collect()

    return ConversationHandler.END

# ========================================
# COMPARATIVE ATTRITION HANDLER
# ========================================
@authorized_only
async def attrition_view_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the Comparative Attrition View request."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        if update.message:
            await update.message.reply_text("❌ This feature is only available to Admin.")
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("❌ This feature is only available to Admin.")
        return

    try:
        # Get current year
        current_year = datetime.now().year
        prev_year = current_year - 1

        # Show loading message
        if update.message:
            loading_msg = await update.message.reply_text(
                f"📊 *Comparative Attrition Analysis*\n\n"
                f"Generating graph for {prev_year} vs {current_year}...\n"
                f"Please wait...",
                parse_mode='Markdown'
            )
        elif update.callback_query:
            await update.callback_query.answer()
            loading_msg = await update.callback_query.edit_message_text(
                f"📊 *Comparative Attrition Analysis*\n\n"
                f"Generating graph for {prev_year} vs {current_year}...\n"
                f"Please wait...",
                parse_mode='Markdown'
            )

        # Fetch attrition data
        attrition_data = DatabaseManager.get_attrition_counts_comparative(current_year)

        # Generate the comparative graph using lazy loading
        visualizers = _load_visualizers()
        buf = visualizers['attrition'](current_year, attrition_data)

        # Create drill-down buttons
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"View {prev_year} List", callback_data=f'attrition_list_{prev_year}'),
                InlineKeyboardButton(f"View {current_year} List", callback_data=f'attrition_list_{current_year}')
            ],
            [InlineKeyboardButton("Cancel", callback_data='attrition_cancel')]
        ])

        # Send the photo with keyboard
        prev_total = sum(attrition_data.get('prev_monthly_counts', [0] * 12))
        curr_total = sum(attrition_data.get('curr_monthly_counts', [0] * 12))

        if update.message:
            await loading_msg.delete()
            await update.message.reply_photo(
                photo=buf,
                caption=f"📊 Comparative Attrition: {prev_year} vs {current_year}\nTotal: {prev_total + curr_total} patients",
                reply_markup=keyboard
            )
        elif update.callback_query:
            await update.callback_query.edit_message_text(
                f"✅ Graph generated!\n\nSee photo below.",
                parse_mode='Markdown'
            )
            await update.callback_query.message.reply_photo(
                photo=buf,
                caption=f"📊 Comparative Attrition: {prev_year} vs {current_year}\nTotal: {prev_total + curr_total} patients",
                reply_markup=keyboard
            )

    except Exception as e:
        print(f"Error generating comparative attrition graph: {e}")
        error_message = (
            f"❌ Error generating graph: {e}\n\n"
            f"Use /start to return to the main menu."
        )
        if update.message:
            await update.message.reply_text(error_message, parse_mode='Markdown')
        elif update.callback_query:
            await update.callback_query.edit_message_text(error_message, parse_mode='Markdown')

async def attrition_list_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the drill-down list request for a specific year."""
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id

    if not is_admin(chat_id):
        await query.edit_message_text("❌ This feature is only available to Admin.")
        return

    callback_data = query.data

    if callback_data == 'attrition_cancel':
        await query.edit_message_text(
            "❌ Operation cancelled.\n\n"
            "Use /start to return to the main menu."
        )
        return

    # Extract year from callback data
    try:
        year = int(callback_data.replace('attrition_list_', ''))
    except (ValueError, AttributeError):
        await query.edit_message_text("❌ Invalid year selection.")
        return

    try:
        # Fetch attrition details for the year
        attrition_details = DatabaseManager.get_attrition_details_by_year(year)

        # Build the message with monthly breakdown
        months = ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December']

        message = f"📋 *Attrition Details - {year}*\n\n"

        total_count = 0
        for month_idx, month_name in enumerate(months, 1):
            patients = attrition_details.get(month_idx, [])
            if patients:
                message += f"📅 *{month_name}* ({len(patients)} patients)\n"
                for patient in patients:
                    status = patient.get('status', 'N/A')
                    gravida = patient.get('gravida_status', 'N/A')
                    edc_date = patient.get('edc_date', 'N/A')
                    name = patient.get('patient_name', 'N/A')
                    phone = patient.get('phone_number', 'N/A')

                    # Status icons
                    status_icons = {
                        'Delivered': '👶',
                        'Dropped': '🛑',
                        'Unreachable': '🚫',
                        'Active': ''
                    }
                    status_icon = status_icons.get(status, '')

                    message += f"  • {status_icon} {name} ({gravida})\n"
                    message += f"    📱 {phone} | 📅 {format_date(edc_date)} | 📊 {status}\n"
                message += "\n"
                total_count += len(patients)

        if total_count == 0:
            message += f"📭 No attrition records found for {year}.\n"
        else:
            message += f"📊 Total Attrition: {total_count} patients\n"

        message += "\nUse /start to return to the main menu."

        # Check if message is too long (Telegram limit is 4096 chars)
        if len(message) > 4000:
            message = (
                f"📋 *Attrition Details - {year}*\n\n"
                f"📊 Total Attrition: {total_count} patients\n\n"
                f"The detailed list is too long to display in one message.\n"
                f"Please use the PK Row Editor to view specific patient details.\n\n"
                f"Use /start to return to the main menu."
            )

        await query.edit_message_text(message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error fetching attrition details: {e}")
        error_message = (
            f"❌ Error fetching attrition details: {e}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')

# ========================================
# CANCEL HANDLER
# ========================================
async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the current conversation."""
    if update.message:
        await update.message.reply_text(
            "❌ Operation cancelled.\n\n"
            "Use /start to return to the main menu."
        )
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "❌ Operation cancelled.\n\n"
            "Use /start to return to the main menu."
        )

    context.user_data.clear()
    return ConversationHandler.END

# ========================================
# ADMIN TASK DATA REPORT HANDLER
# ========================================
@authorized_only
async def handle_admin_task_data_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle staff data input for admin tasks that require details."""
    text = update.message.text.strip()
    task_id = context.user_data.get('awaiting_admin_task_data')
    chat_id = update.effective_chat.id
    staff_name = get_user_name(chat_id)

    if not task_id:
        await update.message.reply_text("❌ Error: Task information not found.")
        context.user_data.pop('awaiting_admin_task_data', None)
        return

    try:
        # Get the task details
        admin_task = None
        try:
            task_result = supabase.table('admin_tasks').select('*').eq('id', task_id).execute()
            if task_result.data:
                admin_task = task_result.data[0]
        except Exception as e:
            print(f"Error fetching admin task: {e}")

        if not admin_task:
            await update.message.reply_text("❌ Error: Task not found or may have been already completed.")
            context.user_data.pop('awaiting_admin_task_data', None)
            return

        task_message = admin_task.get('task_message', 'Unknown Task')

        # Send the data report to all admin users
        admins = DatabaseManager.get_staff_by_role('admin')

        admin_notification = (
            f"🔔 **Task Data Report from {staff_name}**\n\n"
            f"📋 Task: {task_message}\n\n"
            f"**Detail Provided**: {text}"
        )

        for admin in admins:
            admin_id = admin.get('telegram_id')
            if admin_id:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_notification, parse_mode='Markdown')
                except Exception as e:
                    print(f"Error notifying admin {admin_id}: {e}")

        # Mark the task as completed in database
        DatabaseManager.update_admin_task_status(task_id, 'Completed')

        # Clear the awaiting flag
        context.user_data.pop('awaiting_admin_task_data', None)

        # Notify staff of success
        await update.message.reply_text(
            "✅ Task completed and details forwarded to Admin.\n\n"
            "Use /start to return to the main menu."
        )

    except Exception as e:
        print(f"Error handling admin task data report: {e}")
        await update.message.reply_text(
            f"❌ Error processing your input: {e}\n\n"
            "Please try again or contact support."
        )
        context.user_data.pop('awaiting_admin_task_data', None)

# ========================================
# MESSAGE HANDLERS FOR MENU BUTTONS
# ========================================
@authorized_only
async def handle_menu_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle menu button presses and booking date input."""
    text = update.message.text.strip()

    # Check if user is in visit conversation - don't interfere
    if context.user_data.get('conversation') == 'visit':
        # Let the visit conversation handler handle this
        return

    # Check if user is in booking input conversation
    if context.user_data.get('conversation') == 'booking_input':
        await handle_booking_date_input(update, context)
        return

    # Check if user is providing data for an admin task
    if 'awaiting_admin_task_data' in context.user_data:
        await handle_admin_task_data_report(update, context)
        return

    # Check if user is entering custom date for view bookings (Admin only)
    if context.user_data.get('conversation') == 'view_bookings_custom_date':
        # Validate date format
        if validate_date(text):
            selected_date = text
            context.user_data.pop('conversation', None)
            await display_bookings_for_date(update, context, selected_date)
        else:
            await update.message.reply_text(
                "❌ Invalid date format. Please use YYYY-MM-DD format (e.g., 2026-04-03):",
                parse_mode='Markdown'
            )
        return

    if text == '🤰 Pregnancy Registry':
        await show_pregnancy_registry(update, context)
    elif text == '📊 Trends':
        await trends_start(update, context)
    elif text == '📞 Planned Patient Calls':
        await show_tasks(update, context, task_view='planned')
    elif text == '🧹 Unplanned Tasks':
        await show_tasks(update, context, task_view='unplanned')
    elif text == '📅 View Bookings':
        await show_todays_bookings(update, context)
    else:
        await update.message.reply_text(
            "Please use the menu buttons or commands.\n\n"
            "Type /start to see the main menu.",
            parse_mode='Markdown'
        )

# ========================================
# INLINE BUTTON CALLBACK HANDLERS
# ========================================
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all inline button callback queries."""
    query = update.callback_query
    await query.answer()

    callback_data = query.data
    chat_id = query.message.chat_id
    role = get_user_role(chat_id)

    print(f"[DEBUG handle_callback_query] callback_data={callback_data}, role={role}")

    # ========================================
    # STAFF TASK CALLBACKS
    # ========================================
    if callback_data.startswith('call|'):
        # Handle Call Now button
        parts = callback_data.split('|')
        print(f"[DEBUG handle_callback_query] Call Now parts: {parts}")
        idx = parts[1]
        patient_name = parts[2]
        phone = parts[3]
        task_type = parts[4]
        assignee = parts[5]

        # Show call prompt with patient info
        call_message = (
            f"📞 *Calling {patient_name}*\n\n"
            f"📱 Phone: {phone}\n"
            f"📋 Task: {task_type}\n"
            f"👤 Assigned to: {assignee}\n\n"
            f"Tap '✅ Completed' when done to update the task status."
        )
        await query.edit_message_text(call_message, parse_mode='Markdown')

    elif callback_data.startswith('complete|'):
        # Handle Completed button - Start booking verification flow
        parts = callback_data.split('|')
        print(f"[DEBUG handle_callback_query] Complete parts: {parts}")
        idx = parts[1]
        patient_name = parts[2]
        phone = parts[3]
        task_type = parts[4]
        assignee = parts[5]

        # Store task info for the booking verification flow
        context.user_data['task_info'] = {
            'patient_name': patient_name,
            'phone': phone,
            'task_type': task_type,
            'assignee': assignee
        }

        # Check if there's a future booking for this patient
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            # Query bookings for future dates (only Auto-generated bookings)
            bookings_result = supabase.table('bookings').select('*')\
                .ilike('patient_name', patient_name)\
                .eq('phone_number', phone)\
                .eq('booked_by', 'Auto')\
                .gt('planned_date', today)\
                .order('planned_date')\
                .limit(1)\
                .execute()

            future_booking = None
            if bookings_result.data:
                future_booking = bookings_result.data[0]

            # Show booking verification keyboard
            if future_booking:
                booking_date = future_booking.get('planned_date')
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Confirm Booking", callback_data='booking_confirm'),
                        InlineKeyboardButton("❌ No Booking", callback_data='booking_none'),
                        InlineKeyboardButton("📅 Change Date", callback_data='booking_change')
                    ]
                ])
                verify_message = (
                    f"📅 *Next Booking Verification*\n\n"
                    f"👤 Patient: {patient_name}\n"
                    f"📱 Phone: {phone}\n"
                    f"📋 Task: {task_type}\n\n"
                    f"🗓️ Next Booking Found: {format_date(booking_date)}\n\n"
                    f"Please confirm the next visit date:"
                )
                await query.edit_message_text(verify_message, reply_markup=keyboard, parse_mode='Markdown')
            else:
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("➕ Add Booking", callback_data='booking_add'),
                        InlineKeyboardButton("❌ No Booking", callback_data='booking_none')
                    ]
                ])
                verify_message = (
                    f"📅 *Next Booking Verification*\n\n"
                    f"👤 Patient: {patient_name}\n"
                    f"📱 Phone: {phone}\n"
                    f"📋 Task: {task_type}\n\n"
                    f"❗ No future booking found for this patient.\n\n"
                    f"Would you like to add a next visit date?"
                )
                await query.edit_message_text(verify_message, reply_markup=keyboard, parse_mode='Markdown')

        except Exception as e:
            print(f"Error checking booking: {e}")
            # Proceed to mark as completed anyway
            await mark_task_completed(query, context)

    elif callback_data.startswith('admin_complete_'):
        # Handle Mark Done button for admin tasks
        task_id = callback_data.replace('admin_complete_', '')

        # Get staff name from context
        staff_name = get_user_name(chat_id)

        try:
            # Get the task details first for notification
            admin_task = None
            try:
                task_result = supabase.table('admin_tasks').select('*').eq('id', task_id).execute()
                if task_result.data:
                    admin_task = task_result.data[0]
            except Exception as e:
                print(f"Error fetching admin task: {e}")

            if not admin_task:
                await query.edit_message_text(
                    f"❌ Error: Task not found or may have been already completed.",
                    parse_mode='Markdown'
                )
                return

            task_message = admin_task.get('task_message', 'Unknown Task')

            # Check if task requires data input from staff
            requires_data = 'receptionist names' in task_message.lower() or 'lunch timings' in task_message.lower()

            if requires_data:
                # Set context to await data input and ask staff for details
                context.user_data['awaiting_admin_task_data'] = task_id
                await query.edit_message_text(
                    f"📝 *Please provide the details*\n\n"
                    f"Task: {task_message}\n\n"
                    f"Please provide the details (e.g., Names or Timings):",
                    parse_mode='Markdown'
                )
            else:
                # Normal completion - mark task as completed
                DatabaseManager.update_admin_task_status(task_id, 'Completed')

                # Get admin(s) to notify
                admins = DatabaseManager.get_staff_by_role('admin')

                # Send notification to all admins
                admin_notification = (
                    f"✅ *Task Completed by {staff_name}*\n\n"
                    f"📋 Task: {task_message}\n\n"
                    f"Status has been updated to 'Completed'."
                )

                for admin in admins:
                    admin_id = admin.get('telegram_id')
                    if admin_id:
                        try:
                            await context.bot.send_message(chat_id=admin_id, text=admin_notification, parse_mode='Markdown')
                        except Exception as e:
                            print(f"Error notifying admin {admin_id}: {e}")

                # Update the message for the staff member
                completion_message = (
                    f"✅ *Task Completed*\n\n"
                    f"📋 Task: {task_message}\n\n"
                    f"Admin has been notified."
                )

                await query.edit_message_text(completion_message, parse_mode='Markdown')

        except Exception as e:
            print(f"Error completing admin task: {e}")
            await query.edit_message_text(
                f"❌ Error completing task: {e}\n\n"
                f"Please try again or contact support.",
                parse_mode='Markdown'
            )

    # ========================================
    # STAFF BOOKING VERIFICATION CALLBACKS
    # ========================================
    elif callback_data.startswith('booking_'):
        task_info = context.user_data.get('task_info', {})
        patient_name = task_info.get('patient_name')
        phone = task_info.get('phone')

        if callback_data == 'booking_confirm':
            # Confirm existing booking and mark task as completed
            await mark_task_completed(query, context)

        elif callback_data == 'booking_none':
            # Mark task as completed without booking
            await mark_task_completed(query, context)

        elif callback_data == 'booking_add':
            # Start conversation to add new booking date
            await query.edit_message_text(
                f"📅 Please enter the next visit date for {patient_name} (YYYY-MM-DD format):",
                reply_markup=None
            )
            # Set conversation state for booking date input
            context.user_data['conversation'] = 'booking_input'

        elif callback_data == 'booking_change':
            # Start conversation to change booking date
            await query.edit_message_text(
                f"📅 Please enter the new next visit date for {patient_name} (YYYY-MM-DD format):",
                reply_markup=None
            )
            # Set conversation state for booking date input
            context.user_data['conversation'] = 'booking_input'

        # SUNDAY HANDLERS FOR BOOKING
        elif callback_data.startswith('booking_monday|'):
            # Move booking to Monday
            parts = callback_data.split('|')
            monday_date = parts[1]
            context.user_data['booking_date'] = monday_date

            # Continue with booking creation
            name = context.user_data.get('name')
            phone = context.user_data.get('phone')
            booking_date = monday_date

            # Get the staff member's name who is creating this booking
            chat_id = query.message.chat_id
            booked_by = get_user_name(chat_id)

            try:
                # Upsert the booking
                booking = DatabaseManager.upsert_booking(name, phone, booking_date, booked_by)

                if booking:
                    is_update = 'existing_booking' in context.user_data

                    # If booking is for today, create a task for confirming availability
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    if booking_date == today_str and not is_update:
                        # Create "0-Day Reminder" task for Nimisha
                        DatabaseManager.create_patient_task(
                            assignee='Nimisha',
                            name=name,
                            phone=phone,
                            followup_type='0-Day Reminder',
                            status='Pending',
                            due_date=today_str
                        )

                    if is_update:
                        success_message = (
                            f"✅ *Booking Updated Successfully!*\n\n"
                            f"👤 Patient: {name}\n"
                            f"📱 Phone: {phone}\n"
                            f"📅 New Date: {format_date(booking_date)}\n\n"
                            f"📍 Use /start to return to main menu"
                        )
                    else:
                        success_message = (
                            f"✅ *Booking Created Successfully!*\n\n"
                            f"👤 Patient: {name}\n"
                            f"📱 Phone: {phone}\n"
                            f"📅 Booking Date: {format_date(booking_date)}\n"
                            f"👤 Booked by: {booked_by}\n\n"
                            f"📍 Use /start to return to main menu"
                        )

                    # Show appropriate menu based on user role
                    role = get_user_role(chat_id)
                    keyboard = get_admin_menu_keyboard() if role == 'admin' else get_staff_menu_keyboard()

                    await query.message.reply_text(success_message, parse_mode='Markdown', reply_markup=keyboard)
                else:
                    await query.message.reply_text(
                        "❌ Failed to create booking. Please try again.",
                        reply_markup=get_admin_menu_keyboard() if role == 'admin' else get_staff_menu_keyboard()
                    )

                context.user_data.clear()
            except Exception as e:
                print(f"Error in booking_monday callback: {e}")
                await query.message.reply_text(
                    "❌ An error occurred. Please try again.",
                    reply_markup=get_admin_menu_keyboard() if role == 'admin' else get_staff_menu_keyboard()
                )

        elif callback_data == 'booking_newdate':
            # Prompt user to enter a different date
            name = context.user_data.get('name')
            phone = context.user_data.get('phone')

            await query.message.reply_text(
                f"👤 Patient Name: {name}\n"
                f"📱 Phone: {phone}\n\n"
                f"📅 Please enter a different booking date (YYYY-MM-DD format):",
                reply_markup=get_today_keyboard()
            )
            # Stay in BOOKING_DATE state to handle new date input

    # SUNDAY HANDLERS FOR VISIT
    elif callback_data.startswith('visit_monday|'):
        # Move visit to Monday
        parts = callback_data.split('|')
        monday_date = parts[1]
        context.user_data['next_visit_date'] = monday_date

        # Continue with visit saving
        name = context.user_data.get('name')
        phone = context.user_data.get('phone')
        visit_date = context.user_data.get('visit_date')
        patient_id = context.user_data.get('patient_id')
        is_pregnancy = context.user_data.get('is_pregnancy')
        edc = context.user_data.get('edc')
        gravida = context.user_data.get('gravida')
        notes = context.user_data.get('notes')
        next_visit_date = monday_date

        # Get the current user's name for task assignment
        chat_id = query.message.chat_id
        current_user_name = get_user_name(chat_id)

        # Check if patient exists - if not, we'll create them
        existing_patient = DatabaseManager.fetch_patient(name, phone)
        is_new_patient = not existing_patient

        # 1. Patients: Create/update patient record with notes, patient_id, and last_visit_date
        # upsert_patient will create a new patient if they don't exist
        DatabaseManager.prepend_patient_notes(name, phone, notes, visit_date)
        DatabaseManager.upsert_patient(name, phone, patient_id=patient_id, last_visit=visit_date)

        # 2. Visits: Insert clinical record with gravida_status
        DatabaseManager.insert_visit(
            name, phone, visit_date, is_pregnancy,
            remarks=notes,
            next_visit_date=next_visit_date,
            gravida_status=gravida
        )

        # 2.5. Bookings: Mark current booking as visited
        DatabaseManager.mark_booking_visited(name, phone)

        # 3. Pregnancy Registry: UPSERT EDC date with gravida_status
        edc_display = "N/A"
        gravida_display = "N/A"
        if is_pregnancy and edc:
            DatabaseManager.upsert_pregnancy_registry(
                name, phone, edc, gravida_status=gravida, status='Active'
            )
            edc_display = format_date(edc)
            gravida_display = gravida

        # 4. Bookings: Update bookings table if next visit is provided
        if next_visit_date:
            DatabaseManager.upsert_booking(name, phone, next_visit_date)

        # 5. Tasks: Create '3-Day Feedback' task for the current user (dynamic assignee)
        three_day_due = DatabaseManager.calculate_due_date(visit_date, 3)
        DatabaseManager.create_patient_task(
            assignee=current_user_name,
            name=name,
            phone=phone,
            followup_type='3-Day Feedback',
            status='Pending',
            due_date=three_day_due
        )

        # Success Message
        success_message = (
            f"✅ *Visit Recorded Successfully!*\n\n"
            f"👤 Patient: {name}\n"
            f"📱 Phone: {phone}\n"
            f"🏥 Patient ID: {patient_id}\n"
            f"📅 Visit Date: {format_date(visit_date)}\n"
            f"🤰 Pregnancy: {'Yes' if is_pregnancy else 'No'}\n"
        )
        if is_pregnancy and edc:
            success_message += f"👶 Gravida: {gravida_display}\n"
            success_message += f"📅 EDC: {edc_display}\n"
        success_message += f"📝 Notes: {notes}\n"
        success_message += f"📅 Next Visit: {format_date(next_visit_date)}\n"
        if is_new_patient:
            success_message += f"\n🆕 *New patient added to the patient table*\n"
        success_message += f"\n📋 Task Created for {current_user_name} (3-Day Feedback)\n"
        success_message += f"📅 Task Due: {format_date(three_day_due)}\n"
        success_message += f"\n📍 Use /start to return to main menu"

        # Show appropriate menu based on user role
        role = get_user_role(chat_id)
        keyboard = get_admin_menu_keyboard() if role == 'admin' else get_staff_menu_keyboard()

        await query.message.reply_text(success_message, parse_mode='Markdown', reply_markup=keyboard)
        context.user_data.clear()

    elif callback_data == 'visit_newdate':
        # Prompt user to enter a different date for next visit
        await query.message.reply_text(
            "📅 Please enter a different next visit date (YYYY-MM-DD format) or type 'none':",
            reply_markup=ReplyKeyboardMarkup([['Today'], ['Cancel']], resize_keyboard=True, one_time_keyboard=True)
        )
        # Stay in NEXT_VISIT state to handle new date input

    # ========================================
    # ADMIN STATUS SYNC CALLBACKS
    # ========================================
    elif callback_data.startswith('status_') and role == 'admin':
        # Parse status callback data
        parts = callback_data.split('_')
        status_type = parts[1]  # delivered, unreachable, discontinued
        idx = parts[2]
        patient_name = '_'.join(parts[3:-1]).replace('_', ' ')  # Decode underscores back to spaces
        phone = parts[-1]

        # Map status type to display name
        status_names = {
            'delivered': '👶 Delivered',
            'unreachable': '🚫 Unreachable',
            'discontinued': '🛑 Discontinued'
        }

        # Confirm before updating
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f'confirm_{status_type}_{idx}_{patient_name}_{phone}'),
                InlineKeyboardButton("❌ Cancel", callback_data='status_cancel')
            ]
        ])

        confirm_message = (
            f"⚠️ *Confirm Status Change*\n\n"
            f"👤 Patient: {patient_name}\n"
            f"📱 Phone: {phone}\n"
            f"📊 New Status: {status_names.get(status_type, status_type)}\n\n"
            f"This will update:\n"
            f"• Latest visit status\n"
            f"• Pregnancy registry status\n"
            f"• All pending tasks for this patient\n\n"
            f"Confirm to proceed?"
        )
        await query.edit_message_text(confirm_message, reply_markup=keyboard, parse_mode='Markdown')

    elif callback_data.startswith('confirm_') and role == 'admin':
        # Confirm and execute status sync
        parts = callback_data.split('_')
        status_type = parts[1]  # delivered, unreachable, discontinued
        idx = parts[2]
        patient_name = '_'.join(parts[3:-1]).replace('_', ' ')  # Decode underscores back to spaces
        phone = parts[-1]

        # Execute global status sync
        result = DatabaseManager.sync_global_status(patient_name, phone, status_type)

        # Map status type to display name
        status_names = {
            'delivered': '👶 Delivered',
            'unreachable': '🚫 Unreachable',
            'discontinued': '🛑 Discontinued'
        }

        if result['errors']:
            error_message = (
                f"❌ *Status Update Failed*\n\n"
                f"👤 Patient: {patient_name}\n"
                f"📱 Phone: {phone}\n\n"
                f"Errors:\n"
            )
            for error in result['errors']:
                error_message += f"• {error}\n"
            await query.edit_message_text(error_message, parse_mode='Markdown')
        else:
            success_message = (
                f"✅ *Status Updated Successfully!*\n\n"
                f"👤 Patient: {patient_name}\n"
                f"📱 Phone: {phone}\n"
                f"📊 New Status: {status_names.get(status_type, status_type)}\n\n"
                f"Updated:\n"
                f"• Latest visit status: ✅\n"
                f"• Pregnancy registry status: ✅\n"
                f"• Tasks updated: {result['tasks_updated']} ✅\n\n"
                f"Use /start to return to the main menu."
            )
            await query.edit_message_text(success_message, parse_mode='Markdown')

    elif callback_data == 'status_cancel':
        # Cancel status update
        await query.edit_message_text(
            "❌ Status update cancelled.\n\n"
            "Use /start to return to the main menu."
        )

    # ========================================
    # ATTRITION DRILL-DOWN CALLBACKS
    # ========================================
    elif callback_data.startswith('attrition_list_') and role == 'admin':
        # Handle attrition list drill-down
        await attrition_list_handler(update, context)
    elif callback_data == 'attrition_cancel':
        # Cancel attrition drill-down
        await query.edit_message_text(
            "❌ Operation cancelled.\n\n"
            "Use /start to return to the main menu."
        )

    # ========================================
    # VIEW BOOKINGS DATE CALLBACKS (ADMIN ONLY)
    # ========================================
    elif callback_data.startswith('view_date_') or callback_data == 'view_date_custom':
        # Handle date selection for view bookings (Admin only)
        if role == 'admin':
            await view_bookings_date_callback(update, context)

# ========================================
# TASK CLEANUP CONVERSATION (CLEANUP_CONV)
# ========================================
@authorized_only
async def task_cleanup_year_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the Task Cleanup workflow - Step 1: Select year (Current/Previous)."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        if update.message:
            await update.message.reply_text("❌ This feature is only available to Admin.")
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("❌ This feature is only available to Admin.")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data['conversation'] = 'task_cleanup'

    # Get current and previous year
    current_year = datetime.now().year
    previous_year = current_year - 1

    # Create inline keyboard with year options
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"Current ({current_year})", callback_data=f'cleanup_year_{current_year}'),
            InlineKeyboardButton(f"Previous ({previous_year})", callback_data=f'cleanup_year_{previous_year}')
        ],
        [InlineKeyboardButton("Cancel", callback_data='cleanup_cancel')]
    ])

    message = (
        f"🧹 *Task Cleanup*\n\n"
        f"Select the year to cleanup tasks:\n\n"
        f"⚠️ This will delete tasks where status != 'Pending'"
    )

    if update.message:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')
    elif update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return CLEANUP_SELECT_YEAR


async def task_cleanup_month_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle year selection - Step 2: Select month."""
    query = update.callback_query
    await query.answer()

    if query.data == 'cleanup_cancel':
        return await cancel_conversation(update, context)

    # Extract year from callback data
    year = int(query.data.replace('cleanup_year_', ''))
    context.user_data['cleanup_year'] = year

    # Create inline keyboard with month options
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    keyboard_rows = []
    for i in range(0, len(month_names), 3):
        row = []
        for j in range(i, min(i + 3, len(month_names))):
            month_num = j + 1
            row.append(InlineKeyboardButton(month_names[j], callback_data=f'cleanup_month_{month_num}'))
        keyboard_rows.append(row)
    keyboard_rows.append([InlineKeyboardButton("Cancel", callback_data='cleanup_cancel')])

    keyboard = InlineKeyboardMarkup(keyboard_rows)

    message = (
        f"🧹 *Task Cleanup*\n\n"
        f"Selected Year: {year}\n\n"
        f"Select the month to cleanup:"
    )

    await query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')
    return CLEANUP_SELECT_MONTH


async def task_cleanup_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle month selection - Step 3: Show count and confirm deletion."""
    query = update.callback_query
    await query.answer()

    if query.data == 'cleanup_cancel':
        return await cancel_conversation(update, context)

    # Extract month from callback data
    month = int(query.data.replace('cleanup_month_', ''))
    year = context.user_data.get('cleanup_year')
    context.user_data['cleanup_month'] = month

    # Count tasks that will be deleted
    task_count = DatabaseManager.count_old_tasks(year, month)

    # Get month name
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    month_name = month_names[month - 1]

    # Create confirmation keyboard
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🗑️ Confirm Deletion", callback_data='cleanup_confirm_delete'),
            InlineKeyboardButton("❌ Cancel", callback_data='cleanup_cancel')
        ]
    ])

    if task_count > 0:
        message = (
            f"🧹 *Task Cleanup - Confirmation*\n\n"
            f"Period: {month_name} {year}\n"
            f"Tasks to delete: {task_count}\n\n"
            f"⚠️ These tasks have status != 'Pending'\n"
            f"This action cannot be undone.\n\n"
            f"Confirm deletion?"
        )
    else:
        message = (
            f"🧹 *Task Cleanup - No Tasks*\n\n"
            f"Period: {month_name} {year}\n"
            f"Tasks to delete: {task_count}\n\n"
            f"✅ No completed tasks found for this period.\n"
            f"Nothing to cleanup."
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("OK", callback_data='cleanup_cancel')]
        ])

    await query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')
    return CLEANUP_CONFIRM_DELETE


async def task_cleanup_delete_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle confirmation - Step 4: Delete tasks and show result."""
    query = update.callback_query
    await query.answer()

    if query.data == 'cleanup_cancel':
        return await cancel_conversation(update, context)

    # Extract year and month from context
    year = context.user_data.get('cleanup_year')
    month = context.user_data.get('cleanup_month')

    # Get month name
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    month_name = month_names[month - 1]

    try:
        # Perform the deletion
        deleted_count = DatabaseManager.delete_old_tasks(year, month)

        if deleted_count > 0:
            message = (
                f"🧹 *Task Cleanup - Complete*\n\n"
                f"Period: {month_name} {year}\n"
                f"✅ Deleted {deleted_count} task(s)\n\n"
                f"Use /start to return to the main menu."
            )
        else:
            message = (
                f"🧹 *Task Cleanup - No Changes*\n\n"
                f"Period: {month_name} {year}\n"
                f"No tasks were deleted.\n\n"
                f"Use /start to return to the main menu."
            )

        await query.edit_message_text(message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error in task cleanup deletion: {e}")
        message = (
            f"❌ *Task Cleanup - Error*\n\n"
            f"An error occurred: {e}\n\n"
            f"Please try again or contact support."
        )
        await query.edit_message_text(message, parse_mode='Markdown')

    context.user_data.clear()
    return ConversationHandler.END


# ========================================
# DIRECT TASK DELEGATION CONVERSATION (ASSIGN_TASK_CONV)
# ========================================
@authorized_only
async def assign_task_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the Direct Task Delegation workflow - Step 1: Choose staff."""
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        if update.message:
            await update.message.reply_text("❌ This feature is only available to Admin.")
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("❌ This feature is only available to Admin.")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data['conversation'] = 'assign_task'

    # Get active staff mapping
    staff_mapping = DatabaseManager.get_active_staff_mapping()

    if not staff_mapping:
        message = "❌ No active staff members found to assign tasks to."
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(message)
        return ConversationHandler.END

    # Create inline keyboard with staff names
    keyboard_rows = []
    staff_list = sorted(staff_mapping.keys())
    for i in range(0, len(staff_list), 2):
        row = []
        for j in range(i, min(i + 2, len(staff_list))):
            staff_name = staff_list[j]
            # Encode staff name to handle spaces in callback data
            staff_name_encoded = staff_name.replace(' ', '_')
            row.append(InlineKeyboardButton(staff_name, callback_data=f'assign_staff_{staff_name_encoded}'))
        keyboard_rows.append(row)
    keyboard_rows.append([InlineKeyboardButton("Cancel", callback_data='assign_cancel')])

    keyboard = InlineKeyboardMarkup(keyboard_rows)

    message = (
        f"📣 *Direct Task Delegation*\n\n"
        f"Select the staff member to assign the task to:"
    )

    if update.message:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')

    return ASSIGN_CHOOSE_STAFF


async def assign_task_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle staff selection - Step 2: Input task message."""
    query = update.callback_query
    await query.answer()

    if query.data == 'assign_cancel':
        return await cancel_conversation(update, context)

    # Extract staff name from callback data and decode
    staff_name = query.data.replace('assign_staff_', '').replace('_', ' ')
    context.user_data['assign_staff_name'] = staff_name

    message = (
        f"📣 *Direct Task Delegation*\n\n"
        f"Assigning to: {staff_name}\n\n"
        f"Please enter the task message:"
    )

    await query.edit_message_text(message, parse_mode='Markdown')
    return ASSIGN_INPUT_MESSAGE


async def assign_task_due_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle task message input - Step 3: Input due date."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    if not text:
        await update.message.reply_text(
            "❌ Task message cannot be empty. Please enter the task message:",
            reply_markup=get_cancel_keyboard()
        )
        return ASSIGN_INPUT_MESSAGE

    context.user_data['assign_task_message'] = text
    staff_name = context.user_data.get('assign_staff_name')

    # Show today's date as default option
    today = datetime.now().strftime('%Y-%m-%d')

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"Today ({today})", callback_data='assign_due_today'),
            InlineKeyboardButton("Enter custom date", callback_data='assign_due_custom')
        ],
        [InlineKeyboardButton("Cancel", callback_data='assign_cancel')]
    ])

    message = (
        f"📣 *Direct Task Delegation*\n\n"
        f"Assigning to: {staff_name}\n"
        f"Task: {text}\n\n"
        f"Select due date:"
    )

    await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')
    return ASSIGN_INPUT_DUE_DATE


async def assign_task_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle due date selection - Step 4: Confirm and send."""
    query = update.callback_query
    await query.answer()

    if query.data == 'assign_cancel':
        return await cancel_conversation(update, context)

    # Handle due date selection
    if query.data == 'assign_due_today':
        due_date = datetime.now().strftime('%Y-%m-%d')
        context.user_data['assign_due_date'] = due_date
    elif query.data == 'assign_due_custom':
        context.user_data['assign_custom_date'] = True
        await query.edit_message_text(
            "Please enter the due date (YYYY-MM-DD):",
            reply_markup=get_cancel_keyboard()
        )
        return ASSIGN_INPUT_DUE_DATE

    # Get task details
    staff_name = context.user_data.get('assign_staff_name')
    task_message = context.user_data.get('assign_task_message')
    due_date = context.user_data.get('assign_due_date', datetime.now().strftime('%Y-%m-%d'))

    # Create confirmation keyboard
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm & Send", callback_data='assign_confirm_send'),
            InlineKeyboardButton("❌ Cancel", callback_data='assign_cancel')
        ]
    ])

    message = (
        f"📣 *Direct Task Delegation - Confirm*\n\n"
        f"To: {staff_name}\n"
        f"Task: {task_message}\n"
        f"Due: {due_date}\n\n"
        f"⚠️ The task will be sent immediately via push notification."
    )

    await query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')
    return ASSIGN_CONFIRM_SEND


async def assign_task_custom_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle custom due date input."""
    text = update.message.text.strip()

    if text == 'Cancel':
        return await cancel_conversation(update, context)

    # Validate date format
    try:
        datetime.strptime(text, '%Y-%m-%d')
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid date format. Please use YYYY-MM-DD (e.g., 2024-03-27):",
            reply_markup=get_cancel_keyboard()
        )
        return ASSIGN_INPUT_DUE_DATE

    context.user_data['assign_due_date'] = text
    context.user_data.pop('assign_custom_date', None)

    # Proceed to confirmation
    staff_name = context.user_data.get('assign_staff_name')
    task_message = context.user_data.get('assign_task_message')

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm & Send", callback_data='assign_confirm_send'),
            InlineKeyboardButton("❌ Cancel", callback_data='assign_cancel')
        ]
    ])

    message = (
        f"📣 *Direct Task Delegation - Confirm*\n\n"
        f"To: {staff_name}\n"
        f"Task: {task_message}\n"
        f"Due: {text}\n\n"
        f"⚠️ The task will be sent immediately via push notification."
    )

    await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')
    return ASSIGN_CONFIRM_SEND


async def assign_task_send_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle confirmation - Send task with push notification."""
    query = update.callback_query
    await query.answer()

    if query.data == 'assign_cancel':
        return await cancel_conversation(update, context)

    # Get task details
    staff_name = context.user_data.get('assign_staff_name')
    task_message = context.user_data.get('assign_task_message')
    due_date = context.user_data.get('assign_due_date')
    admin_id = update.effective_user.id

    try:
        # Get staff telegram_id
        staff_mapping = DatabaseManager.get_active_staff_mapping()
        staff_id = staff_mapping.get(staff_name)

        if not staff_id:
            message = (
                f"❌ *Task Assignment Failed*\n\n"
                f"Could not find Telegram ID for {staff_name}.\n\n"
                f"Please try again."
            )
            await query.edit_message_text(message, parse_mode='Markdown')
            context.user_data.clear()
            return ConversationHandler.END

        # Save to database (insert into admin_tasks table)
        task_record = DatabaseManager.create_admin_task(
            task_message=task_message,
            assignee=staff_name,
            assignee_id=staff_id,
            assigned_by=admin_id,
            due_date=due_date
        )

        # Format message for staff
        staff_message = (
            f"📣 *New Admin Task Assigned*\n\n"
            f"👤 From: Admin\n"
            f"📋 Task: {task_message}\n"
            f"📅 Due: {due_date}\n\n"
            f"Use the '🧹 Admin Tasks' menu to view and complete this task."
        )

        # Send push notification to staff
        await context.bot.send_message(chat_id=staff_id, text=staff_message, parse_mode='Markdown')

        # Confirm to admin
        success_message = (
            f"✅ *Task Sent Successfully*\n\n"
            f"To: {staff_name}\n"
            f"Task: {task_message}\n"
            f"Due: {due_date}\n\n"
            f"📣 Push notification sent to staff member.\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(success_message, parse_mode='Markdown')

    except Exception as e:
        print(f"Error sending task: {e}")
        error_message = (
            f"❌ *Task Assignment Failed*\n\n"
            f"An error occurred: {e}\n\n"
            f"Please try again."
        )
        await query.edit_message_text(error_message, parse_mode='Markdown')

    context.user_data.clear()
    return ConversationHandler.END


# ========================================
# HELPER FUNCTION FOR TASK COMPLETION
# ========================================
async def mark_task_completed(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark the task as completed and show confirmation."""
    task_info = context.user_data.get('task_info', {})
    patient_name = task_info.get('patient_name')
    phone = task_info.get('phone')
    task_type = task_info.get('task_type')
    assignee = task_info.get('assignee')

    print(f"[DEBUG mark_task_completed] patient_name={patient_name}, phone={phone}, task_type={task_type}, assignee={assignee}")

    try:
        # Update task status to Completed
        result = DatabaseManager.update_patient_task_status(assignee, patient_name, phone, task_type, 'Completed')
        print(f"[DEBUG mark_task_completed] Update result: {result}")

        success_message = (
            f"✅ *Task Completed Successfully!*\n\n"
            f"👤 Patient: {patient_name}\n"
            f"📱 Phone: {phone}\n"
            f"📋 Task: {task_type}\n\n"
            f"Use /start to return to the main menu."
        )
        await query.edit_message_text(success_message, parse_mode='Markdown')

        # Clear task info
        context.user_data.pop('task_info', None)

    except Exception as e:
        print(f"Error marking task as completed: {e}")
        import traceback
        traceback.print_exc()
        await query.edit_message_text(
            f"❌ Error marking task as completed: {e}\n\n"
            f"Please try again or contact support."
        )

# ========================================
# BOOKING DATE INPUT HANDLER
# ========================================
@authorized_only
async def handle_booking_date_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle booking date input from staff."""
    text = update.message.text.strip()

    if text.lower() == 'cancel':
        context.user_data.clear()
        await update.message.reply_text(
            "❌ Operation cancelled.\n\n"
            "Use /start to return to the main menu."
        )
        return

    # Validate date format
    if not validate_date(text):
        await update.message.reply_text(
            "❌ Invalid date format. Please use YYYY-MM-DD format or type 'Cancel':"
        )
        return

    # Get task info
    task_info = context.user_data.get('task_info', {})
    patient_name = task_info.get('patient_name')
    phone = task_info.get('phone')

    try:
        # Update bookings table
        DatabaseManager.upsert_booking(patient_name, phone, text)

        # Mark task as completed
        await mark_task_completed_with_message(update, context, text)

    except Exception as e:
        print(f"Error updating booking: {e}")
        await update.message.reply_text(
            f"❌ Error updating booking: {e}\n\n"
            f"Please try again or contact support."
        )

async def mark_task_completed_with_message(update: Update, context: ContextTypes.DEFAULT_TYPE, booking_date: str) -> None:
    """Mark the task as completed with booking confirmation."""
    task_info = context.user_data.get('task_info', {})
    patient_name = task_info.get('patient_name')
    phone = task_info.get('phone')
    task_type = task_info.get('task_type')
    assignee = task_info.get('assignee')

    try:
        # Update task status to Completed
        DatabaseManager.update_patient_task_status(assignee, patient_name, phone, task_type, 'Completed')

        success_message = (
            f"✅ *Task Completed Successfully!*\n\n"
            f"👤 Patient: {patient_name}\n"
            f"📱 Phone: {phone}\n"
            f"📋 Task: {task_type}\n"
            f"📅 Next Booking: {format_date(booking_date)}\n\n"
            f"Use /start to return to the main menu."
        )
        await update.message.reply_text(success_message, parse_mode='Markdown')

        # Clear task info
        context.user_data.pop('task_info', None)
        context.user_data.pop('conversation', None)

    except Exception as e:
        print(f"Error marking task as completed: {e}")
        await update.message.reply_text(
            f"❌ Error marking task as completed: {e}\n\n"
            f"Please try again or contact support."
        )

# ========================================
# ERROR HANDLER
# ========================================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors."""
    print(f'Error: {context.error}')

# ========================================
# DAILY AUTOMATION ENGINE
# ========================================
async def daily_sync_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Daily sync job that runs at 8:00 AM IST.
    Syncs bookings to patient tasks for automated reminders.
    Sends summary to all registered Admin users.
    Optimized for memory efficiency with cache clearing.
    """
    # Using UTC for consistent date calculation across different environments
    today_dt = datetime.now(timezone.utc).astimezone()
    today = today_dt.strftime('%Y-%m-%d')
    print(f"[{today}] Running daily sync automation... (System timezone: {today_dt.tzinfo})")

    # Clear stale caches to free memory before heavy operations
    DatabaseManager.clear_all_caches()

    # Call 1: Sync today's bookings (0 days)
    print(f"[{today}] Starting Today's Reminders sync...")
    result_1 = DatabaseManager.sync_bookings_to_tasks(0, "0-Day Reminder")
    print(f"[{today}] Today's Reminders - Created: {result_1['tasks_created']}, Skipped: {result_1['tasks_skipped']}")
    if result_1['errors']:
        print(f"[{today}] Errors: {result_1['errors']}")

    # Call 2: Sync bookings 3 days from now
    print(f"[{today}] Starting 3-Day Reminders sync...")
    result_2 = DatabaseManager.sync_bookings_to_tasks(3, "3-Day Reminder")
    print(f"[{today}] 3-Day Reminders - Created: {result_2['tasks_created']}, Skipped: {result_2['tasks_skipped']}")
    if result_2['errors']:
        print(f"[{today}] Errors: {result_2['errors']}")

    # Call 3: Sync bookings 14 days from now
    print(f"[{today}] Starting 14-Day Reminders sync...")
    result_3 = DatabaseManager.sync_bookings_to_tasks(14, "14-Day Reminder")
    print(f"[{today}] 14-Day Reminders - Created: {result_3['tasks_created']}, Skipped: {result_3['tasks_skipped']}")
    if result_3['errors']:
        print(f"[{today}] Errors: {result_3['errors']}")

    # Call 4: Sync missed bookings to no-show followup tasks
    print(f"[{today}] Starting No Visit sync...")
    result_4 = DatabaseManager.sync_missed_bookings_to_tasks()
    print(f"[{today}] No Visit - Created: {result_4['tasks_created']}, Skipped: {result_4['tasks_skipped']}")
    if result_4['errors']:
        print(f"[{today}] Errors: {result_4['errors']}")

    # Call 5: Create daily admin chores for staff
    print(f"[{today}] Starting Daily Admin Chores sync...")
    result_5 = DatabaseManager.create_daily_admin_chores()
    print(f"[{today}] Daily Admin Chores - Created: {result_5['tasks_created']}, Skipped: {result_5['tasks_skipped']}")
    if result_5['errors']:
        print(f"[{today}] Errors: {result_5['errors']}")

    total_created = result_1['tasks_created'] + result_2['tasks_created'] + result_3['tasks_created'] + result_4['tasks_created'] + result_5['tasks_created']

    # Force garbage collection after heavy operations
    gc.collect()

    # Create summary message
    summary_message = (
        f"📊 *Daily Automation Summary ({today})*\n"
        f"✅ Today's Reminders: {result_1['tasks_created']}\n"
        f"✅ 3-Day Reminders: {result_2['tasks_created']}\n"
        f"✅ 14-Day Reminders: {result_3['tasks_created']}\n"
        f"✅ No Visit: {result_4['tasks_created']}\n"
        f"✅ Daily Admin Chores: {result_5['tasks_created']}\n"
        f"Total tasks created: {total_created}"
    )

    # Send summary to all registered Admin users
    admins = DatabaseManager.get_staff_by_role('admin')
    print(f"DEBUG: Starting automation for {len(admins)} admins")

    for admin in admins:
        telegram_id = admin.get('telegram_id')
        if telegram_id:
            try:
                await context.bot.send_message(
                    chat_id=telegram_id,
                    text=summary_message,
                    parse_mode='Markdown'
                )
                print(f"DEBUG: Summary sent to admin {admin.get('staff_name', 'Unknown')} (ID: {telegram_id})")
            except Exception as e:
                print(f"DEBUG: Failed to send summary to admin {admin.get('staff_name', 'Unknown')} (ID: {telegram_id}): {e}")

    print(f"[{today}] Daily sync completed. Total tasks created: {total_created}")


# ========================================
# PERIODIC MEMORY CLEANUP JOB
# ========================================
async def memory_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Periodic memory cleanup job that runs every 2 hours.
    Clears database caches and forces garbage collection.
    Optimized for 512 MB RAM deployment.
    """
    try:
        # Clear all database caches
        DatabaseManager.clear_all_caches()

        # Force Python garbage collection
        gc.collect()

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memory cleanup completed")
    except Exception as e:
        print(f"Error in memory cleanup job: {e}")


# ========================================
# LIGHTWEIGHT HTTP HEARTBEAT SERVER (RENDER FREE TIER)
# ========================================
class HealthRequestHandler(BaseHTTPRequestHandler):
    """Lightweight HTTP request handler for Render Free Tier heartbeat endpoint."""

    def log_message(self, format, *args):
        """Suppress default logging to avoid clutter."""
        pass

    def do_GET(self):
        """Handle GET requests - only respond to /heartbeat endpoint."""
        if self.path == '/heartbeat':
            # Send 200 OK response with "ALIVE" text
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'ALIVE')
            # Log heartbeat receipt
            print(f"[{datetime.now()}] Heartbeat received.")
        else:
            # Return 404 for all other paths
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')


def run_health_server():
    """Start the lightweight HTTP heartbeat server in the current thread."""
    # Get port from environment variable, default to 10000 for Render
    port = int(os.getenv('PORT', 10000))

    # Allow address reuse to prevent port errors during rapid redeployments
    socketserver.TCPServer.allow_reuse_address = True

    # Create and start the server
    try:
        with HTTPServer(('', port), HealthRequestHandler) as httpd:
            print(f"[{datetime.now()}] Heartbeat server started on port {port}")
            httpd.serve_forever()
    except Exception as e:
        print(f"[{datetime.now()}] Error starting heartbeat server: {e}")


# ========================================
# MAIN APPLICATION
# ========================================
def main():
    """Run the bot."""
    # ========================================
    # START HEARTBEAT SERVER (RENDER FREE TIER)
    # ========================================
    # Start the lightweight HTTP heartbeat server in a daemon thread.
    # This ensures the server runs in the background without blocking the bot.
    # External cronjobs (e.g., Cron-job.org) can ping /heartbeat every 5 minutes
    # to prevent Render from spinning down the service after 15 minutes of inactivity.
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # Create application with JobQueue
    application = Application.builder().token(BOT_TOKEN).build()

    # ========================================
    # SET UP DAILY AUTOMATION JOB
    # ========================================
    # IST timezone is UTC+5:30, so 8:00 AM IST = 2:30 AM UTC
    # Add daily job to run at 8:00 AM IST
    # This syncs bookings to tasks for automation
    application.job_queue.run_daily(
        daily_sync_job,
        time=time(hour=2, minute=30),
        days=(0, 1, 2, 3, 4, 5, 6)
    )
    print("Daily automation job scheduled for 8:00 AM IST (2:30 AM UTC)")

    # ========================================
    # SET UP PERIODIC MEMORY CLEANUP JOB
    # ========================================
    # Add job to run every 2 hours for memory cleanup
    application.job_queue.run_repeating(
        memory_cleanup_job,
        interval=timedelta(hours=2),
        first=time(hour=6, minute=0)
    )
    print("Memory cleanup job scheduled every 2 hours")

    # Create the /visit conversation handler with 11 states
    visit_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('visit', visit_start),
            MessageHandler(filters.Regex('📝 Add Visit'), visit_start)
        ],
        states={
            VISIT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, visit_date_handler)],
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, name_handler)],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, phone_handler)],
            PATIENT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, patient_id_handler)],
            IS_PREGNANCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, is_pregnancy_handler)],
            GRAVIDA: [MessageHandler(filters.TEXT & ~filters.COMMAND, gravida_handler)],
            EDC_CHECK: [CallbackQueryHandler(edc_check_handler)],
            EDC_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, edc_input_handler)],
            EDC_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, edc_confirm_handler)],
            NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, notes_handler)],
            NEXT_VISIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, next_visit_handler)],
            NEXT_VISIT_SUNDAY_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, next_visit_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=30),
        per_message=False,
    )

    # Create the EDC Annual View conversation handler
    edc_view_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('edc_view', edc_view_start)],
        states={
            EDC_VIEW_SELECT_YEAR: [CallbackQueryHandler(edc_view_year_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=10),
        per_message=False,
    )

    # Create the Trends Analytics conversation handler
    trends_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex('📊 Trends'), trends_start)
        ],
        states={
            TRENDS_SELECT_YEAR: [
                CallbackQueryHandler(trends_delivery_handler, pattern='^trends_delivery$'),
                CallbackQueryHandler(trends_attrition_handler, pattern='^trends_attrition$'),
                CallbackQueryHandler(trends_visit_handler, pattern='^trends_visit$'),
                CallbackQueryHandler(trends_year_handler, pattern='^trends_year_')
            ],
            ATTRITION_TREND_SELECT_YEAR: [
                CallbackQueryHandler(attrition_year_handler, pattern='^attrition_year_')
            ],
            VISIT_TREND_SELECT_YEAR: [
                CallbackQueryHandler(visit_trend_year_handler, pattern='^visit_year_')
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=10),
        per_message=False,
    )

    # Create the Task Cleanup conversation handler
    task_cleanup_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('cleanup_tasks', task_cleanup_year_selection)],
        states={
            CLEANUP_SELECT_YEAR: [CallbackQueryHandler(task_cleanup_month_handler)],
            CLEANUP_SELECT_MONTH: [CallbackQueryHandler(task_cleanup_confirm_handler)],
            CLEANUP_CONFIRM_DELETE: [CallbackQueryHandler(task_cleanup_delete_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=10),
        per_message=False,
    )

    # Create the Direct Task Delegation conversation handler
    assign_task_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('assign_task', assign_task_start),
            MessageHandler(filters.Regex('📣 Assign Task'), assign_task_start)
        ],
        states={
            ASSIGN_CHOOSE_STAFF: [CallbackQueryHandler(assign_task_message_handler)],
            ASSIGN_INPUT_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, assign_task_due_date_handler)],
            ASSIGN_INPUT_DUE_DATE: [
                CallbackQueryHandler(assign_task_confirm_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, assign_task_custom_date_handler)
            ],
            ASSIGN_CONFIRM_SEND: [CallbackQueryHandler(assign_task_send_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=15),
        per_message=False,
    )

    # Create the Manual Booking conversation handler
    manual_booking_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('📅 Add Booking'), manual_booking_start)],
        states={
            BOOKING_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, booking_name_handler)],
            BOOKING_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, booking_phone_handler)],
            BOOKING_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, booking_date_handler)],
            BOOKING_CONFIRM_CHANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, booking_confirm_change_handler)],
            BOOKING_SUNDAY_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, booking_date_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=15),
        per_message=False,
    )

    # Create the Search conversation handler
    search_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('search', search_patients),
            MessageHandler(filters.Regex('🔍 Search Patient'), search_patients)
        ],
        states={
            SEARCH_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_name_handler)],
            SEARCH_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_phone_handler)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        conversation_timeout=timedelta(minutes=10),
        per_message=False,
    )

    # Register handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('forcesync', force_sync))
    application.add_handler(CommandHandler('pregnancy_registry', show_pregnancy_registry))
    application.add_handler(visit_conv_handler)
    application.add_handler(edc_view_conv_handler)
    application.add_handler(trends_conv_handler)
    application.add_handler(task_cleanup_conv_handler)
    application.add_handler(assign_task_conv_handler)
    application.add_handler(manual_booking_conv_handler)
    application.add_handler(search_conv_handler)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_buttons))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_error_handler(error_handler)

    # Start the bot
    print("KadeejaClinic Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()