"""Database module for KadeejaClinic bot using Supabase.
Optimized for 512 MB RAM deployment with pagination and selective column loading.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from supabase import create_client, Client
from functools import lru_cache


# Initialize Supabase client with timeout configuration
supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

# Pagination settings for memory efficiency
DEFAULT_PAGE_SIZE = 100
MAX_RESULTS_LIMIT = 1000


# ========================================
# PRIMARY KEY MAPPING FOR ROW EDITOR
# ========================================
PK_MAPPING = {
    'patients': ['patient_name', 'phone_number'],
    'visits': ['patient_name', 'phone_number', 'visit_date'],
    'pregnancy_registry': ['patient_name', 'phone_number', 'gravida_status'],
    'bookings': ['patient_name', 'phone_number'],
    'staff_config': ['telegram_id'],
    'admin_tasks': ['id'],  # UUID primary key
    'patient_tasks': ['assignee', 'patient_name', 'phone_number', 'followup_type'],  # Composite PK
}


class DatabaseManager:
    """Manages all database operations for the clinic bot."""

    # ========================================
    # STAFF CONFIG TABLE OPERATIONS
    # ========================================
    @staticmethod
    @lru_cache(maxsize=64)
    def get_staff_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
        """Fetch staff member by Telegram ID (cached)."""
        try:
            result = supabase.table('staff_config').select('telegram_id', 'staff_name', 'role', 'is_active')\
                .eq('telegram_id', telegram_id)\
                .eq('is_active', True)\
                .execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            print(f"Database error in get_staff_by_telegram_id: {e}")
        return None

    @staticmethod
    @lru_cache(maxsize=64)
    def get_staff_by_name(staff_name: str) -> Optional[Dict[str, Any]]:
        """Fetch staff member by name (cached)."""
        try:
            result = supabase.table('staff_config').select('telegram_id', 'staff_name', 'role', 'is_active')\
                .ilike('staff_name', staff_name)\
                .eq('is_active', True)\
                .execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            print(f"Database error in get_staff_by_name: {e}")
        return None

    @staticmethod
    @lru_cache(maxsize=16)
    def get_all_active_staff() -> List[Dict[str, Any]]:
        """Fetch all active staff members (cached for 5 minutes via clear_all_caches)."""
        try:
            result = supabase.table('staff_config').select('telegram_id', 'staff_name', 'role', 'is_active')\
                .eq('is_active', True)\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in get_all_active_staff: {e}")
            return []

    @staticmethod
    @lru_cache(maxsize=16)
    def get_active_staff_mapping() -> Dict[str, int]:
        """
        Get a mapping of staff name to telegram_id for active staff members.
        Excludes admin from the mapping since tasks are assigned TO staff.
        Cached to avoid repeated queries.

        Returns:
            Dictionary mapping staff_name to telegram_id
        """
        try:
            result = supabase.table('staff_config').select('staff_name', 'telegram_id', 'role', 'is_active')\
                .eq('is_active', True)\
                .neq('role', 'admin')\
                .execute()
            staff_list = result.data if result.data else []
            return {staff['staff_name']: staff['telegram_id'] for staff in staff_list}
        except Exception as e:
            print(f"Database error in get_active_staff_mapping: {e}")
            return {}

    @staticmethod
    def clear_all_caches() -> None:
        """Clear all LRU caches. Call this periodically to free memory."""
        DatabaseManager.get_staff_by_telegram_id.cache_clear()
        DatabaseManager.get_staff_by_name.cache_clear()
        DatabaseManager.get_all_active_staff.cache_clear()
        DatabaseManager.get_active_staff_mapping.cache_clear()

    @staticmethod
    def get_staff_by_role(role: str) -> List[Dict[str, Any]]:
        """Fetch staff members by role (admin or staff)."""
        try:
            result = supabase.table('staff_config').select('*').eq('role', role).eq('is_active', True).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in get_staff_by_role: {e}")
            return []

    @staticmethod
    def get_staff_name(telegram_id: int) -> str:
        """Get staff name by Telegram ID."""
        staff = DatabaseManager.get_staff_by_telegram_id(telegram_id)
        return staff.get('staff_name', 'Unknown') if staff else 'Unknown'

    @staticmethod
    def get_staff_role(telegram_id: int) -> str:
        """Get staff role by Telegram ID."""
        staff = DatabaseManager.get_staff_by_telegram_id(telegram_id)
        return staff.get('role', 'unknown') if staff else 'unknown'

    @staticmethod
    def is_admin(telegram_id: int) -> bool:
        """Check if user is an admin."""
        staff = DatabaseManager.get_staff_by_telegram_id(telegram_id)
        return staff.get('role') == 'admin' if staff else False

    @staticmethod
    def is_staff(telegram_id: int) -> bool:
        """Check if user is a staff member."""
        staff = DatabaseManager.get_staff_by_telegram_id(telegram_id)
        return staff is not None and staff.get('is_active', False)

    # ========================================
    # PATIENTS TABLE OPERATIONS
    # ========================================
    @staticmethod
    def fetch_patient(name: str, phone: str) -> Optional[Dict[str, Any]]:
        """Fetch a patient by name and phone number (selective columns)."""
        try:
            result = supabase.table('patients').select('patient_name', 'phone_number', 'patient_id', 'last_visit_date', 'notes')\
                .ilike('patient_name', name)\
                .ilike('phone_number', phone)\
                .execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            print(f"Database error in fetch_patient: {e}")
        return None

    @staticmethod
    def search_patients(search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search patients by name or phone number with result limit."""
        try:
            result = supabase.table('patients').select('patient_name', 'phone_number', 'patient_id', 'last_visit_date')\
                .or_(f"patient_name.ilike.%{search_term}%,phone_number.ilike.%{search_term}%")\
                .limit(limit)\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in search_patients: {e}")
            return []

    @staticmethod
    def search_patients_by_name(name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search patients by name (exact match or ILIKE)."""
        try:
            result = supabase.table('patients').select('patient_name', 'phone_number', 'patient_id', 'last_visit_date')\
                .ilike('patient_name', f'%{name}%')\
                .limit(limit)\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in search_patients_by_name: {e}")
            return []

    @staticmethod
    def search_patients_by_phone(phone: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search patients by phone number (exact match or ILIKE)."""
        try:
            result = supabase.table('patients').select('patient_name', 'phone_number', 'patient_id', 'last_visit_date')\
                .ilike('phone_number', f'%{phone}%')\
                .limit(limit)\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in search_patients_by_phone: {e}")
            return []

    @staticmethod
    def search_patients_by_name_and_phone(name: str, phone: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search patients by both name and phone number (optimized query)."""
        try:
            result = supabase.table('patients').select('patient_name', 'phone_number', 'patient_id', 'last_visit_date')\
                .ilike('patient_name', f'%{name}%')\
                .ilike('phone_number', f'%{phone}%')\
                .limit(limit)\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in search_patients_by_name_and_phone: {e}")
            return []

    @staticmethod
    def upsert_patient(name: str, phone: str, patient_id: Optional[str] = None,
                       last_visit: Optional[str] = None, notes: Optional[str] = None) -> Dict[str, Any]:
        """Insert or update a patient record."""
        data = {
            'patient_name': name,
            'phone_number': phone,
        }
        if patient_id is not None:
            data['patient_id'] = patient_id
        if last_visit is not None:
            data['last_visit_date'] = last_visit
        if notes is not None:
            data['notes'] = notes

        try:
            result = supabase.table('patients').upsert(data).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in upsert_patient: {e}")
            return {}

    @staticmethod
    def prepend_patient_notes(name: str, phone: str, new_notes: str, visit_date: str) -> Dict[str, Any]:
        """
        Prepend notes to an existing patient's record.
        History is maintained by prepending new notes at the top.
        """
        patient = DatabaseManager.fetch_patient(name, phone)
        current_notes = patient.get('notes', '') if patient else ''
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        new_entry = f"[{visit_date}]: {new_notes}\n"
        updated_notes = new_entry + current_notes if current_notes else new_entry.rstrip()
        return DatabaseManager.upsert_patient(name, phone, notes=updated_notes)

    @staticmethod
    def get_patient_visit_history(name: str, phone: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get recent visit history for a patient."""
        try:
            result = supabase.table('visits').select('*').ilike('patient_name', name).ilike('phone_number', phone)\
                .order('visit_date', desc=True).limit(limit).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in get_patient_visit_history: {e}")
            return []

    # ========================================
    # VISITS TABLE OPERATIONS
    # ========================================
    @staticmethod
    def insert_visit(name: str, phone: str, visit_date: str, is_pregnancy: bool,
                     next_visit_date: Optional[str] = None, followup_status: str = 'Pending',
                     remarks: Optional[str] = None, gravida_status: Optional[str] = None) -> Dict[str, Any]:
        """
        Insert a new visit record.
        Includes gravida_status in the data dictionary.
        """
        data = {
            'patient_name': name,
            'phone_number': phone,
            'visit_date': visit_date,
            'is_pregnancy': is_pregnancy,
            'followup_status': followup_status,
        }
        if next_visit_date is not None:
            data['next_visit_planned_date'] = next_visit_date
        if remarks is not None:
            data['remarks'] = remarks
        if gravida_status is not None:
            data['gravida_status'] = gravida_status

        try:
            result = supabase.table('visits').insert(data).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in insert_visit: {e}")
            return {}

    # ========================================
    # PREGNANCY REGISTRY OPERATIONS
    # ========================================
    @staticmethod
    def fetch_pregnancy_registry(name: str, phone: str, gravida: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch pregnancy registry entry for a patient.

        Args:
            name: Patient name
            phone: Patient phone number
            gravida: Optional Gravida status for precise composite key matching

        Returns:
            Single matching row as dict, or None if not found
        """
        try:
            query = supabase.table('pregnancy_registry').select('patient_name', 'phone_number', 'gravida_status', 'edc_date', 'status')
            query = query.ilike('patient_name', name).ilike('phone_number', phone)
            if gravida:
                query = query.eq('gravida_status', gravida)
            result = query.execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            print(f"Database error in fetch_pregnancy_registry: {e}")
        return None

    @staticmethod
    def fetch_all_pregnancies() -> List[Dict[str, Any]]:
        """Fetch all active pregnancy registry entries (optimized)."""
        try:
            result = supabase.table('pregnancy_registry')\
                .select('patient_name', 'phone_number', 'gravida_status', 'edc_date', 'status')\
                .eq('status', 'Active')\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in fetch_all_pregnancies: {e}")
            return []

    @staticmethod
    def get_edcs_for_year(year: int) -> List[Dict[str, Any]]:
        """
        Fetch all EDCs for a specific year from the pregnancy registry.

        Args:
            year: The year to fetch EDCs for (e.g., 2026)

        Returns:
            List of dictionaries with patient_name and edc_date for the specified year
        """
        try:
            # Query pregnancy_registry where edc_date starts with the year
            # Using ilike for pattern matching (e.g., '2026-%' matches all dates in 2026)
            year_pattern = f"{year}-"
            result = supabase.table('pregnancy_registry').select('patient_name', 'edc_date')\
                .ilike('edc_date', f'{year_pattern}%')\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in get_edcs_for_year: {e}")
            return []

    @staticmethod
    def get_attrition_counts_comparative(current_year: int) -> Dict[str, Any]:
        """
        Fetch attrition counts for comparative graph (prev year vs current year).

        Attrition is defined as pregnancy registry entries where status is NOT 'Active'.
        This includes: Delivered, Dropped, Unreachable, etc.
        Optimized to fetch only active-status records.

        Args:
            current_year: The current year to compare

        Returns:
            Dictionary with prev_year, curr_year and monthly counts:
            {
                'prev_year': int,
                'curr_year': int,
                'prev_monthly_counts': List[int] (12 months),
                'curr_monthly_counts': List[int] (12 months)
            }
        """
        result = {
            'prev_year': current_year - 1,
            'curr_year': current_year,
            'prev_monthly_counts': [0] * 12,
            'curr_monthly_counts': [0] * 12,
            'errors': []
        }

        try:
            prev_year = current_year - 1
            curr_year = current_year

            # Fetch registry entries for previous year with non-active status
            prev_pattern = f"{prev_year}-"
            prev_result = supabase.table('pregnancy_registry')\
                .select('edc_date')\
                .ilike('edc_date', f'{prev_pattern}%')\
                .not_.eq('status', 'Active')\
                .execute()

            prev_data = prev_result.data if prev_result.data else []

            # Count attrition by month (status NOT 'Active')
            for entry in prev_data:
                try:
                    edc_date = entry.get('edc_date')
                    if edc_date:
                        edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
                        month_idx = edc_dt.month - 1  # 0-11 for Jan-Dec
                        result['prev_monthly_counts'][month_idx] += 1
                except (ValueError, TypeError):
                    result['errors'].append(f"Invalid EDC date: {edc_date}")

            # Fetch registry entries for current year with non-active status
            curr_pattern = f"{curr_year}-"
            curr_result = supabase.table('pregnancy_registry')\
                .select('edc_date')\
                .ilike('edc_date', f'{curr_pattern}%')\
                .not_.eq('status', 'Active')\
                .execute()

            curr_data = curr_result.data if curr_result.data else []

            # Count attrition by month (status NOT 'Active')
            for entry in curr_data:
                try:
                    edc_date = entry.get('edc_date')
                    if edc_date:
                        edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
                        month_idx = edc_dt.month - 1  # 0-11 for Jan-Dec
                        result['curr_monthly_counts'][month_idx] += 1
                except (ValueError, TypeError):
                    result['errors'].append(f"Invalid EDC date: {edc_date}")

        except Exception as e:
            error_msg = f"Error in get_attrition_counts_comparative: {e}"
            print(error_msg)
            result['errors'].append(error_msg)

        return result

    @staticmethod
    def get_attrition_details_by_year(year: int) -> Dict[int, List[Dict[str, Any]]]:
        """
        Fetch detailed attrition data organized by month for a specific year.

        Args:
            year: The year to fetch attrition details for

        Returns:
            Dictionary with month keys (1-12) and list of patient details:
            {
                1: [
                    {
                        'patient_name': str,
                        'phone_number': str,
                        'edc_date': str,
                        'status': str,
                        'gravida_status': str
                    },
                    ...
                ],
                2: [...],
                ...
            }
        """
        result = {i: [] for i in range(1, 13)}  # Months 1-12

        try:
            # Fetch all registry entries for the year
            year_pattern = f"{year}-"
            all_result = supabase.table('pregnancy_registry')\
                .select('patient_name', 'phone_number', 'edc_date', 'status', 'gravida_status')\
                .ilike('edc_date', f'{year_pattern}%')\
                .execute()

            all_data = all_result.data if all_result.data else []

            # Organize by month (only non-Active status)
            for entry in all_data:
                try:
                    edc_date = entry.get('edc_date')
                    status = entry.get('status', 'Active')

                    if status != 'Active':
                        edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
                        month = edc_dt.month

                        result[month].append({
                            'patient_name': entry.get('patient_name', 'N/A'),
                            'phone_number': entry.get('phone_number', 'N/A'),
                            'edc_date': edc_date,
                            'status': status,
                            'gravida_status': entry.get('gravida_status', 'N/A')
                        })
                except (ValueError, TypeError):
                    continue

        except Exception as e:
            print(f"Database error in get_attrition_details_by_year: {e}")

        return result

    @staticmethod
    def get_new_pregnancy_counts() -> List[Dict[str, Any]]:
        """
        Fetch new pregnancy registrations by year and month from pregnancy_registry table.
        Uses created_at timestamp to determine when each pregnancy was registered.
        Optimized to limit to last 5 years of data.

        Returns:
            List of dictionaries with year, month, and count sorted by year, month
        """
        result = []

        try:
            # Limit to last 5 years to reduce memory usage
            five_years_ago = (datetime.now() - timedelta(days=365 * 5)).strftime('%Y-%m-%d')

            # Get pregnancy data with created_at from last 5 years
            pregnancy_result = supabase.table('pregnancy_registry')\
                .select('created_at')\
                .not_.is_('created_at', 'null')\
                .gte('created_at', five_years_ago)\
                .execute()

            pregnancy_data = pregnancy_result.data if pregnancy_result.data else []

            # Group by year and month
            pregnancy_counts = {}  # Key: (year, month), Value: count

            for pregnancy in pregnancy_data:
                created_at = pregnancy.get('created_at')
                if not created_at:
                    continue

                try:
                    # Parse ISO format timestamp (e.g., "2024-03-15T10:30:00+00:00")
                    if isinstance(created_at, str):
                        # Remove timezone info for simple parsing
                        created_at = created_at.split('T')[0]
                        preg_dt = datetime.strptime(created_at, '%Y-%m-%d')
                    else:
                        # If it's already a datetime object
                        preg_dt = created_at

                    year = preg_dt.year
                    month = preg_dt.month

                    key = (year, month)
                    pregnancy_counts[key] = pregnancy_counts.get(key, 0) + 1
                except (ValueError, TypeError, AttributeError):
                    continue

            # Convert to sorted list
            sorted_counts = []
            for (year, month), count in sorted(pregnancy_counts.items()):
                sorted_counts.append({
                    'year': year,
                    'month': month,
                    'count': count
                })

            return sorted_counts

        except Exception as e:
            print(f"Database error in get_new_pregnancy_counts: {e}")
            return []

    @staticmethod
    def get_monthly_delivery_trends(year: int) -> Dict[int, int]:
        """
        Fetch monthly delivery trends for a given year from pregnancy_registry table.
        Fetches ALL records with EDC in the selected year regardless of status.
        Uses range filtering (gte/lte) to avoid PostgreSQL type mismatch with DATE type.
        Month-wise counting is performed in Python to minimize database load.

        Args:
            year: The year to fetch delivery trends for (e.g., 2026)

        Returns:
            Dictionary with month (1-12) as key and count of expected deliveries as value
        """
        try:
            # Use range filtering instead of .like() to avoid PostgreSQL type mismatch
            # edc_date is a DATE type, and .like() doesn't work with it
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            # Query all EDCs in the specified year range (regardless of status)
            result = supabase.table('pregnancy_registry')\
                .select('edc_date')\
                .gte('edc_date', start_date)\
                .lte('edc_date', end_date)\
                .execute()

            data = result.data if result.data else []

            # Perform month-wise counting in Python
            month_counts = {}
            for entry in data:
                edc_date = entry.get('edc_date')
                if not edc_date:
                    continue

                try:
                    edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
                    if edc_dt.year == year:
                        month = edc_dt.month
                        month_counts[month] = month_counts.get(month, 0) + 1
                except (ValueError, TypeError):
                    continue

            # Ensure all months 1-12 are present in the result
            for month in range(1, 13):
                if month not in month_counts:
                    month_counts[month] = 0

            return month_counts

        except Exception as e:
            print(f"Database error in get_monthly_delivery_trends: {e}")
            # Return empty dictionary for all months on error
            return {month: 0 for month in range(1, 13)}

    @staticmethod
    def get_monthly_attrition_trends(year: int) -> List[int]:
        """
        Fetch monthly attrition trends for a given year from pregnancy_registry table.
        Optimized: Selects only edc_date column where status is in ['Unreachable', 'Dropped'].
        Filter: edc_date starts with the selected year (e.g., '2026-%').
        Month-wise counting is performed in Python to minimize database load.

        Args:
            year: The year to fetch attrition trends for (e.g., 2026)

        Returns:
            List of 12 integers representing counts for Jan-Dec (index 0 = January)
        """
        try:
            # Build year string for filtering
            year_pattern = f'{year}-%'

            # Optimized query: Select only edc_date column for attrition cases in the specified year
            result = supabase.table('pregnancy_registry')\
                .select('edc_date')\
                .in_('status', ['Unreachable', 'Dropped'])\
                .ilike('edc_date', year_pattern)\
                .execute()

            data = result.data if result.data else []

            # Perform month-wise counting in Python
            month_counts = [0] * 12  # Initialize list with 12 zeros for Jan-Dec

            for entry in data:
                edc_date = entry.get('edc_date')
                if not edc_date:
                    continue

                try:
                    edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
                    if edc_dt.year == year:
                        month = edc_dt.month
                        month_counts[month - 1] += 1  # Convert to 0-indexed
                except (ValueError, TypeError):
                    continue

            return month_counts

        except Exception as e:
            print(f"Database error in get_monthly_attrition_trends: {e}")
            # Return list of 12 zeros on error
            return [0] * 12

    @staticmethod
    def get_monthly_visit_trends(year: int) -> List[int]:
        """
        Fetch monthly visit trends for a given year from visits table.
        Optimized: Selects only visit_date column where visit_date is within the year range.
        Uses range filtering (gte/lte) to avoid PostgreSQL type mismatch with DATE type.
        Month-wise counting is performed in Python to minimize database load.

        Args:
            year: The year to fetch visit trends for (e.g., 2026)

        Returns:
            List of 12 integers representing counts for Jan-Dec (index 0 = January)
        """
        try:
            # Use range filtering instead of .ilike() to avoid PostgreSQL type mismatch
            # visit_date is a DATE type, and .ilike() doesn't work with it
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            # Optimized query: Select only visit_date column for the specified year range
            result = supabase.table('visits')\
                .select('visit_date')\
                .gte('visit_date', start_date)\
                .lte('visit_date', end_date)\
                .execute()

            data = result.data if result.data else []

            # Perform month-wise counting in Python
            month_counts = [0] * 12  # Initialize list with 12 zeros for Jan-Dec

            for entry in data:
                visit_date = entry.get('visit_date')
                if not visit_date:
                    continue

                try:
                    visit_dt = datetime.strptime(visit_date, '%Y-%m-%d')
                    if visit_dt.year == year:
                        month = visit_dt.month
                        month_counts[month - 1] += 1  # Convert to 0-indexed
                except (ValueError, TypeError):
                    continue

            return month_counts

        except Exception as e:
            print(f"Database error in get_monthly_visit_trends: {e}")
            # Return list of 12 zeros on error
            return [0] * 12

    @staticmethod
    def upsert_pregnancy_registry(name: str, phone: str, edc_date: str,
                                  gravida_status: Optional[str] = None,
                                  status: str = 'Active') -> Dict[str, Any]:
        """
        Insert or update pregnancy registry record.

        Uses composite primary key (patient_name, phone_number, gravida_status).
        This ensures that EDC corrections only overwrite the row matching that specific pregnancy instance.

        Args:
            name: Patient name
            phone: Patient phone number
            edc_date: Expected Date of Confinement
            gravida_status: Gravida status (G1, G2, G3, etc.) for precise matching
            status: Pregnancy status (default 'Active')

        Returns:
            Upserted record as dict, or empty dict on error
        """
        data = {
            'patient_name': name,
            'phone_number': phone,
            'edc_date': edc_date,
            'status': status,
        }
        if gravida_status is not None:
            data['gravida_status'] = gravida_status

        try:
            result = supabase.table('pregnancy_registry').upsert(data).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in upsert_pregnancy_registry: {e}")
            return {}

    # ========================================
    # BOOKINGS TABLE OPERATIONS
    # ========================================
    @staticmethod
    def fetch_bookings_by_date(target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch all bookings scheduled for a specific date (IST timezone).
        If target_date is None, defaults to today's date.
        Returns bookings with patient_id joined from patients table.

        Args:
            target_date: Date string in YYYY-MM-DD format. If None, uses today's date.

        Returns:
            List of booking dictionaries with patient_id included
        """
        try:
            # Get current date in IST (UTC+5:30)
            ist_offset = timedelta(hours=5, minutes=30)
            ist_timezone = timezone(ist_offset)
            today_utc = datetime.now(timezone.utc)
            today_ist = today_utc.astimezone(ist_timezone)

            # Use target_date if provided, otherwise use today
            if target_date:
                target_date_str = target_date
            else:
                target_date_str = today_ist.strftime('%Y-%m-%d')

            # Query bookings for the target date with selective columns
            bookings_result = supabase.table('bookings')\
                .select('patient_name', 'phone_number', 'planned_date', 'status', 'booked_by')\
                .eq('planned_date', target_date_str)\
                .execute()
            bookings = bookings_result.data if bookings_result.data else []

            # Join with patients table to get patient_id (batch query optimization)
            result = []
            for booking in bookings:
                patient_name = booking.get('patient_name')
                phone_number = booking.get('phone_number')

                # Fetch patient to get patient_id
                patient = DatabaseManager.fetch_patient(patient_name, phone_number)
                patient_id = patient.get('patient_id') if patient else None

                # Combine booking data with patient_id
                booking_with_id = booking.copy()
                booking_with_id['patient_id'] = patient_id
                result.append(booking_with_id)

            return result

        except Exception as e:
            print(f"Database error in fetch_bookings_by_date: {e}")
            return []

    @staticmethod
    def fetch_todays_bookings() -> List[Dict[str, Any]]:
        """
        Fetch all bookings scheduled for today (IST timezone).
        Returns bookings with patient_id joined from patients table.
        (Legacy method - calls fetch_bookings_by_date with no date parameter)
        """
        return DatabaseManager.fetch_bookings_by_date()

    @staticmethod
    def upsert_booking(name: str, phone: str, planned_date: str, booked_by: str = 'Auto', status: str = '') -> Dict[str, Any]:
        """Insert or update a booking record."""
        data = {
            'patient_name': name,
            'phone_number': phone,
            'planned_date': planned_date,
            'booked_by': booked_by,
            'status': status,
        }
        try:
            result = supabase.table('bookings').upsert(data).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in upsert_booking: {e}")
            return {}

    @staticmethod
    def fetch_booking(name: str, phone: str) -> Optional[Dict[str, Any]]:
        """Fetch a booking by patient name and phone number."""
        try:
            result = supabase.table('bookings').select('*').ilike('patient_name', name).ilike('phone_number', phone).execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            print(f"Database error in fetch_booking: {e}")
        return None

    @staticmethod
    def mark_booking_visited(name: str, phone: str) -> Dict[str, Any]:
        """Mark a booking as visited by setting status to 'Visited'."""
        try:
            result = supabase.table('bookings').update({'status': 'Visited'})\
                .ilike('patient_name', name)\
                .ilike('phone_number', phone)\
                .execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in mark_booking_visited: {e}")
            return {}

    @staticmethod
    def sync_missed_bookings_to_tasks() -> Dict[str, Any]:
        """
        Sync missed bookings to patient tasks for no-show followup automation.
        Optimized to only fetch relevant bookings from the past 90 days.

        LOGIC:
        - Query bookings where planned_date < CURRENT_DATE AND status != 'Visited' (limited to 90 days back)
        - For each booking, check if a 'No Visit' task already exists with status='Pending'
        - If not exists, create new task with:
          - assignee: 'Nimisha'
          - followup_type: 'No Visit'
          - status: 'Pending'
          - due_date: CURRENT_DATE

        Returns:
            Dict with sync results: {'tasks_created': int, 'tasks_skipped': int, 'errors': list}
        """
        result = {
            'tasks_created': 0,
            'tasks_skipped': 0,
            'errors': []
        }

        try:
            # Using UTC for consistent date calculation
            today_dt = datetime.now(timezone.utc).astimezone()
            today_str = today_dt.strftime('%Y-%m-%d')
            # Only look at bookings from the past 90 days to save memory
            ninety_days_ago = (today_dt - timedelta(days=90)).strftime('%Y-%m-%d')

            print(f"[DEBUG sync_missed_bookings_to_tasks] today={today_str}, ninety_days_ago={ninety_days_ago}")

            # Fetch bookings where planned_date < today AND status != 'Visited' AND planned_date >= 90 days ago
            # Use lt filter for planned_date < today to reduce data fetch
            bookings_result = supabase.table('bookings')\
                .select('patient_name', 'phone_number', 'planned_date', 'status', 'booked_by')\
                .gte('planned_date', ninety_days_ago)\
                .not_.eq('status', 'Visited')\
                .execute()
            all_bookings = bookings_result.data if bookings_result.data else []

            print(f"[DEBUG sync_missed_bookings_to_tasks] Found {len(all_bookings)} non-visited bookings in past 90 days")

            for booking in all_bookings:
                planned_date_str = booking.get('planned_date')
                status = booking.get('status', '')
                patient_name = booking.get('patient_name')
                phone_number = booking.get('phone_number')
                booked_by = booking.get('booked_by')

                try:
                    # Check if booking is missed (planned_date < today)
                    if planned_date_str:
                        planned_date = datetime.strptime(planned_date_str, '%Y-%m-%d').replace(tzinfo=today_dt.tzinfo)

                        if planned_date < today_dt:
                            print(f"[DEBUG sync_missed_bookings_to_tasks] Processing missed booking: {patient_name}, {phone_number}, planned_date={planned_date_str}, status={status}, booked_by={booked_by}")

                            # Check if patient exists in patients table (foreign key constraint)
                            patient_check = supabase.table('patients').select('patient_name', 'phone_number')\
                                .eq('patient_name', patient_name)\
                                .eq('phone_number', phone_number)\
                                .execute()

                            if not patient_check.data:
                                # Patient doesn't exist, skip creating task
                                print(f"[DEBUG sync_missed_bookings_to_tasks] Skipping {patient_name}: Patient not found in patients table")
                                result['tasks_skipped'] += 1
                                continue

                            # Check if Pending No Visit task already exists
                            # We don't filter by assignee - if any Pending task exists, don't create duplicate
                            existing_task = supabase.table('patient_tasks').select('*')\
                                .ilike('patient_name', patient_name)\
                                .ilike('phone_number', phone_number)\
                                .eq('followup_type', 'No Visit')\
                                .eq('status', 'Pending')\
                                .execute()

                            if existing_task.data:
                                # Pending task already exists, skip
                                print(f"[DEBUG sync_missed_bookings_to_tasks] Skipping {patient_name}: Pending No Visit task already exists")
                                result['tasks_skipped'] += 1
                            else:
                                # Create new No Visit task
                                # Default assignee can be configured or fetched from database
                                task_data = {
                                    'assignee': 'Nimisha',  # TODO: Make configurable via env var or database
                                    'patient_name': patient_name,
                                    'phone_number': phone_number,
                                    'followup_type': 'No Visit',
                                    'status': 'Pending',
                                    'due_date': today_str
                                }

                                supabase.table('patient_tasks').insert(task_data).execute()
                                result['tasks_created'] += 1
                                print(f"[DEBUG sync_missed_bookings_to_tasks] Created No Visit task for {patient_name} (missed date: {planned_date_str})")

                except (ValueError, TypeError) as e:
                    error_msg = f"Error processing booking for {patient_name}: {e}"
                    print(error_msg)
                    result['errors'].append(error_msg)

        except Exception as e:
            error_msg = f"Error in sync_missed_bookings_to_tasks: {e}"
            print(error_msg)
            result['errors'].append(error_msg)

        return result

    @staticmethod
    def debug_sync_state() -> Dict[str, Any]:
        """
        Debug helper function to analyze current sync state.
        Returns information about bookings and tasks for troubleshooting.
        """
        result = {
            'bookings_today_auto': [],
            'bookings_today_other': [],
            'bookings_missed': [],
            'pending_no_visit_tasks': [],
            'pending_today_reminders': [],
        }

        try:
            today_dt = datetime.now(timezone.utc).astimezone()
            today_str = today_dt.strftime('%Y-%m-%d')
            ninety_days_ago = (today_dt - timedelta(days=90)).strftime('%Y-%m-%d')

            # Check today's bookings with booked_by = 'Auto'
            today_auto_result = supabase.table('bookings').select('*')\
                .eq('planned_date', today_str)\
                .eq('booked_by', 'Auto')\
                .execute()
            result['bookings_today_auto'] = today_auto_result.data if today_auto_result.data else []

            # Check today's bookings with other booked_by values
            today_other_result = supabase.table('bookings').select('*')\
                .eq('planned_date', today_str)\
                .not_.eq('booked_by', 'Auto')\
                .execute()
            result['bookings_today_other'] = today_other_result.data if today_other_result.data else []

            # Check missed bookings (planned_date < today AND status != 'Visited')
            missed_result = supabase.table('bookings').select('*')\
                .gte('planned_date', ninety_days_ago)\
                .not_.eq('status', 'Visited')\
                .execute()

            # Filter for actual missed bookings in Python
            for booking in (missed_result.data if missed_result.data else []):
                planned_date_str = booking.get('planned_date')
                if planned_date_str:
                    planned_date = datetime.strptime(planned_date_str, '%Y-%m-%d').replace(tzinfo=today_dt.tzinfo)
                    if planned_date < today_dt:
                        result['bookings_missed'].append(booking)

            # Check existing Pending No Visit tasks (any assignee)
            no_visit_tasks_result = supabase.table('patient_tasks').select('*')\
                .eq('followup_type', 'No Visit')\
                .eq('status', 'Pending')\
                .execute()
            result['pending_no_visit_tasks'] = no_visit_tasks_result.data if no_visit_tasks_result.data else []

            # Check existing Pending Today Reminder tasks (any assignee)
            today_reminder_result = supabase.table('patient_tasks').select('*')\
                .ilike('followup_type', '0-Day Reminder')\
                .eq('status', 'Pending')\
                .execute()
            result['pending_today_reminders'] = today_reminder_result.data if today_reminder_result.data else []

            print(f"[DEBUG] Today's bookings with booked_by='Auto': {len(result['bookings_today_auto'])}")
            print(f"[DEBUG] Today's bookings with other booked_by: {len(result['bookings_today_other'])}")
            print(f"[DEBUG] Missed bookings (status != Visited): {len(result['bookings_missed'])}")
            print(f"[DEBUG] Pending No Visit tasks: {len(result['pending_no_visit_tasks'])}")
            print(f"[DEBUG] Pending Today Reminder tasks: {len(result['pending_today_reminders'])}")

        except Exception as e:
            print(f"Error in debug_sync_state: {e}")
            result['error'] = str(e)

        return result

    @staticmethod
    def create_daily_admin_chores() -> Dict[str, Any]:
        """
        Create recurring daily administrative chores for staff in the admin_tasks table.

        Creates four specific tasks for each active staff member:
        1. 'Add all new bookings into system (Deadline: 5 PM)'
        2. 'Update receptionist names (First Floor & Base Floor)'
        3. 'Call all newly booked patients for confirmation'
        4. 'Update staff lunch timings (Deadline: 12:30 PM)'
        5. 'Add all new visit into system (Deadline: 6:00 PM)'

        All tasks have due_date set to CURRENT_DATE and status='Pending'.
        Prevents duplication by checking if tasks already exist for current date.

        Returns:
            Dict with creation results: {'tasks_created': int, 'tasks_skipped': int, 'errors': list}
        """
        result = {
            'tasks_created': 0,
            'tasks_skipped': 0,
            'errors': []
        }

        # Define the four daily admin chores
        daily_chores = [
            'Add all new bookings into system (Deadline: 5 PM)',
            'Update receptionist names (First Floor & Base Floor)',
            'Call all newly booked patients for confirmation',
            'Update staff lunch timings (Deadline: 12:30 PM)',
            'Add all new visit into system (Deadline: 6:00 PM)'

        ]

        try:
            today = datetime.now().strftime('%Y-%m-%d')

            # Get all active staff members (cached)
            staff_list = DatabaseManager.get_all_active_staff()
            print(f"[DEBUG create_daily_admin_chores] Found {len(staff_list)} active staff members")

            for staff in staff_list:
                staff_name = staff.get('staff_name')
                if not staff_name:
                    continue

                print(f"[DEBUG create_daily_admin_chores] Creating chores for staff: {staff_name}")

                # For each staff member, check and create the four daily chores
                for chore_message in daily_chores:
                    try:
                        # Check if this task already exists for today (optimized query)
                        existing_task = supabase.table('admin_tasks').select('id')\
                            .ilike('assignee', staff_name)\
                            .ilike('task_message', chore_message)\
                            .eq('status', 'Pending')\
                            .eq('due_date', today)\
                            .execute()

                        if existing_task.data:
                            # Task already exists for today, skip
                            print(f"[DEBUG create_daily_admin_chores] Skipping existing task: {chore_message}")
                            result['tasks_skipped'] += 1
                        else:
                            # Create new admin task
                            task_data = {
                                'task_message': chore_message,
                                'assignee': staff_name,
                                'status': 'Pending',
                                'due_date': today
                            }

                            supabase.table('admin_tasks').insert(task_data).execute()
                            print(f"[DEBUG create_daily_admin_chores] Created task: {chore_message}")
                            result['tasks_created'] += 1

                    except Exception as e:
                        error_msg = f"Error creating chore '{chore_message}' for {staff_name}: {e}"
                        print(error_msg)
                        result['errors'].append(error_msg)

        except Exception as e:
            error_msg = f"Error in create_daily_admin_chores: {e}"
            print(error_msg)
            result['errors'].append(error_msg)

        return result

    # ========================================
    # ADMIN TASKS TABLE OPERATIONS
    # ========================================
    @staticmethod
    def create_admin_task(task_message: str, assignee: Optional[str] = None,
                          assignee_id: Optional[int] = None,
                          assigned_by: Optional[int] = None,
                          due_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new admin task (general clinic chore - Unplanned Tasks).
        Uses UUID primary key generated by Supabase.
        Uses 'task_message', 'assignee', 'assignee_id', and 'assigned_by' fields as per schema.
        """
        data = {
            'task_message': task_message,
        }
        if assignee is not None:
            data['assignee'] = assignee
        if assignee_id is not None:
            data['assignee_id'] = assignee_id
        if assigned_by is not None:
            data['assigned_by'] = assigned_by
        if due_date is not None:
            data['due_date'] = due_date

        try:
            result = supabase.table('admin_tasks').insert(data).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in create_admin_task: {e}")
            return {}

    @staticmethod
    def fetch_admin_tasks(assignee: Optional[str] = None, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        """Fetch admin tasks, optionally filtered by assignee and status with limit."""
        try:
            query = supabase.table('admin_tasks')\
                .select('id', 'task_message', 'assignee', 'status', 'due_date', 'created_at')
            if assignee:
                query = query.ilike('assignee', assignee)
            if status:
                query = query.eq('status', status)
            query = query.limit(limit)
            result = query.execute()
            tasks = result.data if result.data else []
            print(f"[DEBUG fetch_admin_tasks] assignee={assignee}, status={status}, fetched={len(tasks)} tasks")
            for task in tasks:
                print(f"  - {task.get('task_message')}: due_date={task.get('due_date')}, status={task.get('status')}, id={task.get('id')}")
            return tasks
        except Exception as e:
            print(f"Database error in fetch_admin_tasks: {e}")
            return []

    @staticmethod
    def update_admin_task_status(task_id: str, status: str) -> Dict[str, Any]:
        """Update admin task status by UUID."""
        try:
            result = supabase.table('admin_tasks').update({'status': status}).eq('id', task_id).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in update_admin_task_status: {e}")
            return {}

    @staticmethod
    def complete_admin_task_by_message(assignee: str, task_message: str) -> Dict[str, Any]:
        """
        Mark an admin task as completed by task message and assignee.

        Args:
            assignee: The staff member name
            task_message: The task message to identify the specific task

        Returns:
            The updated task record
        """
        try:
            result = supabase.table('admin_tasks').update({'status': 'Completed'})\
                .ilike('assignee', assignee)\
                .ilike('task_message', task_message)\
                .eq('status', 'Pending')\
                .execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in complete_admin_task_by_message: {e}")
            return {}

    # ========================================
    # PATIENT TASKS TABLE OPERATIONS
    # ========================================
    @staticmethod
    def create_patient_task(assignee: str, name: str, phone: str, followup_type: str,
                           status: str = 'Pending', due_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new patient task (clinical follow-up - Planned Call).
        Primary Key: (assignee, patient_name, phone_number, followup_type)
        """
        data = {
            'assignee': assignee,
            'patient_name': name,
            'phone_number': phone,
            'followup_type': followup_type,
            'status': status,
        }
        if due_date is not None:
            data['due_date'] = due_date

        try:
            result = supabase.table('patient_tasks').insert(data).execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in create_patient_task: {e}")
            return {}

    @staticmethod
    def fetch_patient_tasks(assignee: Optional[str] = None, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        """Fetch patient tasks, optionally filtered by assignee and status with limit."""
        try:
            query = supabase.table('patient_tasks')\
                .select('assignee', 'patient_name', 'phone_number', 'followup_type', 'status', 'due_date', 'created_at')
            if assignee:
                query = query.ilike('assignee', assignee)
            if status:
                query = query.eq('status', status)
            query = query.limit(limit)
            result = query.execute()
            tasks = result.data if result.data else []
            print(f"[DEBUG fetch_patient_tasks] assignee={assignee}, status={status}, fetched={len(tasks)} tasks")
            for task in tasks:
                print(f"  - {task.get('followup_type')}: {task.get('patient_name')} ({task.get('status')}) assigned to {task.get('assignee')}")
            return tasks
        except Exception as e:
            print(f"Database error in fetch_patient_tasks: {e}")
            return []

    @staticmethod
    def update_patient_task_status(assignee: str, name: str, phone: str, followup_type: str, status: str) -> Dict[str, Any]:
        """
        Update patient task status.
        Uses composite primary key: (assignee, patient_name, phone_number, followup_type)
        """
        try:
            print(f"[DEBUG update_patient_task_status] Updating task: assignee={assignee}, patient_name={name}, phone={phone}, followup_type={followup_type}, new_status={status}")
            result = supabase.table('patient_tasks').update({'status': status})\
                .ilike('assignee', assignee)\
                .ilike('patient_name', name)\
                .ilike('phone_number', phone)\
                .ilike('followup_type', followup_type)\
                .execute()
            print(f"[DEBUG update_patient_task_status] Update result: {result.data if result.data else 'No data returned'}")
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in update_patient_task_status: {e}")
            return {}

    @staticmethod
    def get_staff_task_summary() -> Dict[str, Dict[str, int]]:
        """
        Get task summary for all staff (Admin view).
        Returns counts of Completed vs Pending tasks for each staff member.
        """
        summary = {}

        # Get all active staff from database
        all_staff = DatabaseManager.get_all_active_staff()
        staff_names = [s.get('staff_name') for s in all_staff]

        # Initialize summary for all staff
        for staff in staff_names:
            summary[staff] = {'completed': 0, 'pending': 0}

        # Patient tasks summary
        patient_tasks = DatabaseManager.fetch_patient_tasks()
        for task in patient_tasks:
            assignee = task.get('assignee', 'Unknown')
            status = task.get('status', 'Pending')
            if assignee in summary:
                if status == 'Completed':
                    summary[assignee]['completed'] += 1
                else:
                    summary[assignee]['pending'] += 1

        # Admin tasks summary
        admin_tasks = DatabaseManager.fetch_admin_tasks()
        for task in admin_tasks:
            assignee = task.get('assignee', 'Unknown')
            status = task.get('status', 'Pending')
            if assignee not in summary:
                summary[assignee] = {'completed': 0, 'pending': 0}
            if status == 'Completed':
                summary[assignee]['completed'] += 1
            else:
                summary[assignee]['pending'] += 1

        return summary

    @staticmethod
    def get_staff_granular_audit(staff_name: str = None) -> Dict[str, Any]:
        """
        Get granular audit report for staff (Admin view).
        Returns detailed task information for each staff member including:
        - TODAY: Tasks where (due_date OR created_at) is CURRENT_DATE
        - BACKLOG: Tasks where status = 'Pending' AND date < CURRENT_DATE

        Args:
            staff_name: Optional staff name to filter for specific staff member.
                       If None, returns audit for all staff members.

        Returns:
            Dict with structure:
            {
                'audit_date': 'YYYY-MM-DD',
                'staff': {
                    'Staff Name': {
                        'telegram_id': int,
                        'clinical': {
                            'today': [...],  # List of today's patient_tasks
                            'backlog': [...]  # List of overdue patient_tasks
                        },
                        'admin': {
                            'today': [...],  # List of today's admin_tasks
                            'backlog': [...]  # List of overdue admin_tasks
                        },
                        'overdue_pending': [...]  # Combined list of all overdue pending tasks
                    }
                }
            }
        """
        import json

        # Get current date
        today = datetime.now().strftime('%Y-%m-%d')
        today_dt = datetime.now()

        result = {
            'audit_date': today,
            'staff': {}
        }

        # Get all active staff or specific staff
        if staff_name:
            staff_list = [DatabaseManager.get_staff_by_name(staff_name)]
        else:
            staff_list = DatabaseManager.get_all_active_staff()

        # Initialize audit for each staff member
        for staff in staff_list:
            name = staff.get('staff_name')
            telegram_id = staff.get('telegram_id')
            if not name:
                continue

            result['staff'][name] = {
                'telegram_id': telegram_id,
                'clinical': {'today': [], 'backlog': []},
                'admin': {'today': [], 'backlog': []},
                'overdue_pending': []
            }

        # Get all patient tasks
        patient_tasks = DatabaseManager.fetch_patient_tasks()
        for task in patient_tasks:
            assignee = task.get('assignee', 'Unknown')
            status = task.get('status', 'Pending')
            due_date_str = task.get('due_date', '')
            created_at_str = task.get('created_at', '')

            # Skip if assignee not in audit
            if assignee not in result['staff']:
                continue

            # Skip unplanned tasks (they'll be handled in admin section)
            if task.get('patient_name') == 'Unplanned Task':
                continue

            # Parse dates
            due_date = None
            created_date = None
            try:
                if due_date_str:
                    due_date = datetime.strptime(due_date_str, '%Y-%m-%d')
                if created_at_str:
                    created_date = datetime.strptime(created_at_str.split('T')[0], '%Y-%m-%d')
            except:
                pass

            # Check if task is from today
            is_today = (due_date == today_dt.date()) if due_date else (created_date == today_dt.date()) if created_date else False

            # Check if task is overdue (due date < today and status is pending)
            is_overdue = (due_date and due_date < today_dt.date() and status == 'Pending')

            # Add to clinical section
            task_data = {
                'patient_name': task.get('patient_name', 'N/A'),
                'phone_number': task.get('phone_number', 'N/A'),
                'task_type': task.get('followup_type', 'N/A'),
                'status': status,
                'due_date': due_date_str,
                'created_at': created_at_str
            }

            if is_today:
                result['staff'][assignee]['clinical']['today'].append(task_data)
            elif is_overdue:
                result['staff'][assignee]['clinical']['backlog'].append(task_data)
                result['staff'][assignee]['overdue_pending'].append({
                    'source': 'Clinical',
                    'description': f"{task.get('patient_name', 'N/A')}: {task.get('followup_type', 'N/A')}",
                    'date': due_date_str,
                    'status': status
                })
            elif status == 'Pending' and due_date:
                # Add to overdue if it's a pending task from before today
                if due_date < today_dt.date():
                    result['staff'][assignee]['clinical']['backlog'].append(task_data)
                    result['staff'][assignee]['overdue_pending'].append({
                        'source': 'Clinical',
                        'description': f"{task.get('patient_name', 'N/A')}: {task.get('followup_type', 'N/A')}",
                        'date': due_date_str,
                        'status': status
                    })

        # Get all admin tasks
        admin_tasks = DatabaseManager.fetch_admin_tasks()
        for task in admin_tasks:
            assignee = task.get('assignee', 'Unknown')
            status = task.get('status', 'Pending')
            due_date_str = task.get('due_date', '')
            created_at_str = task.get('created_at', '')
            task_id = task.get('id', '')

            # Skip if assignee not in audit
            if assignee not in result['staff']:
                continue

            # Parse dates
            due_date = None
            created_date = None
            try:
                if due_date_str:
                    due_date = datetime.strptime(due_date_str, '%Y-%m-%d')
                if created_at_str:
                    created_date = datetime.strptime(created_at_str.split('T')[0], '%Y-%m-%d')
            except:
                pass

            # Check if task is from today
            is_today = (due_date == today_dt.date()) if due_date else (created_date == today_dt.date()) if created_date else False

            # Check if task is overdue (due date < today and status is pending)
            is_overdue = (due_date and due_date < today_dt.date() and status == 'Pending')

            # Add to admin section
            task_data = {
                'id': str(task_id) if task_id else 'N/A',
                'task_message': task.get('task_message', 'N/A'),
                'status': status,
                'due_date': due_date_str,
                'created_at': created_at_str
            }

            if is_today:
                result['staff'][assignee]['admin']['today'].append(task_data)
            elif is_overdue:
                result['staff'][assignee]['admin']['backlog'].append(task_data)
                result['staff'][assignee]['overdue_pending'].append({
                    'source': 'Admin',
                    'description': task.get('task_message', 'N/A'),
                    'date': due_date_str,
                    'status': status,
                    'task_id': str(task_id) if task_id else 'N/A'
                })
            elif status == 'Pending' and due_date:
                # Add to overdue if it's a pending task from before today
                if due_date < today_dt.date():
                    result['staff'][assignee]['admin']['backlog'].append(task_data)
                    result['staff'][assignee]['overdue_pending'].append({
                        'source': 'Admin',
                        'description': task.get('task_message', 'N/A'),
                        'date': due_date_str,
                        'status': status,
                        'task_id': str(task_id) if task_id else 'N/A'
                    })

        # Sort overdue_pending by date
        for staff_name in result['staff']:
            result['staff'][staff_name]['overdue_pending'].sort(key=lambda x: x.get('date', '9999-12-31'))

        return result

    # ========================================
    # UTILITY FUNCTIONS
    # ========================================
    @staticmethod
    def calculate_due_date(base_date: str, days: int) -> str:
        """
        Calculate due date with Sunday shift to Monday.
        Used for both Planned and Unplanned tasks.
        """
        base_dt = datetime.strptime(base_date, '%Y-%m-%d')
        due_dt = base_dt + timedelta(days=days)

        # If due date is Sunday, shift to Monday
        if due_dt.weekday() == 6:  # Sunday is 6
            due_dt = due_dt + timedelta(days=1)

        return due_dt.strftime('%Y-%m-%d')

    @staticmethod
    def sync_bookings_to_tasks(target_days: int, task_label: str) -> Dict[str, Any]:
        """
        Sync bookings to patient tasks for automation.

        LOGIC:
        - Query bookings table for records where planned_date matches (Today + target_days)
          - For target_days = 0: exact match on booked_by = 'Auto'
          - For target_days > 0: case-insensitive match on booked_by containing 'Auto'
        - Check if a Pending task already exists with same (patient_name, phone_number, followup_type)
        - If not exists, insert new task assigned to Nimisha with due_date = Today

        Args:
            target_days: Days from today to sync (0 = today, 3 = 3 days from now, 14 = 14 days from now)
            task_label: The followup_type label for the task (e.g., "0-Day Reminder", "3-Day Reminder", "14-Day Reminder", "No Visit")

        Returns:
            Dict with sync results: {'tasks_created': int, 'tasks_skipped': int, 'errors': list}
        """
        result = {
            'tasks_created': 0,
            'tasks_skipped': 0,
            'errors': []
        }

        try:
            # Calculate target date (using UTC to ensure consistent date calculation)
            today = datetime.now(timezone.utc).astimezone()
            target_date = today + timedelta(days=target_days)
            target_date_str = target_date.strftime('%Y-%m-%d')
            today_str = today.strftime('%Y-%m-%d')

            print(f"[DEBUG sync_bookings_to_tasks] target_days={target_days}, target_date={target_date_str}, today={today_str}")

            # Query bookings for the target date, only for automated bookings
            # For target_days = 0 (today), use exact match for booked_by = 'Auto'
            if target_days == 0:
                bookings_result = supabase.table('bookings').select('*')\
                    .eq('planned_date', target_date_str)\
                    .eq('booked_by', 'Auto')\
                    .execute()
            else:
                bookings_result = supabase.table('bookings').select('*')\
                    .eq('planned_date', target_date_str)\
                    .ilike('booked_by', 'Auto')\
                    .execute()
            bookings = bookings_result.data if bookings_result.data else []

            print(f"[DEBUG sync_bookings_to_tasks] Found {len(bookings)} bookings for {target_date_str}")

            for booking in bookings:
                patient_name = booking.get('patient_name')
                phone_number = booking.get('phone_number')
                booked_by = booking.get('booked_by')
                planned_date = booking.get('planned_date')

                print(f"[DEBUG sync_bookings_to_tasks] Processing booking: {patient_name}, {phone_number}, booked_by={booked_by}, planned_date={planned_date}")

                try:
                    # Check if patient exists in patients table (foreign key constraint)
                    patient_check = supabase.table('patients').select('patient_name', 'phone_number')\
                        .eq('patient_name', patient_name)\
                        .eq('phone_number', phone_number)\
                        .execute()

                    if not patient_check.data:
                        # Patient doesn't exist, skip creating task
                        print(f"[DEBUG sync_bookings_to_tasks] Skipping {patient_name}: Patient not found in patients table")
                        result['tasks_skipped'] += 1
                        continue

                    # Check if Pending task already exists with same (patient_name, phone_number, followup_type)
                    # We don't filter by assignee - if any Pending task exists, don't create duplicate
                    existing_task = supabase.table('patient_tasks').select('*')\
                        .ilike('patient_name', patient_name)\
                        .ilike('phone_number', phone_number)\
                        .ilike('followup_type', task_label)\
                        .eq('status', 'Pending')\
                        .execute()

                    if existing_task.data:
                        # Pending task already exists, skip
                        print(f"[DEBUG sync_bookings_to_tasks] Skipping {patient_name}: Pending task already exists")
                        result['tasks_skipped'] += 1
                    else:
                        # Create new task
                        # Default assignee can be configured or fetched from database
                        task_data = {
                            'assignee': 'Nimisha',  # TODO: Make configurable via env var or database
                            'patient_name': patient_name,
                            'phone_number': phone_number,
                            'followup_type': task_label,
                            'status': 'Pending',
                            'due_date': today_str
                        }

                        supabase.table('patient_tasks').insert(task_data).execute()
                        result['tasks_created'] += 1
                        print(f"[DEBUG sync_bookings_to_tasks] Created task for {patient_name}: {task_label}")

                except Exception as e:
                    error_msg = f"Error syncing booking for {patient_name}: {e}"
                    print(error_msg)
                    result['errors'].append(error_msg)

        except Exception as e:
            error_msg = f"Error in sync_bookings_to_tasks: {e}"
            print(error_msg)
            result['errors'].append(error_msg)

        return result

    @staticmethod
    def fetch_latest_visit(name: str, phone: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest visit for a patient.
        Returns the most recent visit record with gravida_status snapshot.
        """
        try:
            result = supabase.table('visits').select('*').ilike('patient_name', name).ilike('phone_number', phone)\
                .order('visit_date', desc=True).limit(1).execute()
            if result.data:
                return result.data[0]
        except Exception as e:
            print(f"Database error in fetch_latest_visit: {e}")
        return None

    @staticmethod
    def update_latest_visit_status(name: str, phone: str, followup_status: str) -> Dict[str, Any]:
        """
        Update the followup_status of the latest visit for a patient.
        Only updates the most recent visit record.
        """
        try:
            # First, get the latest visit date
            latest_visit = DatabaseManager.fetch_latest_visit(name, phone)
            if not latest_visit:
                return {}

            latest_visit_date = latest_visit.get('visit_date')
            if not latest_visit_date:
                return {}

            # Update only the latest visit
            result = supabase.table('visits').update({'followup_status': followup_status})\
                .ilike('patient_name', name)\
                .ilike('phone_number', phone)\
                .eq('visit_date', latest_visit_date)\
                .execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in update_latest_visit_status: {e}")
            return {}

    @staticmethod
    def update_pregnancy_registry_status(name: str, phone: str, gravida: str, status: str) -> Dict[str, Any]:
        """
        Update pregnancy registry status for a patient.
        Matches on name, phone, AND gravida_status to ensure correct pregnancy.
        """
        try:
            result = supabase.table('pregnancy_registry').update({'status': status})\
                .ilike('patient_name', name)\
                .ilike('phone_number', phone)\
                .eq('gravida_status', gravida)\
                .execute()
            return result.data[0] if result.data else {}
        except Exception as e:
            print(f"Database error in update_pregnancy_registry_status: {e}")
            return {}

    @staticmethod
    def update_all_patient_tasks_status(name: str, phone: str, status: str) -> List[Dict[str, Any]]:
        """
        Update all patient tasks for a specific patient.
        Updates all tasks matching the patient_name and phone_number.
        """
        try:
            result = supabase.table('patient_tasks').update({'status': status})\
                .ilike('patient_name', name)\
                .ilike('phone_number', phone)\
                .execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Database error in update_all_patient_tasks_status: {e}")
            return []

    @staticmethod
    def count_old_tasks(year: int, month: int) -> int:
        """
        Count tasks where status != 'Pending' for a specific year and month.
        Used in Task Cleanup workflow to show Admin how many tasks will be deleted.

        Args:
            year: The year (e.g., 2026)
            month: The month (1-12)

        Returns:
            Number of tasks matching the criteria
        """
        try:
            # Build date range for the month
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"

            result = supabase.table('patient_tasks').select('*', count='exact')\
                .not_.eq('status', 'Pending')\
                .gte('created_at', start_date)\
                .lte('created_at', end_date)\
                .execute()

            return result.count if result.count else 0
        except Exception as e:
            print(f"Database error in count_old_tasks: {e}")
            return 0

    @staticmethod
    def delete_old_tasks(year: int, month: int) -> int:
        """
        Delete tasks where status != 'Pending' for a specific year and month.
        Used in Task Cleanup workflow to remove completed tasks.

        LOGIC:
        DELETE FROM patient_tasks
        WHERE status != 'Pending'
        AND created_at >= '{year}-{month}-01'
        AND created_at <= '{year}-{month}-31'

        Args:
            year: The year (e.g., 2026)
            month: The month (1-12)

        Returns:
            Number of tasks deleted
        """
        try:
            # Build date range for the month
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"

            # First count what will be deleted
            count_result = DatabaseManager.count_old_tasks(year, month)

            # Perform the deletion
            result = supabase.table('patient_tasks')\
                .delete()\
                .not_.eq('status', 'Pending')\
                .gte('created_at', start_date)\
                .lte('created_at', end_date)\
                .execute()

            # Critical safety logging
            deleted_count = result.count if hasattr(result, 'count') else count_result
            print(f"CRITICAL: Admin deleted {deleted_count} tasks for {month}/{year}.")

            return deleted_count
        except Exception as e:
            print(f"Database error in delete_old_tasks: {e}")
            return 0

    @staticmethod
    def sync_global_status(name: str, phone: str, status_type: str) -> Dict[str, Any]:
        """
        Global status synchronization for Admin workflow.
        Syncs status across all tables: visits, pregnancy_registry, patient_tasks.

        MAPPING RULES:
        - Delivered: Registry='Delivered', Last Visit='Visited', Tasks='Completed'
        - Unreachable: Registry='Unreachable', Last Visit='Unreachable', Tasks='Unreachable'
        - Discontinued: Registry='Dropped', Last Visit='Discontinued', Tasks='Discontinued'

        Args:
            name: Patient name
            phone: Patient phone number
            status_type: One of 'delivered', 'unreachable', 'discontinued'

        Returns:
            Dict with sync results: {'visits_updated': bool, 'registry_updated': bool, 'tasks_updated': int, 'errors': list}
        """
        result = {
            'visits_updated': False,
            'registry_updated': False,
            'tasks_updated': 0,
            'errors': []
        }

        # Mapping rules
        status_mapping = {
            'delivered': {
                'registry_status': 'Delivered',
                'visit_status': 'Visited',
                'task_status': 'Completed'
            },
            'unreachable': {
                'registry_status': 'Unreachable',
                'visit_status': 'Unreachable',
                'task_status': 'Unreachable'
            },
            'discontinued': {
                'registry_status': 'Dropped',
                'visit_status': 'Discontinued',
                'task_status': 'Discontinued'
            }
        }

        if status_type not in status_mapping:
            result['errors'].append(f"Invalid status_type: {status_type}")
            return result

        mapping = status_mapping[status_type]

        try:
            # A. Fetch Latest Visit to get gravida_status
            latest_visit = DatabaseManager.fetch_latest_visit(name, phone)
            if not latest_visit:
                result['errors'].append(f"No visit found for patient {name}")
                return result

            gravida = latest_visit.get('gravida_status')
            if not gravida:
                result['errors'].append(f"No gravida_status found for patient {name}")
                return result

            # B. UPDATE 'visits' (Last Record Only): Set followup_status
            visit_update = DatabaseManager.update_latest_visit_status(name, phone, mapping['visit_status'])
            if visit_update:
                result['visits_updated'] = True
            else:
                result['errors'].append("Failed to update visits table")

            # C. UPDATE 'pregnancy_registry' (Where name, phone, AND gravida match): Set status
            registry_update = DatabaseManager.update_pregnancy_registry_status(
                name, phone, gravida, mapping['registry_status']
            )
            if registry_update:
                result['registry_updated'] = True
            else:
                result['errors'].append("Failed to update pregnancy_registry table")

            # D. UPDATE 'patient_tasks' (All for this patient): Set status
            tasks_update = DatabaseManager.update_all_patient_tasks_status(
                name, phone, mapping['task_status']
            )
            if tasks_update:
                result['tasks_updated'] = len(tasks_update)
            else:
                result['errors'].append("Failed to update patient_tasks table")

        except Exception as e:
            error_msg = f"Error in sync_global_status: {e}"
            print(error_msg)
            result['errors'].append(error_msg)

        return result

    @staticmethod
    def fetch_by_match(table: str, match_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row from a table by matching all key-value pairs.

        Args:
            table: Table name to query
            match_dict: Dictionary of key-value pairs to match (typically primary keys)

        Returns:
            Single matching row as dict, or None if not found
        """
        try:
            query = supabase.table(table).select('*')
            for key, value in match_dict.items():
                query = query.ilike(key, value)
            result = query.execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            print(f"Database error in fetch_by_match: {e}")
            return None

    @staticmethod
    def update_by_match(table: str, match_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update a single row in a table by matching all key-value pairs.
        Performs a unique update based on the match criteria.

        Args:
            table: Table name to update
            match_dict: Dictionary of key-value pairs to match (typically primary keys)
            update_dict: Dictionary of key-value pairs to update

        Returns:
            Updated row as dict, or None if update failed
        """
        try:
            query = supabase.table(table).update(update_dict)
            for key, value in match_dict.items():
                query = query.ilike(key, value)
            result = query.execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            print(f"Database error in update_by_match: {e}")
            return None