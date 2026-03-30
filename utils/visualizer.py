"""Visualizer utilities for generating charts and graphs for the clinic bot.
Optimized for 512 MB RAM deployment with reduced figure sizes and proper cleanup.
"""
import io
import gc
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from typing import List, Dict, Any
from datetime import datetime

# Use non-interactive backend for server environments
matplotlib.use('Agg')

# Optimized settings for low memory
plt.rcParams['figure.max_open_warning'] = 10  # Limit open figures
plt.rcParams['agg.path.chunksize'] = 10000  # Reduce memory for large plots

# DPI setting - lower DPI uses less memory
GRAPH_DPI = 72  # Reduced from 100


def generate_edc_annual_graph(edc_data: List[Dict[str, Any]], year: int) -> io.BytesIO:
    """
    Generate an annual EDC graph showing all expected delivery dates for a given year.
    Optimized with smaller figure size and lower DPI.

    Args:
        edc_data: List of dictionaries with 'patient_name' and 'edc_date' keys
        year: The year for the EDC calendar (e.g., 2026)

    Returns:
        BytesIO object containing the PNG image of the graph
    """
    # Create a smaller figure with 12 subplots (one for each month)
    # Reduced from (18, 12) to (14, 10) for memory efficiency
    fig, axes = plt.subplots(4, 3, figsize=(14, 10))
    fig.suptitle(f'🤰 EDC Planner for {year} - KadeejaClinic', fontsize=14, fontweight='bold')

    # Month names and their lengths
    months = [
        ('January', 31), ('February', 29 if year % 4 == 0 else 28),
        ('March', 31), ('April', 30), ('May', 31), ('June', 30),
        ('July', 31), ('August', 31), ('September', 30),
        ('October', 31), ('November', 30), ('December', 31)
    ]

    # Group EDCs by month
    edc_by_month = {i: [] for i in range(12)}  # 0-11 for January-December

    for edc in edc_data:
        edc_date = edc.get('edc_date')
        patient_name = edc.get('patient_name', 'Unknown')

        try:
            edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
            if edc_dt.year == year:
                month_idx = edc_dt.month - 1
                edc_by_month[month_idx].append({
                    'name': patient_name,
                    'date': edc_dt
                })
        except (ValueError, TypeError):
            continue

    # Generate each month subplot
    for month_idx, (month_name, days_in_month) in enumerate(months):
        row = month_idx // 3
        col = month_idx % 3
        ax = axes[row, col]

        # Set up the axis
        ax.set_xlim(0, days_in_month + 1)
        ax.set_ylim(0, 10)
        ax.set_xlabel('Day of Month', fontsize=7)
        ax.set_ylabel('Patients', fontsize=7)
        ax.set_title(f'{month_name}', fontsize=9, fontweight='bold')

        # Add grid lines for each day
        ax.set_xticks(range(1, days_in_month + 1, 5))
        ax.set_xticks(range(1, days_in_month + 1), minor=True)
        ax.grid(True, which='minor', alpha=0.2, linestyle='--')
        ax.grid(True, which='major', alpha=0.3)

        # Remove y-axis ticks (not needed)
        ax.set_yticks([])

        # Plot EDCs for this month
        edcs_in_month = edc_by_month[month_idx]
        if edcs_in_month:
            # Group patients by date to handle overlaps
            edcs_by_date = {}
            for edc in edcs_in_month:
                day = edc['date'].day
                if day not in edcs_by_date:
                    edcs_by_date[day] = []
                edcs_by_date[day].append(edc['name'])

            # Plot each date with stacked names
            for day, patient_names in edcs_by_date.items():
                y_position = 9  # Start from top

                # Use soft colors based on day of month
                color_idx = (day % 12)
                colors = plt.cm.Pastel1(color_idx)

                for idx, name in enumerate(patient_names):
                    # Plot a marker for each patient
                    ax.scatter(
                        day, y_position - idx,
                        s=200, c=[colors],  # Reduced from 300 to 200
                        edgecolors='black', linewidth=0.5,
                        zorder=5
                    )

                    # Annotate with patient name
                    ax.annotate(
                        name,
                        (day, y_position - idx),
                        xytext=(5, 0),
                        textcoords='offset points',
                        fontsize=6,  # Reduced from 7 to 6
                        ha='left',
                        va='center',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='none', alpha=0.8)
                    )
        else:
            # No EDCs in this month
            ax.text(
                days_in_month / 2, 5,
                'No EDCs',
                ha='center', va='center',
                fontsize=8, color='gray',
                style='italic'
            )

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf


def generate_edc_horizontal_graph(edc_data: List[Dict[str, Any]], year: int) -> io.BytesIO:
    """
    Generate a horizontal timeline view of EDCs for the entire year.
    Optimized with smaller figure size and lower DPI.

    Args:
        edc_data: List of dictionaries with 'patient_name' and 'edc_date' keys
        year: The year for the EDC calendar (e.g., 2026)

    Returns:
        BytesIO object containing the PNG image of the graph
    """
    # Create a smaller horizontal plot
    # Reduced from (16, 10) to (12, 8) for memory efficiency
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.suptitle(f'🤰 EDC Planner for {year} - KadeejaClinic', fontsize=14, fontweight='bold')

    # Set up the axis for the entire year (365 days)
    ax.set_xlim(0, 365)
    ax.set_xlabel('Day of Year', fontsize=9)
    ax.set_ylabel('Patient Name', fontsize=9)

    # Calculate day of year for each EDC
    edc_with_doy = []
    for edc in edc_data:
        edc_date = edc.get('edc_date')
        patient_name = edc.get('patient_name', 'Unknown')

        try:
            edc_dt = datetime.strptime(edc_date, '%Y-%m-%d')
            if edc_dt.year == year:
                day_of_year = (edc_dt - datetime(year, 1, 1)).days + 1
                edc_with_doy.append({
                    'name': patient_name,
                    'day': day_of_year,
                    'date': edc_date
                })
        except (ValueError, TypeError):
            continue

    # Sort by day of year
    edc_with_doy.sort(key=lambda x: x['day'])

    # Plot each EDC
    if edc_with_doy:
        y_positions = range(len(edc_with_doy))
        colors = plt.cm.Pastel1(range(len(edc_with_doy)))

        for idx, edc in enumerate(edc_with_doy):
            ax.scatter(
                edc['day'], idx,
                s=250,  # Reduced from 400 to 250
                c=[colors[idx]],
                edgecolors='black', linewidth=0.5,
                zorder=5
            )

            # Annotate with patient name and date
            label = f"{edc['name']} ({edc['date']})"
            ax.annotate(
                label,
                (edc['day'], idx),
                xytext=(8, 0),  # Reduced from 10 to 8
                textcoords='offset points',
                fontsize=8,  # Reduced from 9 to 8
                ha='left',
                va='center',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='none', alpha=0.8)
            )

        # Set y-ticks
        ax.set_yticks(y_positions)
        ax.set_yticklabels([edc['name'] for edc in edc_with_doy], fontsize=7)

        # Add month markers
        month_days = [0]
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        cumulative_days = 0
        for month_idx in range(1, 12):
            if month_idx in [1, 3, 5, 7, 8, 10, 12]:  # Months with 31 days
                cumulative_days += 31
            elif month_idx in [4, 6, 9, 11]:  # Months with 30 days
                cumulative_days += 30
            else:  # February
                cumulative_days += 29 if year % 4 == 0 else 28
            month_days.append(cumulative_days)

        for month_idx, day in enumerate(month_days):
            ax.axvline(x=day, color='gray', linestyle='--', alpha=0.3, linewidth=0.5)
            if month_idx < 12:
                ax.text(day + 12, len(edc_with_doy) - 0.5, month_names[month_idx],
                       ha='center', va='top', fontsize=7, color='gray')

        ax.set_xticks(month_days)
    else:
        ax.text(182.5, 0.5, 'No EDCs found for this year',
               ha='center', va='center', fontsize=11, color='gray', style='italic')
        ax.set_yticks([])

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf

def generate_comparative_attrition_plot(current_year: int, attrition_data: Dict[str, Any]) -> io.BytesIO:
    """
    Generate a comparative attrition plot (Grouped Bar Chart) for prev year vs current year.
    Optimized with smaller figure size and lower DPI.

    Args:
        current_year: The current year to compare
        attrition_data: Dictionary from get_attrition_counts_comparative with:
            - 'prev_year': Previous year
            - 'curr_year': Current year
            - 'prev_monthly_counts': List of 12 counts for previous year
            - 'curr_monthly_counts': List of 12 counts for current year

    Returns:
        BytesIO object containing the PNG image of the graph
    """
    prev_year = attrition_data.get('prev_year', current_year - 1)
    curr_year = attrition_data.get('curr_year', current_year)
    prev_counts = attrition_data.get('prev_monthly_counts', [0] * 12)
    curr_counts = attrition_data.get('curr_monthly_counts', [0] * 12)

    # Create figure and axis (reduced from (14, 8) to (12, 6))
    fig, ax = plt.subplots(figsize=(12, 6))

    # Month labels
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    x = range(12)

    # Calculate bar width
    bar_width = 0.35

    # Create grouped bar chart
    bars1 = ax.bar([x_i - bar_width/2 for x_i in x], prev_counts, bar_width,
                   label=str(prev_year), color='#FF9999', alpha=0.8, edgecolor='black', linewidth=0.5)
    bars2 = ax.bar([x_i + bar_width/2 for x_i in x], curr_counts, bar_width,
                   label=str(curr_year), color='#99CCFF', alpha=0.8, edgecolor='black', linewidth=0.5)

    # Set labels and title
    ax.set_xlabel('Month', fontsize=11, fontweight='bold')
    ax.set_ylabel('Number of Patients', fontsize=11, fontweight='bold')
    ax.set_title(f'Comparative Attrition Analysis: {prev_year} vs {curr_year}',
                 fontsize=13, fontweight='bold')

    # Set x-ticks
    ax.set_xticks(x)
    ax.set_xticklabels(months, fontsize=9)

    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', axis='y')
    ax.set_axisbelow(True)

    # Add legend
    ax.legend(loc='upper right', fontsize=9)

    # Add value labels on top of bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=7, fontweight='bold')

    # Add summary statistics
    prev_total = sum(prev_counts)
    curr_total = sum(curr_counts)
    total_change = curr_total - prev_total
    change_percent = (total_change / prev_total * 100) if prev_total > 0 else 0

    summary_text = (
        f"Summary:\n"
        f"{prev_year}: {prev_total} patients\n"
        f"{curr_year}: {curr_total} patients\n"
        f"Change: {total_change:+d} ({change_percent:+.1f}%)"
    )

    # Add summary box
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.3)
    ax.text(0.95, 0.95, summary_text,
           transform=ax.transAxes,
           fontsize=8,
           verticalalignment='top',
           horizontalalignment='right',
           bbox=props)

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf


def generate_new_pregnancy_inflow_graph(preg_data: List[Dict[str, Any]]) -> io.BytesIO:
    """
    Generate a multi-year new pregnancy inflow line graph showing monthly registration counts.
    Optimized with smaller figure size and lower DPI.

    Args:
        preg_data: List of dictionaries with year, month, and count keys
                   (sorted by year, month from get_new_pregnancy_counts)

    Returns:
        BytesIO object containing the PNG image of the graph
    """
    if not preg_data:
        # Create empty graph with message
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.text(0.5, 0.5, "No pregnancy registration data available",
               ha='center', va='center', fontsize=11, color='gray', style='italic')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([])
        ax.set_yticks([])

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)
        return buf

    # Group data by year
    years_data = {}
    for entry in preg_data:
        year = entry['year']
        month = entry['month']
        count = entry['count']

        if year not in years_data:
            years_data[year] = {}
        years_data[year][month] = count

    if not years_data:
        # Create empty graph with message
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.text(0.5, 0.5, "No pregnancy registration data available",
               ha='center', va='center', fontsize=11, color='gray', style='italic')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([])
        ax.set_yticks([])

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)
        return buf

    # Create figure and axis (reduced from (14, 8) to (12, 6))
    fig, ax = plt.subplots(figsize=(12, 6))

    # Define colors for different years (different palette from visit trends)
    color_palette = ['#2ECC71', '#F39C12', '#E74C3C', '#9B59B6', '#1ABC9C',
                     '#34495E', '#E67E22', '#D35400', '#C0392B', '#16A085',
                     '#27AE60', '#2980B9', '#8E44AD', '#F1C40F', '#1F618D']

    # Sort years
    sorted_years = sorted(years_data.keys())

    # Plot each year as a separate line
    for idx, year in enumerate(sorted_years):
        months_data = years_data[year]
        month_counts = [months_data.get(month, 0) for month in range(1, 13)]

        # Create x-axis (months 1-12)
        x_values = range(1, 13)

        # Plot the line
        color = color_palette[idx % len(color_palette)]
        ax.plot(x_values, month_counts,
               marker='o', markersize=6, linewidth=1.5, color=color,  # Reduced from 8 to 6, 2 to 1.5
               label=str(year), alpha=0.8)

        # Add value labels on each point (only for higher counts to reduce clutter)
        for month, count in enumerate(month_counts, 1):
            if count > 0:
                ax.text(month, count + 0.5, str(count),
                       ha='center', va='bottom', fontsize=7, fontweight='bold')

    # Set labels and title
    ax.set_xlabel('Month', fontsize=11, fontweight='bold')
    ax.set_ylabel('New Pregnancies', fontsize=11, fontweight='bold')
    ax.set_title('KadeejaClinic: Monthly New Pregnancy Registrations',
                 fontsize=13, fontweight='bold')

    # Set x-ticks (months)
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(month_labels, fontsize=9)

    # Set y-ticks starting from 0
    all_counts = [entry['count'] for entry in preg_data]
    max_count = max(all_counts) if all_counts else 0
    ax.set_ylim(0, max_count + max(3, max_count * 0.1))

    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    ax.set_axisbelow(True)

    # Add legend
    ax.legend(loc='upper left', fontsize=9)

    # Add summary statistics
    total_pregnancies = sum(entry['count'] for entry in preg_data)
    num_years = len(sorted_years)
    avg_per_month = total_pregnancies / (12 * num_years) if num_years > 0 else 0

    summary_text = (
        f"Summary:\n"
        f"Years: {num_years} ({sorted_years[0]} - {sorted_years[-1]})\n"
        f"Total Pregnancies: {total_pregnancies}\n"
        f"Avg/Month: {avg_per_month:.1f}"
    )

    # Add summary box
    props = dict(boxstyle='round', facecolor='lightgreen', alpha=0.3)
    ax.text(0.95, 0.95, summary_text,
           transform=ax.transAxes,
           fontsize=8,
           verticalalignment='top',
           horizontalalignment='right',
           bbox=props)

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf


def generate_delivery_trend_graph(data: Dict[int, int], year: int) -> io.BytesIO:
    """
    Generate a bar graph showing monthly delivery trends for a given year.

    Args:
        data: Dictionary with month (1-12) as key and count as value
        year: The year for the delivery trend (e.g., 2026)

    Returns:
        BytesIO object containing the PNG image of the bar graph
    """
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))

    # Month labels
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    # Prepare data for bar chart
    months = list(range(1, 13))
    counts = [data.get(month, 0) for month in months]

    # Create bar chart
    bars = ax.bar(months, counts, color='#4ECDC4', edgecolor='#2C3E50', linewidth=1.5, alpha=0.8)

    # Add value labels on top of each bar
    for bar, count in zip(bars, counts):
        if count > 0:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height,
                   f'{count}',
                   ha='center', va='bottom', fontsize=10, fontweight='bold')

    # Set labels and title
    ax.set_xlabel('Month', fontsize=11, fontweight='bold')
    ax.set_ylabel('Expected Deliveries (EDC)', fontsize=11, fontweight='bold')
    ax.set_title(f'Expected Deliveries - {year} | KadeejaClinic',
                 fontsize=13, fontweight='bold')

    # Set x-ticks with month labels
    ax.set_xticks(months)
    ax.set_xticklabels(month_labels, fontsize=9)

    # Set y-ticks starting from 0
    max_count = max(counts) if counts else 0
    ax.set_ylim(0, max_count + max(3, max_count * 0.1))

    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5, axis='y')
    ax.set_axisbelow(True)

    # Add summary statistics
    total_deliveries = sum(counts)
    avg_per_month = total_deliveries / 12 if total_deliveries > 0 else 0
    max_month = month_labels[counts.index(max_count)] if max_count > 0 else 'N/A'

    summary_text = (
        f"Summary:\n"
        f"Year: {year}\n"
        f"Total Expected Deliveries: {total_deliveries}\n"
        f"Avg/Month: {avg_per_month:.1f}\n"
        f"Peak Month: {max_month} ({max_count})"
    )

    # Add summary box
    props = dict(boxstyle='round', facecolor='lightgreen', alpha=0.3)
    ax.text(0.95, 0.95, summary_text,
           transform=ax.transAxes,
           fontsize=8,
           verticalalignment='top',
           horizontalalignment='right',
           bbox=props)

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf


def generate_attrition_trend_graph(data: List[int], year: int) -> io.BytesIO:
    """
    Generate a bar graph showing monthly attrition trends for a given year.

    Args:
        data: List of 12 integers representing counts for Jan-Dec (index 0 = January)
        year: The year for the attrition trend (e.g., 2026)

    Returns:
        BytesIO object containing the PNG image of the bar graph
    """
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))

    # Month labels
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    # Prepare data for bar chart
    months = list(range(1, 13))

    # Create bar chart
    bars = ax.bar(months, data, color='#FF6B6B', edgecolor='#2C3E50', linewidth=1.5, alpha=0.8)

    # Add value labels on top of each bar
    for bar, count in zip(bars, data):
        if count > 0:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height,
                   f'{count}',
                   ha='center', va='bottom', fontsize=10, fontweight='bold')

    # Set labels and title
    ax.set_xlabel('Month', fontsize=11, fontweight='bold')
    ax.set_ylabel('Attrition Count (Unreachable/Dropped)', fontsize=11, fontweight='bold')
    ax.set_title(f'Monthly Attrition Trend - {year} | KadeejaClinic',
                 fontsize=13, fontweight='bold')

    # Set x-ticks with month labels
    ax.set_xticks(months)
    ax.set_xticklabels(month_labels, fontsize=9)

    # Set y-ticks starting from 0
    max_count = max(data) if data else 0
    ax.set_ylim(0, max_count + max(3, max_count * 0.1))

    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5, axis='y')
    ax.set_axisbelow(True)

    # Add summary statistics
    total_attrition = sum(data)
    avg_per_month = total_attrition / 12 if total_attrition > 0 else 0
    max_month = month_labels[data.index(max_count)] if max_count > 0 else 'N/A'

    summary_text = (
        f"Summary:\n"
        f"Year: {year}\n"
        f"Total Attrition Cases: {total_attrition}\n"
        f"Avg/Month: {avg_per_month:.1f}\n"
        f"Peak Month: {max_month} ({max_count})"
    )

    # Add summary box
    props = dict(boxstyle='round', facecolor='lightcoral', alpha=0.3)
    ax.text(0.95, 0.95, summary_text,
           transform=ax.transAxes,
           fontsize=8,
           verticalalignment='top',
           horizontalalignment='right',
           bbox=props)

    plt.tight_layout()

    # Save to BytesIO (avoid disk I/O for 512MB RAM optimization)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf


def generate_visit_trend_graph(data: List[int], year: int) -> io.BytesIO:
    """
    Generate a bar graph showing monthly visit trends for a given year.

    Args:
        data: List of 12 integers representing counts for Jan-Dec (index 0 = January)
        year: The year for the visit trend (e.g., 2026)

    Returns:
        BytesIO object containing the PNG image of the bar graph
    """
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))

    # Month labels
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    # Prepare data for bar chart
    months = list(range(1, 13))

    # Create bar chart
    bars = ax.bar(months, data, color='#3498DB', edgecolor='#2C3E50', linewidth=1.5, alpha=0.8)

    # Add value labels on top of each bar
    for bar, count in zip(bars, data):
        if count > 0:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height,
                   f'{count}',
                   ha='center', va='bottom', fontsize=10, fontweight='bold')

    # Set labels and title
    ax.set_xlabel('Months', fontsize=11, fontweight='bold')
    ax.set_ylabel('Number of Visits', fontsize=11, fontweight='bold')
    ax.set_title(f'Monthly Visit Trend - {year} | KadeejaClinic',
                 fontsize=13, fontweight='bold')

    # Set x-ticks with month labels
    ax.set_xticks(months)
    ax.set_xticklabels(month_labels, fontsize=9)

    # Set y-ticks starting from 0
    max_count = max(data) if data else 0
    ax.set_ylim(0, max_count + max(3, max_count * 0.1))

    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5, axis='y')
    ax.set_axisbelow(True)

    # Add summary statistics
    total_visits = sum(data)
    avg_per_month = total_visits / 12 if total_visits > 0 else 0
    max_month = month_labels[data.index(max_count)] if max_count > 0 else 'N/A'

    summary_text = (
        f"Summary:\n"
        f"Year: {year}\n"
        f"Total Visits: {total_visits}\n"
        f"Avg/Month: {avg_per_month:.1f}\n"
        f"Peak Month: {max_month} ({max_count})"
    )

    # Add summary box
    props = dict(boxstyle='round', facecolor='lightblue', alpha=0.3)
    ax.text(0.95, 0.95, summary_text,
           transform=ax.transAxes,
           fontsize=8,
           verticalalignment='top',
           horizontalalignment='right',
           bbox=props)

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=GRAPH_DPI, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    # Force garbage collection to free memory
    gc.collect()

    return buf