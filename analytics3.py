import sys
from analytics import RealtimeAnalytics

def main():
    # 1. Check if the date argument was provided
    if len(sys.argv) != 2:
        print("Usage: python report.py YYYY-MM-DD")
        sys.exit(1)

    date_str = sys.argv[1]
    
    try:
        # 2. Initialize the service
        analytics = RealtimeAnalytics()
        
        # 3. Fetch the metrics for the provided date
        dau = analytics.count_daily_active_users(date_str)
        uv = analytics.count_unique_visitors(date_str)
        
        # 4. Print the report
        print(f"--- Analytics Report for {date_str} ---")
        print(f"Daily Active Users (DAU): {dau}")
        print(f"Daily Unique Visitors (UV): {uv}")
        
    except Exception as e:
        print(f"Error fetching data: {e}")

if __name__ == '__main__':
    main()
