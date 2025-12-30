# scripts/analytics_dashboard.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsDashboard:
    def __init__(self, db_path='data/traffic_analytics.db'):
        self.db_path = db_path
    
    def generate_daily_report(self):
        """Generate daily analytics report"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Get today's date
            # today = datetime.now().strftime("%Y-%m-%d")
            today = datetime.strptime("2019-12-05", "%Y-%m-%d").strftime("%Y-%m-%d")
            
            # Daily peaks
            peaks_df = pd.read_sql('''
                SELECT * FROM daily_peaks 
                WHERE date = ? 
                ORDER BY peak_volume DESC
            ''', conn, params=[today])
            
            # Hourly averages
            hourly_df = pd.read_sql('''
                SELECT * FROM hourly_averages 
                WHERE date(hour_key) = date(?)
                ORDER BY hourly_avg_volume DESC
            ''', conn, params=[today])
            
            # Sensor availability
            availability_df = pd.read_sql('''
                SELECT * FROM sensor_availability 
                WHERE date = ?
                ORDER BY availability_percentage DESC
            ''', conn, params=[today])
            
            # Recent alerts
            alerts_df = pd.read_sql('''
                SELECT * FROM traffic_alerts 
                WHERE date(timestamp) = date(?)
                ORDER BY created_at DESC
            ''', conn, params=[today])
            
            # Generate report
            self.print_report(peaks_df, hourly_df, availability_df, alerts_df, today)
            
            # Generate visualizations
            self.generate_visualizations(hourly_df, availability_df, today)
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
        finally:
            conn.close()
    
    def print_report(self, peaks_df, hourly_df, availability_df, alerts_df, date):
        """Print formatted analytics report"""
        print("\n" + "="*80)
        print(f"🚦 TRAFFIC ANALYTICS DASHBOARD - {date}")
        print("="*80)
        
        # Daily Peaks
        if not peaks_df.empty:
            peak = peaks_df.iloc[0]
            print(f"\n🏆 DAILY PEAK TRAFFIC:")
            print(f"   📍 {peak['peak_intersection']}")
            print(f"   🚗 {peak['peak_volume']} vehicles")
        else:
            print("\n🏆 DAILY PEAK TRAFFIC: No data yet")
        
        # Top 5 Busiest Hours
        if not hourly_df.empty:
            print(f"\n📊 TOP 5 BUSIEST HOURS:")
            top_hours = hourly_df.head(5)
            for _, row in top_hours.iterrows():
                print(f"   🕐 {row['hour_key']}: {row['hourly_avg_volume']:.1f} avg vehicles")
        else:
            print("\n📊 BUSIEST HOURS: No data yet")
        
        # Sensor Availability
        if not availability_df.empty:
            avg_availability = availability_df['availability_percentage'].mean()
            print(f"\n📡 SENSOR AVAILABILITY:")
            print(f"   📊 Average: {avg_availability:.1f}%")
            print(f"   🔝 Best: {availability_df['availability_percentage'].max():.1f}%")
            print(f"   🔻 Worst: {availability_df['availability_percentage'].min():.1f}%")
        else:
            print("\n📡 SENSOR AVAILABILITY: No data yet")
        
        # Recent Alerts
        if not alerts_df.empty:
            print(f"\n🚨 RECENT ALERTS ({len(alerts_df)}):")
            for _, alert in alerts_df.head(5).iterrows():
                print(f"   ⚠️  {alert['alert_type']} at {alert['intersection']}")
        else:
            print(f"\n🚨 RECENT ALERTS: No alerts today")
        
        print("\n" + "="*80)
    
    def generate_visualizations(self, hourly_df, availability_df, date):
        """Generate simple visualizations"""
        if hourly_df.empty or availability_df.empty:
            logger.info("Not enough data for visualizations")
            return
        
        try:
            # Create figure with subplots
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'Traffic Analytics Dashboard - {date}', fontsize=16)
            
            # Plot 1: Hourly volume trends
            if not hourly_df.empty:
                hourly_df['hour'] = hourly_df['hour_key'].str.extract(r'(\d{2})$')
                hourly_avg = hourly_df.groupby('hour')['hourly_avg_volume'].mean()
                ax1.plot(hourly_avg.index, hourly_avg.values, marker='o', linewidth=2)
                ax1.set_title('Average Hourly Traffic Volume')
                ax1.set_xlabel('Hour of Day')
                ax1.set_ylabel('Average Vehicles')
                ax1.grid(True)
            
            # Plot 2: Sensor availability
            if not availability_df.empty:
                availability_df = availability_df.sort_values('availability_percentage')
                ax2.bar(range(len(availability_df)), availability_df['availability_percentage'])
                ax2.set_title('Sensor Availability (%)')
                ax2.set_xlabel('Sensors')
                ax2.set_ylabel('Availability %')
                ax2.grid(True)
            
            # Plot 3: Top intersections by volume
            if not hourly_df.empty:
                top_intersections = hourly_df.groupby('intersection_name')['hourly_avg_volume'].max().nlargest(5)
                ax3.bar(range(len(top_intersections)), top_intersections.values)
                ax3.set_title('Top 5 Intersections by Volume')
                ax3.set_xlabel('Intersections')
                ax3.set_ylabel('Max Hourly Volume')
                ax3.set_xticks(range(len(top_intersections)))
                ax3.set_xticklabels(top_intersections.index, rotation=45)
                ax3.grid(True)
            
            # Plot 4: Data distribution
            if not hourly_df.empty:
                volume_ranges = ['0-10', '11-25', '26-50', '51-100', '100+']
                ranges = [0, 10, 25, 50, 100, float('inf')]
                hourly_df['volume_range'] = pd.cut(hourly_df['hourly_avg_volume'], bins=ranges, labels=volume_ranges)
                range_counts = hourly_df['volume_range'].value_counts().sort_index()
                ax4.bar(range_counts.index, range_counts.values)
                ax4.set_title('Traffic Volume Distribution')
                ax4.set_xlabel('Volume Range (vehicles)')
                ax4.set_ylabel('Count')
                ax4.grid(True)
            
            plt.tight_layout()
            plt.savefig(f'data/analytics_dashboard_{date}.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"📈 Analytics dashboard saved: data/analytics_dashboard_{date}.png")
            
        except Exception as e:
            logger.error(f"Error generating visualizations: {e}")

if __name__ == "__main__":
    dashboard = AnalyticsDashboard()
    dashboard.generate_daily_report()