import pandas as pd
import isodate

class Transform:
    def iso_duration_to_seconds(self, duration_str):
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    
    def convert_to_seconds(self, df, column_name):
        df[column_name] = df[column_name].apply(self.iso_duration_to_seconds)
        return df

    def filter_video_titles(self, df): 
        if 'video_title' in df.columns:
            mask = df['video_title'].isin(['Deleted video', 'Private video'])
            df_filtered = df[~mask]
        else:
            df_filtered = df
        return df_filtered
    
    def convert_to_datetime(self, df, column_name):
        df = self.filter_video_titles(df)
        df[column_name] = pd.to_datetime(df[column_name])
        return df
    
    def drop_duplicate(self, df):
        df = df.drop_duplicates()
        return df