import pandas as pd
import datetime as dt
import os


def read_dir(path):

    return os.listdir(path)


def to_date_obj(timestamp):
    return dt.datetime.strptime(str(timestamp), '%Y%m%d%H%M%S')


non_html = ('.aac', '.abw', '.arc', '.avif', '.avi',
 '.azw', '.bin', '.bmp', '.bz', '.bz2', '.cda',
  '.csh', '.css', '.csv', '.doc', '.docx', '.eot', 
  '.epub', '.gz', '.gif', '.ico', '.ics', '.jar',
   '.jpeg', '.jpg', '.js', '.jsonld', '.mid', '.midi', 
   '.mjs', '.mp3', '.mp4', '.mpeg', '.mpkg', '.odp', '.ods',
    '.odt', '.oga', '.ogv', '.ogx', '.opus', '.otf', '.png', 
    '.pdf', '.ppt', '.pptx', '.rar', '.rtf', '.sh', '.svg', 
    '.tar', '.tif', '.tiff', '.ts', '.ttf', '.vsd', '.wav', 
    '.weba', '.webm', '.webp', '.woff', '.woff2', '.xls', 
    '.xlsx', '.xul', '.zip', '.3gp', '.3g2', '.7z')


def tranform(fileName):
    try:

        df = pd.read_csv(fileName, header=None, delim_whitespace=True)
        df.columns = ["urlkey", "timestamp", "original",
                      "mimetype", "statuscode", "digest", "length"]
        df = df.astype({'timestamp': 'string'})

        df['date'] = df['timestamp'].apply(lambda x: to_date_obj(x))
        df['quarter'] = df['date'].dt.to_period('Q')
        df = df.loc[df.groupby(['quarter', 'urlkey'])['date'].idxmax()]
        df[~df.original.str.endswith(non_html)]
        df.drop(columns =['mimetype', 'statuscode', 'length'], inplace=True)

      
        # os.mkdir('./csv/{}'.format(csv_name))
        df['raw_url'] = "https://web.archive.org/web/" + \
            df['timestamp']+"id_/"+df['original']
            
        csv_name = fileName.split('.txt')[0]    
        df.to_csv('./csv/{}.csv'.format(csv_name))
    except Exception as e:
        print(e)


def main():
    files =  read_dir('./sites')
    for file in files:
        tranform(file)


if __name__ == "__main__":
    main()
