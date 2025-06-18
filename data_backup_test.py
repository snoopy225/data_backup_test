######FTP서버에 저장#####
import shutil
import psycopg2
import csv
import os
from datetime import datetime, timedelta
import yaml
import tarfile
import paramiko

# config.yml 파일 읽기
config_path = '/home/ubuntu/kosmos/data_backup/config.yml'  # 리눅스 절대 경로로 수정
with open(config_path, 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

# PostgreSQL 데이터베이스 연결 정보
DB_CONFIG = config['DB_CONFIG']

# 백업할 테이블 이름과 날짜 	컬럼
TABLE_NAMES = config['TABLE_NAME']
TIME_COLUMN = config['TIME_COLUMN']

# 백업 파일을 저장할 경로
BACKUP_DIR = config['BACKUP_DIR']
table_names = TABLE_NAMES

# SFTP 서버 정보
SFTP_CONFIG = config['SFTP_CONFIG']
FAILED_UPLOAD_PATH = config['FAILED_UPLOAD_PATH']
def get_latest_backup_date_from_tar():
    """TAR.GZ 파일명을 읽어 가장 최근 백업 날짜 확인"""
    tar_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith('.tar.gz')]
    if not tar_files:
        return None  # TAR 파일이 없으면 None 반환

    # TAR 파일명에서 날짜 추출
    dates = []
    for file in tar_files:
        try:
            date_str = file.split('_')[1].split('.tar.gz')[0]
            dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
            tar_filename = os.path.join(BACKUP_DIR, file)
            os.remove(tar_filename)
            print(f"Deleted local backup file: {tar_filename}")
        except (IndexError, ValueError):
            continue

    return max(dates) if dates else None

def backup_table_by_date(target_date, table_name):
    """특정 날짜의 데이터를 백업"""

    #{TABLE_NAME}_backup_{YYYY-MM-DD}.csv
    file_name = f'{table_name}_backup_{target_date.strftime("%Y-%m-%d")}.csv'
    file_path = os.path.join(BACKUP_DIR, file_name)

    start_time = f"{target_date.strftime('%Y-%m-%d')} 00:00:00.000+09"
    end_time = f"{target_date.strftime('%Y-%m-%d')} 23:59:59.999+09"

    try:
        # PostgreSQL에 연결
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 특정 날짜 데이터 쿼리
        query = f"""
        SELECT * FROM {table_name}
        WHERE {TIME_COLUMN} BETWEEN '{start_time}' AND '{end_time}'
        ORDER BY {TIME_COLUMN}
        ASC;
        """
        cur.execute(query)

        # 결과를 파일로 저장
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([desc[0] for desc in cur.description])  # 컬럼 헤더 쓰기
            writer.writerows(cur.fetchall())  # 데이터 쓰기

        print(f"Backup completed: {file_path}")

    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        # 연결 닫기
        if conn:
            cur.close()
            conn.close()

def backup_missing_dates(table_name, latest_date):
    """누락된 날짜의 데이터를 백업"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today = datetime.today()
    latest_backup_date = latest_date
    if not latest_backup_date:
        print("No previous backup found. Backing up yesterday data.")
        latest_backup_date = today - timedelta(days=2)  # 어제 날짜부터 시작하기 위해 2일전 날짜로 설정

    # 날짜 차이 계산
    delta_days = (today - latest_backup_date).days
    print(f"Days to backup: {delta_days}")
    if delta_days < 1:
        print("No missing backups.")
        return

    # 누락된 날짜별로 백업
    for i in range(1, delta_days):
        target_date = latest_backup_date + timedelta(days=i)
        print(f"Backing up data for: {target_date.strftime('%Y-%m-%d')}")
        backup_table_by_date(target_date, table_name)

def upload_to_sftp(local_file, remote_file):
    """SFTP 서버에 tar.gz 파일 업로드"""
    try:
        transport = paramiko.Transport((SFTP_CONFIG['HOST'], SFTP_CONFIG['PORT']))
        transport.connect(username=SFTP_CONFIG['USERNAME'], password=SFTP_CONFIG['PASSWORD'])
        
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(local_file, remote_file)  # 파일 업로드
        print(f"Uploaded to SFTP: {remote_file}")

        sftp.close()
        transport.close()

    except Exception as e:
        print("SFTP upload failed")
        # 백업 디렉터리 확인 및 생성
        os.makedirs(os.path.dirname(FAILED_UPLOAD_PATH), exist_ok=True)

        # 실패 시 백업 파일 복사
        shutil.copy(local_file, FAILED_UPLOAD_PATH)
        print(f"파일이 {FAILED_UPLOAD_PATH} 경로에 백업되었습니다.")

        print(f"SFTP upload failed: {e}")

def tar_backup_files():
    """생성된 CSV 파일들을 TAR.GZ 파일로 압축"""
    # 어제 날짜를 TAR 파일명에 포함
    yesterday = datetime.now() - timedelta(days=1)

    #backup_{YYYY-MM-DD}.tar.gz
    tar_filename = os.path.join(BACKUP_DIR, f"backup_{yesterday.strftime('%Y-%m-%d')}.tar.gz")
    remote_filename = os.path.join(SFTP_CONFIG['REMOTE_PATH'], os.path.basename(tar_filename))

    # TAR 파일이 이미 존재하면 함수 종료
    if os.path.exists(tar_filename):
        print(f"TAR file for {yesterday.strftime('%Y-%m-%d')} already exists. Skipping compression.")
        return
    with tarfile.open(tar_filename, "w:gz") as tar:
        for root, _, files in os.walk(BACKUP_DIR):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    tar.add(file_path, arcname=file)  # TAR에 파일 추가
                    os.remove(file_path)  # 원본 CSV 파일 삭제 (선택 사항)

    print(f"All backup files are compressed into: {tar_filename}")
    # SFTP 업로드
    upload_to_sftp(tar_filename, remote_filename)

# 메인 함수 실행
if __name__ == '__main__':
    latest_backup_file_date = get_latest_backup_date_from_tar()
    for table_name in table_names:
        backup_missing_dates(table_name, latest_backup_file_date)
    # TAR.GZ으로 압축
    tar_backup_files()


######SFTP가 아닌 로컬 폴더에 저장#######
# import psycopg2
# import csv
# import os
# from datetime import datetime, timedelta
# import yaml
# import zipfile

# # config.yml 파일 읽기
# config_path = '/home/ubuntu/kosmos/data_backup/config.yml'  # 리눅스 절대 경로로 수정
# with open(config_path, 'r', encoding='utf-8') as file:
#     config = yaml.safe_load(file)

# # PostgreSQL 데이터베이스 연결 정보
# DB_CONFIG = config['DB_CONFIG']

# # 백업할 테이블 이름과 날짜 컬럼
# TABLE_NAMES = config['TABLE_NAME']
# TIME_COLUMN = config['TIME_COLUMN']

# # 백업 파일을 저장할 경로
# BACKUP_DIR = config['BACKUP_DIR']
# table_names = TABLE_NAMES

# def get_latest_backup_date_from_zip():
#     """ZIP 파일명을 읽어 가장 최근 백업 날짜 확인"""
#     zip_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith('.zip')]
#     if not zip_files:
#         return None  # ZIP 파일이 없으면 None 반환

#     # ZIP 파일명에서 날짜 추출
#     dates = []
#     for file in zip_files:
#         try:
#             date_str = file.split('_')[1].split('.zip')[0]
#             dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
#         except (IndexError, ValueError):
#             continue

#     return max(dates) if dates else None

# def backup_table_by_date(target_date, table_name):
#     """특정 날짜의 데이터를 백업"""
#     file_name = f'{table_name}_backup_{target_date.strftime("%Y-%m-%d")}.csv'
#     file_path = os.path.join(BACKUP_DIR, file_name)

#     start_time = f"{target_date.strftime('%Y-%m-%d')} 00:00:00.000+09"
#     end_time = f"{target_date.strftime('%Y-%m-%d')} 23:59:59.999+09"

#     try:
#         # PostgreSQL에 연결
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()

#         # 특정 날짜 데이터 쿼리
#         query = f"""
#         SELECT * FROM {table_name}
#         WHERE {TIME_COLUMN} BETWEEN '{start_time}' AND '{end_time}'
#         ORDER BY {TIME_COLUMN}
#         ASC;
#         """
#         cur.execute(query)

#         # 결과를 파일로 저장
#         with open(file_path, mode='w', newline='') as file:
#             writer = csv.writer(file)
#             writer.writerow([desc[0] for desc in cur.description])  # 컬럼 헤더 쓰기
#             writer.writerows(cur.fetchall())  # 데이터 쓰기

#         print(f"Backup completed: {file_path}")

#     except Exception as e:
#         print(f"Error occurred: {e}")
    
#     finally:
#         # 연결 닫기
#         if conn:
#             cur.close()
#             conn.close()

# def backup_missing_dates(table_name):
#     """누락된 날짜의 데이터를 백업"""
#     os.makedirs(BACKUP_DIR, exist_ok=True)
#     today = datetime.today()
#     latest_backup_date = get_latest_backup_date_from_zip()

#     if not latest_backup_date:
#         print("No previous backup found. Backing up yesterday data.")
#         latest_backup_date = today - timedelta(days=2)  # 어제 날짜부터 시작하기 위해 2일전 날짜로 설정

#     # 날짜 차이 계산
#     delta_days = (today - latest_backup_date).days
#     print(f"Days to backup: {delta_days}")
#     if delta_days < 1:
#         print("No missing backups.")
#         return

#     # 누락된 날짜별로 백업
#     for i in range(1, delta_days):
#         target_date = latest_backup_date + timedelta(days=i)
#         print(f"Backing up data for: {target_date.strftime('%Y-%m-%d')}")
#         backup_table_by_date(target_date, table_name)

# def zip_backup_files():
#     """생성된 CSV 파일들을 ZIP 파일로 압축"""
#     # 어제 날짜를 ZIP 파일명에 포함
#     yesterday = datetime.now() - timedelta(days=1)
#     zip_filename = os.path.join(BACKUP_DIR, f"backup_{yesterday.strftime('%Y-%m-%d')}.zip")

#     # ZIP 파일이 이미 존재하면 함수 종료
#     if os.path.exists(zip_filename):
#         print(f"ZIP file for {yesterday.strftime('%Y-%m-%d')} already exists. Skipping compression.")
#         return
#     with zipfile.ZipFile(zip_filename, 'w') as zipf:
#         for root, _, files in os.walk(BACKUP_DIR):
#             for file in files:
#                 if file.endswith('.csv'):
#                     file_path = os.path.join(root, file)
#                     zipf.write(file_path, os.path.relpath(file_path, BACKUP_DIR))  # ZIP에 파일 추가
#                     os.remove(file_path)  # 원본 CSV 파일 삭제 (선택 사항)

#     print(f"All backup files are zipped into: {zip_filename}")

# # 메인 함수 실행
# if __name__ == '__main__':
#     for table_name in table_names:
#         backup_missing_dates(table_name)
#     # ZIP으로 압축
#     zip_backup_files()
