# process_population_data.py

import pandas as pd

# 위도와 경도 추가하기 위한 대표 값 (임의로 설정한 예시)
coordinates = {
    '세종특별자치시': {'latitude': 36.4800, 'longitude': 127.2890},
    '서울특별자치시': {'latitude': 37.5665, 'longitude': 126.9780},
    '부산광역시': {'latitude': 35.1796, 'longitude': 129.0756},
    '인천광역시': {'latitude': 37.4563, 'longitude': 126.7052},
    '대구광역시': {'latitude': 35.8714, 'longitude': 128.6014},
    '대전광역시': {'latitude': 36.3504, 'longitude': 127.3845},
    '광주광역시': {'latitude': 35.1595, 'longitude': 126.8526},
    '울산광역시': {'latitude': 35.5384, 'longitude': 129.3114},
    '제주특별자치도': {'latitude': 33.4996, 'longitude': 126.5312},
    '경상남도': {'latitude': 35.2354, 'longitude': 128.6922},
    '경상북도': {'latitude': 36.1584, 'longitude': 128.7364},
    '전라남도': {'latitude': 34.8006, 'longitude': 126.2994},
    '전라북도': {'latitude': 35.8175, 'longitude': 127.1400},
    '충청남도': {'latitude': 36.6353, 'longitude': 126.7404},
    '충청북도': {'latitude': 36.6297, 'longitude': 127.9210},
    '강원도': {'latitude': 37.8225, 'longitude': 128.1555},
    '경기도': {'latitude': 37.4138, 'longitude': 127.5183},  # 경기도
    '강원특별자치도': {'latitude': 37.8225, 'longitude': 128.1555},  # 강원특별자치도
    '전북특별자치도': {'latitude': 35.8175, 'longitude': 127.1400}  # 전북특별자치도
}


# 해당 지역에 위도와 경도를 추가하는 함수
def add_coordinates(row):
    region = row['지역']  # 지역명이 '지역' 컬럼에 들어 있다고 가정
    if region in coordinates:
        row['latitude'] = coordinates[region]['latitude']
        row['longitude'] = coordinates[region]['longitude']
    else:
        row['latitude'] = None
        row['longitude'] = None
    return row


# CSV 파일을 정리하는 함수
def process_population_data(input_file_path, output_file_path):
    # CSV 파일 불러오기
    population_data = pd.read_csv(input_file_path)

    # '행정구역(시도)별' 컬럼을 '지역'으로 변환하여 위도/경도 추가
    population_data['지역'] = population_data['행정구역(시도)별'].str.strip()  # 공백 제거

    # 위도/경도 데이터 추가
    population_data = population_data.apply(add_coordinates, axis=1)

    # 데이터 변환: Wide Format -> Long Format
    id_vars = ['지역', 'latitude', 'longitude']  # 고정 열
    value_vars = [col for col in population_data.columns if '/' in col]  # 시간 관련 열

    long_format = pd.melt(
        population_data,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='시간',
        value_name='값'
    )

    # 시간과 지표 분리 및 날짜 생성
    def split_time_metric(row):
        try:
            period, metric = row['시간'].split('/')
            year, quarter = period.split('.')
            quarter_map = {
                '1': 'Q1',
                '2': 'Q2',
                '3': 'Q3',
                '4': 'Q4'
            }
            row['연도'] = year
            row['분기'] = quarter_map[quarter]

            if metric.endswith('.1'):
                row['지표'] = '전출률'
            elif metric.endswith('.2'):
                row['지표'] = '순이동률'
            else:
                row['지표'] = '전입률'

            # 날짜 생성
            quarter_start_month = {
                'Q1': '01',
                'Q2': '04',
                'Q3': '07',
                'Q4': '10'
            }
            row['날짜'] = f"{year}-{quarter_start_month[row['분기']]}-01"
        except Exception as e:
            print(f"Error processing row: {row['시간']}, Error: {e}")
        return row

    long_format = long_format.apply(split_time_metric, axis=1)

    # 불필요한 열 제거 및 재정렬
    long_format = long_format[['지역', 'latitude', 'longitude', '날짜', '연도', '분기', '지표', '값']]

    # 새로운 CSV 파일로 저장
    long_format.to_csv(output_file_path, index=False)

    print(f"처리된 파일이 저장되었습니다: {output_file_path}")


# 예시 실행
if __name__ == '__main__':
    input_file_path = 'data/인구이동률_월__분기__년__20250120211642.csv'  # 업로드된 파일 경로
    output_file_path = 'data/processed_population_data_long_format_with_date.csv'  # 저장할 파일 경로

    process_population_data(input_file_path, output_file_path)
