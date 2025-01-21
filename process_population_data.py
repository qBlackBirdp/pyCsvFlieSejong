# process_population_data.py

import pandas as pd

# 위도와 경도 추가하기 위한 대표 값 (임의로 설정한 예시)
coordinates = {
    '세종특별자치시': {'latitude': 36.4800, 'longitude': 127.2890},
    '서울특별시': {'latitude': 37.5665, 'longitude': 126.9780},
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


# CSV 파일을 읽어오는 함수
def process_population_data(input_file_path, output_file_path):
    # CSV 파일 불러오기
    population_data = pd.read_csv(input_file_path)

    # '행정구역(시도)별' 컬럼을 '지역'으로 변환하여 위도/경도 추가
    population_data['지역'] = population_data['행정구역(시도)별'].apply(lambda x: x.strip())  # 공백 제거

    # 위도/경도 데이터 추가
    population_data = population_data.apply(add_coordinates, axis=1)

    # 새로운 CSV 파일로 저장
    population_data.to_csv(output_file_path, index=False)

    print(f"처리된 파일이 저장되었습니다: {output_file_path}")


# 예시 실행
if __name__ == '__main__':
    input_file_path = 'data/인구이동률_월__분기__년__20250120211642.csv'  # 업로드된 파일 경로
    output_file_path = 'data/processed_population_data.csv'  # 저장할 파일 경로

    process_population_data(input_file_path, output_file_path)
