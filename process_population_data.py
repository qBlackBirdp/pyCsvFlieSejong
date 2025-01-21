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
    '경기도': {'latitude': 37.4138, 'longitude': 127.5183},
    '강원특별자치도': {'latitude': 37.8225, 'longitude': 128.1555},
    '전북특별자치도': {'latitude': 35.8175, 'longitude': 127.1400}
}

# 해당 지역에 위도와 경도를 추가하는 함수
def add_coordinates(row):
    region = row['지역']
    if region in coordinates:
        row['latitude'] = coordinates[region]['latitude']
        row['longitude'] = coordinates[region]['longitude']
    else:
        row['latitude'] = None
        row['longitude'] = None
    return row

# 기존 처리된 파일을 수정하여 월 단위 데이터를 채우는 함수
def fill_monthly_data(input_file_path, output_file_path):
    # 처리된 CSV 파일 불러오기
    processed_data = pd.read_csv(input_file_path)

    # 날짜 필드를 datetime 형식으로 변환
    processed_data['date'] = pd.to_datetime(processed_data['날짜'], errors='coerce')

    # 값을 숫자로 변환
    processed_data['값'] = pd.to_numeric(processed_data['값'], errors='coerce')

    # 지표를 개별 컬럼으로 변환
    wide_format = processed_data.pivot_table(
        index=['지역', 'latitude', 'longitude', 'date'],
        columns='지표',
        values='값'
    ).reset_index()

    # 모든 지역에 대해 월 단위로 데이터 채우기
    min_date = wide_format['date'].min()
    max_date = wide_format['date'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='MS')

    expanded_data = []
    for region, group in wide_format.groupby(['지역', 'latitude', 'longitude']):
        region_data = pd.DataFrame({'date': all_dates})
        region_data['지역'] = region[0]
        region_data['latitude'] = region[1]
        region_data['longitude'] = region[2]
        merged = pd.merge(region_data, group, on=['date', '지역', 'latitude', 'longitude'], how='left')
        merged = merged.fillna(method='ffill').fillna(method='bfill')
        expanded_data.append(merged)

    result = pd.concat(expanded_data, ignore_index=True)

    # 저장된 결과를 새로운 CSV 파일로 저장
    result.to_csv(output_file_path, index=False)
    print(f"월 단위로 확장된 데이터가 저장되었습니다: {output_file_path}")

# 예시 실행
if __name__ == '__main__':
    input_file_path = 'data/processed_population_data_long_format_with_date.csv'  # 처리된 파일 경로
    output_file_path = 'data/expanded_population_data.csv'  # 저장할 파일 경로

    fill_monthly_data(input_file_path, output_file_path)
